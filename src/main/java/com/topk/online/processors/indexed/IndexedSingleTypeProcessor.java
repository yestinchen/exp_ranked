package com.topk.online.processors.indexed;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.PWGenWGroup;
import com.topk.online.component.WindowComputer;
import com.topk.online.ps.PlaneSweepUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import com.topk.utils.Combinations;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class IndexedSingleTypeProcessor {

    Logger LOG = LogManager.getLogger(IndexedSingleTypeProcessor.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<BasePartition<Node<String, PayloadIntervals>,
            IndexedPartitionPayload>> partitions;
    int partitionSize;

    public IndexedSingleTypeProcessor(List<BasePartition<Node<String, PayloadIntervals>,
            IndexedPartitionPayload>> partitions,
                                      int partitionSize) {
        this.partitions = partitions;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, int objNum) {
        //
        int partitionNum = (int) Math.ceil(w*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize) + 1;
        LOG.debug("max partition num :" + maxPartitionNum);

        List<PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload>> partitionWindows =
                PWGenWGroup.genPWs(partitions, objNum, partitionNum, partitionSize);
        for (PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload> pw : partitionWindows) {
            pw.estimateScore(objNum);
        }
        Collections.sort(partitionWindows, (x1, x2)->  - Integer.compare(x1.getScore(), x2.getScore()));

        LOG.debug("sorted pws");
        if (LOG.isDebugEnabled()) {
            for (PartitionWindow p : partitionWindows) {
                LOG.debug("window: {}, score: {}", p.getStart(), p.getScore());
            }
        }

        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);
        for (PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload> pw: partitionWindows) {
            LOG.debug("processing partition window {}", pw.getStart());
            if (pw.getScore() <= topkBookKeeper.getMin()) {
                break;
            }

            // process this partition window.

            // get all retrieved keys.
            Set<String> commonObjs = pw.selectCommonObjects();
            // 1. obtain all object sets with the same
            List<IndexedWorkingPartition2> wps = pw.getPartitions().stream().map(i ->
                    new IndexedWorkingPartition2(i,commonObjs, objNum)).collect(Collectors.toList());


            int remainingPossibleMax = computePossibleMax(wps, maxPartitionNum);

            int startWindow = WindowComputer.computeStartWindow(pw.getStart(), partitionSize, partitionNum, w);
            int baseIdx = pw.getStart() + startWindow;
            int[] windowScoreArr;
            int size = pw.getEnd() - pw.getStart() - w + 2 - startWindow;
            if (size > 0) {
                windowScoreArr = new int[size];
            } else {
                windowScoreArr = new int[1];
            }
            int minScore = 0;

            // put all computed sets.
            Set<Set<String>> computedSet = new HashSet<>();

            while (remainingPossibleMax > minScore) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("remaining possible max: {}, current min: {}", remainingPossibleMax, minScore);
                    LOG.debug(" current score window: {}", Arrays.toString(windowScoreArr));
                    int totalCount = 0;
                    for (IndexedWorkingPartition2 wp : wps) {
                        totalCount += wp.getProcessedCount();
                        LOG.debug("p: {}, computed node count: {}, remaining max: {}",
                                wp.getBasePartition().getStartFrame(), wp.getProcessedCount(),
                                wp.getRemainingCount());
                    }
                    LOG.debug("total processed: {}, current min: {}", totalCount, minScore);
                }

                // start working.
                Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

                IndexedWorkingPartition2 wp = wps.get(0);
                // get the first one.
                Candidate currentCandidate = wp.nextNode(objNum);
                // means no candidate anymore.
                if (currentCandidate == null) break;

                Node<String, PayloadIntervals> currentNode = currentCandidate.node;
                Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
                // retrieve the same item from other partitions.
                List<String> objs = currentCandidate.prefix;

                if (commonObjs.contains(currentNode.getKey()) && objs.size() >= objNum - 1) {
                    LOG.debug("processing : {} + {}", objs, currentNode.getKey());
                    // 1. split into multiple object sets.
                    List<List<String>> allCombs = Combinations.combinations(objs, objNum -1);

                    List<Set<String>> setToCompute = new ArrayList<>();
                    // add the current one.
                    for (List<String> comb: allCombs) {
                        Set<String> set = new HashSet<>(comb);
                        set.add(currentNode.getKey());
                        if (!computedSet.contains(set)) {
                            setToCompute.add(set);
                        }
                    }

                    // init each set.
                    for (Set<String> set: setToCompute) {
                        intervalMap.computeIfAbsent(set, x-> new ArrayList<>())
                                .addAll(currentNode.getPayload().getIntervals());
                    }

                    LOG.debug("set to compute: {}", setToCompute);
                    if (setToCompute.size() > 0) {
                        //
                        Set<String> uniqueObjs = new HashSet<>();
                        for (Set<String> set: setToCompute) {
                            uniqueObjs.addAll(set);
                        }
                        for (int i = 1; i < wps.size(); i++) {
                            IndexedWorkingPartition2 visitingWp = wps.get(i);
                            // retrieve the current key.
                            List<Interval> currentKeyIntervals =
                                    visitingWp.getIntervalsWithKey(currentNode.getKey());
                            if (currentKeyIntervals == null) continue;
                            // get all the rest.
                            Map<String, List<Interval>> otherObjMap = new HashMap<>();
                            // put the current one first.
                            otherObjMap.put(currentNode.getKey(), currentKeyIntervals);
                            LOG.debug("retrieving {} from {} : {}", currentNode.getKey(),
                                    wps.get(i).getBasePartition().getStartFrame(), currentKeyIntervals);
                            for (String obj : uniqueObjs) {
                                // skip the current key.
                                if (obj.equals(currentNode.getKey())) continue;

                                if (visitingWp.getBasePartition().getObjs().contains(obj)) {
                                    List<Interval> intervals = visitingWp.getIntervalsWithKey(obj);
                                    otherObjMap.put(obj, intervals);
                                    LOG.debug("retrieving {} from {} : {}", obj,
                                            wps.get(i).getBasePartition().getStartFrame(), intervals);
                                }
                            }

                            for (Set<String> set : setToCompute) {
                                // 1. all all objects together.
                                List<List<Interval>> intervalLists = new ArrayList<>();
                                for (String s : set) {
                                    List<Interval> intervalList = otherObjMap.get(s);
                                    if (intervalList != null) {
                                        intervalLists.add(intervalList);
                                    }
                                }
                                // should be the same size as the key set.
                                if (intervalLists.size() < set.size()) continue;

                                // 2. plane-sweep.
                                List<Interval> result = PlaneSweepUtils.planeSweep(intervalLists);

                                LOG.debug("ps result for {}:{} ", set, result);

                                // add to interval list.
                                intervalMap.get(set).addAll(result);
                            }
                        }
                        computedSet.addAll(setToCompute);
                    }
                } else if (currentCandidate.prefix.size() == objNum -1) {
                    Set<String> set = new HashSet<>(currentCandidate.prefix);
                    set.add(currentNode.getKey());
                    // add
                    intervalMap.put(set, currentNode.getPayload().getIntervals());
                }
                if (LOG.isDebugEnabled()) {
                    for (Set<String> key : intervalMap.keySet()) {
                        LOG.debug("interval map item, {} : {}", key, intervalMap.get(key));
                    }
                }
                // sort & compute window score.
                List<IntervalListWCount> allList = new ArrayList<>();
                for (Map.Entry<Set<String>, List<Interval>> entry : intervalMap.entrySet()) {
                    allList.add(new IntervalListWCount(entry.getKey(), entry.getValue()));
                }

                // desc order.
                Collections.sort(allList, (x1, x2) -> Integer.compare(x2.getCount(), x1.getCount()));

                // go go go.
                for (IntervalListWCount entry: allList) {
                    if (entry.getCount() < minScore) break;
                    Collections.sort(entry.getIntervals(), Comparator.comparingInt(Interval::getStart));
                    // fill in window arr.

                    // process key.
                    List<Interval> intervals = entry.getIntervals();

                    if (!intervals.isEmpty()) {
                        // N^2, could be improved.
                        int currentMin = Integer.MAX_VALUE;
                        for (int j =0; j < windowScoreArr.length; j++) {
                            int start = j + baseIdx;
                            int end = start + w -1;
                            int score = 0;
                            for (Interval inter: intervals) {
                                if (inter.getEnd() < start) continue;
                                if (inter.getStart() > end) break; // we assume ordered. list
                                int maxStart = Math.max(start, inter.getStart());
                                int minEnd = Math.min(end, inter.getEnd());
                                if (minEnd >= maxStart) {
                                    score += minEnd - maxStart + 1;
                                }
                            }
                            if (score > windowScoreArr[j]) {
                                windowScoreArr[j] = score;
                            }
                            if (windowScoreArr[j] < currentMin) {
                                currentMin = windowScoreArr[j];
                            }
                        }
//                        minScore = currentMin;
                    }
                }

                // update remaining possible max.
                remainingPossibleMax = computePossibleMax(wps, maxPartitionNum);
                minScore = getKthFromArray(windowScoreArr, k);
            }

            // update window.

            // d. update topk
            for (int i =0; i < windowScoreArr.length; i++) {
                if (windowScoreArr[i] >= topkBookKeeper.getMin()) {
                    WindowWithScore wws = new WindowWithScore();
                    wws.setWindow(new Interval(baseIdx + i, baseIdx + i + w - 1));
                    wws.setScore(windowScoreArr[i]);
                    topkBookKeeper.update(wws);
                }
            }

        }
        return topkBookKeeper.getTopkResults();
    }

    int getKthFromArray(int[] windowArr, int k) {
        int[] copiedArr = new int[windowArr.length];
        System.arraycopy(windowArr, 0, copiedArr, 0, windowArr.length);
        Arrays.sort(copiedArr);
        int pos = k < windowArr.length ? k : windowArr.length -1;
        return copiedArr[pos];
    }

    int computePossibleMax(List<IndexedWorkingPartition2> wps, int maxPartitionNum) {
        // in order.
        Map<Integer, IndexedWorkingPartition2> pwMap = new HashMap<>();
        for (IndexedWorkingPartition2 wp: wps) {
            pwMap.put(wp.getBasePartition().getStartFrame(), wp);
        }
        List<Map.Entry<Integer, IndexedWorkingPartition2>> sorted = new ArrayList<>(pwMap.entrySet());
        Collections.sort(sorted, Comparator.comparingInt(Map.Entry::getKey));
        // map to remaining count.
        int[] countList = sorted.stream().mapToInt(i -> i.getValue().getRemainingCount()).toArray();
        List<Integer> values = new ArrayList<>();
//        LOG.debug("remaining possible , count list: {}", Arrays.toString(countList));
        int sum = 0;
        for (int i=0; i < sorted.size(); i++) {
            sum += countList[i];
            if (i + 1 >= maxPartitionNum) {
                values.add(sum);
                sum -= countList[i - maxPartitionNum + 1];
            }
        }
//        LOG.debug("remaining possible max arr: {}", values);
        return Collections.max(values);
    }

}
