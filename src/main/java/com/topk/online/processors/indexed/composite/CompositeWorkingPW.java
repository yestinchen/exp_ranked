package com.topk.online.processors.indexed.composite;

import com.topk.bean.Interval;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.indexed.Candidate;
import com.topk.online.processors.indexed.IndexedWorkingPartition2;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.ps.PlaneSweepUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;
import java.util.stream.Collectors;

public class CompositeWorkingPW {
    static Logger LOG = LogManager.getLogger(CompositeWorkingPW.class);

    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    PartitionWindow<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>, String,
            IndexedPartitionPayload> partitionWindow;


    int score = 0;
    List<CompositeWorkingPartition<PayloadClassIntervals>> wps;
    Set<String> commonObjs;
    int remainingPossibleMax;
    int baseIdx;
    int[] windowScoreArr;
    int windowMinScore = 0;
    // put all computed sets.
    Set<Set<String>> computedSet = new HashSet<>();
    int w;
    int maxPartitionNum;
    int k;
    boolean firstVisit = true;
    int firstRetrieveLevels = 0;

    public CompositeWorkingPW(PartitionWindow<String, PayloadClassIntervals,
            Node<String, PayloadClassIntervals>, String, IndexedPartitionPayload> pw,
                              int maxPartitionNum, int partitionSize, int partitionNum, int w,
                              Map<CLabel, Integer> conditionMap, int k, int firstRetrieveLevels) {
        this.partitionWindow = pw;
        this.w = w;
        this.maxPartitionNum = maxPartitionNum;
        this.k = k;
        this.firstRetrieveLevels = firstRetrieveLevels;
        // get all retrieved keys.
        this.commonObjs = pw.selectCommonObjects();

        // 1. obtain all object sets with the same
        this.wps = pw.getPartitions().stream().map(i -> new CompositeWorkingPartition<>(i, commonObjs,
                conditionMap, computedSet)).collect(Collectors.toList());

        this.remainingPossibleMax = IndexedUtils.computePossibleMaxComposite(wps, maxPartitionNum);

        int startWindow = WindowComputer.computeStartWindow(pw.getStart(), partitionSize, partitionNum, w);
        this.baseIdx = pw.getStart() + startWindow;

        this.windowScoreArr = IndexedUtils.initWindowScoreArr(pw, w, startWindow);
    }


    private void processOneBatch(List<Map<Set<String>, List<Interval>>> retrievedMap,
                                 Set<Set<String>> allRetrievedObjs,
                                 Map<Set<String>, List<Interval>> intervalMap) {
        // 1. retrieve all interval list.
        for (int i =0; i < retrievedMap.size(); i++) {
            CompositeWorkingPartition<PayloadClassIntervals> wp = wps.get(i);
            Set<String> thisPartitionObjs = new HashSet<>(wp.getBasePartition().getObjs());
            // get object sets that are not retrieved.
            for (Set<String> set: allRetrievedObjs) {
                if (!retrievedMap.get(i).containsKey(set)) {
                    // retrieve & compute
                    List<List<Interval>> intervalLists = new ArrayList<>();
                    for (String obj: set) {
                        if (thisPartitionObjs.contains(obj) && wp.hasNodeUnvisited(obj)) {
                            intervalLists.add(wps.get(i).getIntervalsWithKey(obj));
                        } else {
                            break;
                        }
                    }
                    if (intervalLists.size() == set.size()) {
                        // plane sweep.
                        List<Interval> intervals = PlaneSweepUtils.planeSweep(intervalLists);
                        if (intervals.size() > 0) {
                            intervalMap.computeIfAbsent(set, x-> new ArrayList<>()).addAll(intervals);
                        }
                    }
                } else {
                    intervalMap.computeIfAbsent(set, x-> new ArrayList<>()).addAll(retrievedMap.get(i).get(set));
                }
                computedSet.add(set);
            }
        }
    }
    public boolean processFirstVisit() {
        if (firstRetrieveLevels <= 0) return false;
        // retrieve all candidates from each partition.
        List<Map<Set<String>, List<Interval>>> mapList = new ArrayList<>();

        Set<Set<String>> allSets = new HashSet<>();

        for (CompositeWorkingPartition<PayloadClassIntervals> wp : wps) {
            Map<Set<String>, List<Interval>> thisMap = wp.visitByLevel(firstRetrieveLevels);
            mapList.add(thisMap);
            allSets.addAll(thisMap.keySet());
        }

        // hold the results.
        Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();

        processOneBatch(mapList, allSets, intervalMap);

        boolean updated = IndexedUtils.updateWindowAccordingToIntervalList(intervalMap,
                windowMinScore, windowScoreArr, baseIdx, w);

        // update remaining possible max.
        remainingPossibleMax = IndexedUtils.computePossibleMaxComposite(wps, maxPartitionNum);
        windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
        // update this score.
        this.score = remainingPossibleMax;
        return updated;
    }

    public void workUtil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
        LOG.debug("stopScore: {}, remaining max: {}", stopScore, remainingPossibleMax);
        boolean processed = false;

        // special case for first visit.
        if (firstVisit) {
            processed = processFirstVisit();
            firstVisit = false;
        }

        while(this.remainingPossibleMax >= stopScore && this.remainingPossibleMax > 0
                && this.remainingPossibleMax > topkBookKeeper.getMin()) {
            // 1. sort all working partitions.

            Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

            IndexedUtils.logCompositeWPsInfo(wps, windowScoreArr, remainingPossibleMax, windowMinScore);

            CompositeWorkingPartition<PayloadClassIntervals> wp = wps.get(0);

            Map<Set<String>, List<Interval>> resultMap = new HashMap<>();
            // get the first one.
            CompositeCandidate<PayloadClassIntervals> currentCandidate = wp.nextNode(resultMap);

            // means no more.
            if (resultMap.isEmpty()) {
//                LOG.debug("empty interval map");
                this.remainingPossibleMax = 0;
                break;
            }

            Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
            // make a new map
            for (Map.Entry<Set<String>, List<Interval>> entry: resultMap.entrySet()) {
                intervalMap.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }

            Node<String, PayloadClassIntervals> currentNode = currentCandidate.node;
            // retrieve the same item from other partitions.
            Map<CLabel, List<String>> prefixMap = currentCandidate.prefixMap;
            LOG.debug("processing : {} + {}", prefixMap, currentNode.getKey());
            if (prefixMap.size() > 0) {
                Set<String> uniqueObjs = new HashSet<>();
                for (Set<String> set: intervalMap.keySet()) {
                    uniqueObjs.addAll(set);
                }
                for (int i =1; i < wps.size(); i++) {
                    CompositeWorkingPartition<PayloadClassIntervals> visitingWp = wps.get(i);
                    // retrieve the current key.
                    List<Interval> currentKeyIntervals = visitingWp.getIntervalsWithKey(currentNode.getKey());
                    if (currentKeyIntervals == null) continue;

                    Map<String, List<Interval>> otherObjMap = new HashMap<>();
                    otherObjMap.put(currentNode.getKey(), currentKeyIntervals);

//                    LOG.debug("retrieving {} from {} : {}", currentNode.getKey(),
//                            wps.get(i).getBasePartition().getStartFrame(), currentKeyIntervals);

                    for (String obj : uniqueObjs) {
                        // skip the current key.
                        if (obj.equals(currentNode.getKey())) continue;

                        if (visitingWp.getBasePartition().getObjs().contains(obj)) {
                            List<Interval> intervals = visitingWp.getIntervalsWithKey(obj);
                            otherObjMap.put(obj, intervals);
//                            LOG.debug("retrieving {} from {} : {}", obj,
//                                    wps.get(i).getBasePartition().getStartFrame(), intervals);
                        }
                    }

                    // compute sets.
                    for (Set<String> set : intervalMap.keySet()) {
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

//                        LOG.debug("ps result for {}:{} ", set, result);

                        // add to interval list.
                        intervalMap.get(set).addAll(result);
                    }
                }
            } else {
                LOG.debug("prefix empty");
            }
//            LOG.debug("interval map: [{}]", intervalMap);
            boolean updated = IndexedUtils.updateWindowAccordingToIntervalList(intervalMap,
                    windowMinScore, windowScoreArr, baseIdx, w);

            if (!processed && updated) {
                processed = true;
            }
            // update remaining possible max.
            remainingPossibleMax = IndexedUtils.computePossibleMaxComposite(wps, maxPartitionNum);
            windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
            // update this score.
            this.score = remainingPossibleMax;

        }

        if (processed) {
            // d. update topk
            IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);
        } else {

            // update remaining possible max.
            remainingPossibleMax = IndexedUtils.computePossibleMaxComposite(wps, maxPartitionNum);
            windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
            // update this score.
            this.score = remainingPossibleMax;

        }
    }
    public PartitionWindow<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>,
            String, IndexedPartitionPayload> getPartitionWindow() {
        return partitionWindow;
    }

    public void setPartitionWindow(PartitionWindow<String, PayloadClassIntervals,
            Node<String, PayloadClassIntervals>, String, IndexedPartitionPayload> partitionWindow) {
        this.partitionWindow = partitionWindow;
    }

    public void estimateScore(int objNum) {
        partitionWindow.estimateScore(objNum);
        score = partitionWindow.getScore();
    }

    public void updateEstimateScore(int newScore) {
        score = newScore;
    }

    public int getScore() {
        return score;
    }
}
