package com.topk.online.processors.indexed;

import com.topk.bean.Interval;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.ps.PlaneSweepUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.utils.Combinations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class WorkingPW {

    static Logger LOG = LogManager.getLogger(WorkingPW.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
            IndexedPartitionPayload> partitionWindow;

    int score = 0;
    List<IndexedWorkingPartition2<PayloadIntervals>> wps;
    Set<String> commonObjs;
    int remainingPossibleMax;
    int baseIdx;
    int[] windowScoreArr;
    int windowMinScore = 0;
    // put all computed sets.
    Set<Set<String>> computedSet = new HashSet<>();
    int objNum;
    int w;
    int maxPartitionNum;
    int k;
    boolean firstVisit = true;
    int firstRetrieveLevels = 0;

    public WorkingPW(PartitionWindow<String, PayloadIntervals,
            Node<String, PayloadIntervals>, String, IndexedPartitionPayload> pw,
                     int maxPartitionNum, int partitionSize, int partitionNum, int w,
                     int objNum, int k, int firstRetrieveLevels) {
        this.partitionWindow = pw;
        this.objNum = objNum;
        this.w = w;
        this.maxPartitionNum = maxPartitionNum;
        this.k = k;
        this.firstRetrieveLevels = firstRetrieveLevels;

        // get all retrieved keys.
        this.commonObjs = pw.selectCommonObjects();

        // 1. obtain all object sets with the same
        this.wps = pw.getPartitions().stream().map(
                i ->new IndexedWorkingPartition2<>(i, commonObjs, objNum)).collect(Collectors.toList());

        this.remainingPossibleMax = IndexedUtils.computePossibleMax(wps, maxPartitionNum);

        int startWindow = WindowComputer.computeStartWindow(pw.getStart(), partitionSize, partitionNum, w);
        this.baseIdx = pw.getStart() + startWindow;
        this.windowScoreArr = IndexedUtils.initWindowScoreArr(pw, w, startWindow);
    }

    private void processOneBatch(List<Map<Set<String>, List<Interval>>> retrievedMap,
                                    Set<Set<String>> allRetrievedObjs,
                                    Map<Set<String>, List<Interval>> intervalMap) {
        // 1. retrieve all interval list.
        for (int i =0; i < retrievedMap.size(); i++) {
            IndexedWorkingPartition2<PayloadIntervals> wp = wps.get(i);
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
        Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
        List<Map<Set<String>, List<Interval>>> mapList = new ArrayList<>();

        Set<Set<String>> allSets = new HashSet<>();
        for (IndexedWorkingPartition2<PayloadIntervals> wp : wps) {
            Map<Set<String>, List<Interval>> thisMap = wp.visitByLevel(firstRetrieveLevels);
            mapList.add(thisMap);
            allSets.addAll(thisMap.keySet());
        }

        processOneBatch(mapList, allSets, intervalMap);

        // retrieve from other partitions. N^2
        boolean updated = IndexedUtils.updateWindowAccordingToIntervalList(intervalMap,
                windowMinScore, windowScoreArr, baseIdx, w);

        // update remaining possible max.
        remainingPossibleMax = IndexedUtils.computePossibleMax(wps, maxPartitionNum);
        windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
        // update this score.
        this.score = remainingPossibleMax;
        return updated;
    }

//    /**
//     * work until < stopScore.
//     * @param stopScore
//     */
//    public void batchWorkUtil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
//        LOG.debug("stopScore: {}, remaining max: {}", stopScore, remainingPossibleMax);
//        boolean processed = false;
//
//        // special case for first visit.
//        if (firstVisit) {
//            processed = processFirstVisit();
//            firstVisit = false;
//        }
//
//        while(this.remainingPossibleMax >= stopScore
//                && this.remainingPossibleMax > 0 &&
//                this.remainingPossibleMax > topkBookKeeper.getMin()) {
//            // 1. sort all working partitions.
//            // start working.
//            Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));
//
//            IndexedUtils.logWPsInfo(wps, windowScoreArr, remainingPossibleMax, windowMinScore);
//
//            IndexedWorkingPartition2<PayloadIntervals> wp = wps.get(0);
//
//            // get the first one.
//            Candidate<PayloadIntervals> currentCandidate = wp.nextNode(objNum);
//            // means no candidate anymore.
//            if (currentCandidate == null) {
//                this.remainingPossibleMax = 0;
//                break;
//            }
//
//
//            Node<String, PayloadIntervals> currentNode = currentCandidate.node;
//            Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
//            // retrieve the same item from other partitions.
//            List<String> objs = currentCandidate.prefix;
//
//            if (commonObjs.contains(currentNode.getKey()) && objs.size() >= objNum - 1) {
//                LOG.debug("processing : {} + {}", objs, currentNode.getKey());
//                // 1. split into multiple object sets.
//                List<List<String>> allCombs = Combinations.combinations(objs, objNum -1);
//
//                List<Set<String>> setToCompute = new ArrayList<>();
//                // add the current one.
//                for (List<String> comb: allCombs) {
//                    Set<String> set = new HashSet<>(comb);
//                    set.add(currentNode.getKey());
//                    if (!computedSet.contains(set)) {
//                        setToCompute.add(set);
//                    }
//                }
//
//                // init each set.
//                for (Set<String> set: setToCompute) {
//                    intervalMap.computeIfAbsent(set, x-> new ArrayList<>())
//                            .addAll(currentNode.getPayload().getIntervals());
//                }
//
//                LOG.debug("set to compute: {}", setToCompute);
//                if (setToCompute.size() > 0) {
//                    //
//                    Set<String> uniqueObjs = new HashSet<>();
//                    for (Set<String> set: setToCompute) {
//                        uniqueObjs.addAll(set);
//                    }
//                    for (int i = 1; i < wps.size(); i++) {
//                        IndexedWorkingPartition2<PayloadIntervals> visitingWp = wps.get(i);
//                        // retrieve the current key.
//                        List<Interval> currentKeyIntervals =
//                                visitingWp.getIntervalsWithKey(currentNode.getKey());
//                        if (currentKeyIntervals == null) continue;
//                        // get all the rest.
//                        Map<String, List<Interval>> otherObjMap = new HashMap<>();
//                        // put the current one first.
//                        otherObjMap.put(currentNode.getKey(), currentKeyIntervals);
//                        LOG.debug("retrieving {} from {} : {}", currentNode.getKey(),
//                                wps.get(i).getBasePartition().getStartFrame(), currentKeyIntervals);
//                        for (String obj : uniqueObjs) {
//                            // skip the current key.
//                            if (obj.equals(currentNode.getKey())) continue;
//
//                            if (visitingWp.getBasePartition().getObjs().contains(obj)) {
//                                List<Interval> intervals = visitingWp.getIntervalsWithKey(obj);
//                                otherObjMap.put(obj, intervals);
//                                LOG.debug("retrieving {} from {} : {}", obj,
//                                        wps.get(i).getBasePartition().getStartFrame(), intervals);
//                            }
//                        }
//
//                        for (Set<String> set : setToCompute) {
//                            // 1. all all objects together.
//                            List<List<Interval>> intervalLists = new ArrayList<>();
//                            for (String s : set) {
//                                List<Interval> intervalList = otherObjMap.get(s);
//                                if (intervalList != null) {
//                                    intervalLists.add(intervalList);
//                                }
//                            }
//                            // should be the same size as the key set.
//                            if (intervalLists.size() < set.size()) continue;
//
//                            // 2. plane-sweep.
//                            List<Interval> result = PlaneSweepUtils.planeSweep(intervalLists);
//
//                            LOG.debug("ps result for {}:{} ", set, result);
//
//                            // add to interval list.
//                            intervalMap.get(set).addAll(result);
//                        }
//                    }
//                    computedSet.addAll(setToCompute);
//                }
//            } else if (currentCandidate.prefix.size() == objNum -1) {
//                Set<String> set = new HashSet<>(currentCandidate.prefix);
//                set.add(currentNode.getKey());
//                // add
//                intervalMap.put(set, currentNode.getPayload().getIntervals());
//            }
//
//            boolean updated = IndexedUtils.updateWindowAccordingToIntervalList(intervalMap,
//                    windowMinScore, windowScoreArr, baseIdx, w);
//
//            if (!processed && updated) {
//                processed = true;
//            }
//            // update remaining possible max.
//            remainingPossibleMax = IndexedUtils.computePossibleMax(wps, maxPartitionNum);
//            windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
//            // update this score.
//            this.score = remainingPossibleMax;
//
//        }
//        if (processed) {
//            // d. update topk
//            IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);
//        }
//    }

    /**
     * work until < stopScore.
     * @param stopScore
     */
    public void workUtil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
        LOG.debug("stopScore: {}, remaining max: {}", stopScore, remainingPossibleMax);
        boolean processed = false;

        // special case for first visit.
        if (firstVisit) {
            processed = processFirstVisit();
            firstVisit = false;
        }

        while(this.remainingPossibleMax >= stopScore
                && this.remainingPossibleMax > 0 &&
                this.remainingPossibleMax > topkBookKeeper.getMin()) {
            // 1. sort all working partitions.
            // start working.
            Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

            IndexedUtils.logWPsInfo(wps, windowScoreArr, remainingPossibleMax, windowMinScore);

            IndexedWorkingPartition2<PayloadIntervals> wp = wps.get(0);

            // get the first one.
            Candidate<PayloadIntervals> currentCandidate = wp.nextNode(objNum);
            // means no candidate anymore.
            if (currentCandidate == null) {
                this.remainingPossibleMax = 0;
                break;
            }


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

//                Set<Set<String>> setToCompute = new HashSet<>();
//                Set<String> newSet = new HashSet<>(currentCandidate.prefix);
//                newSet.add(currentNode.getKey());
//                if (!computedSet.contains(newSet)) {
//                    setToCompute.add(newSet);
//                    intervalMap.computeIfAbsent(newSet, x -> new ArrayList<>())
//                            .addAll(currentNode.getPayload().getIntervals());
//                }

                LOG.debug("set to compute: {}", setToCompute);
                if (setToCompute.size() > 0) {
                    //
                    Set<String> uniqueObjs = new HashSet<>();
                    for (Set<String> set: setToCompute) {
                        uniqueObjs.addAll(set);
                    }
                    for (int i = 1; i < wps.size(); i++) {
                        IndexedWorkingPartition2<PayloadIntervals> visitingWp = wps.get(i);
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

                            // put.
//                            intervalMap.computeIfAbsent(set, k1 -> new ArrayList<>());
//                            if (intervalMap.get(set).isEmpty()) {
//                                intervalMap.get(set).addAll(currentNode.getPayload().getIntervals());
//                            }
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

            boolean updated = IndexedUtils.updateWindowAccordingToIntervalList(intervalMap,
                    windowMinScore, windowScoreArr, baseIdx, w);

            if (!processed && updated) {
                processed = true;
            }

            // update remaining possible max.
            remainingPossibleMax = IndexedUtils.computePossibleMax(wps, maxPartitionNum);
            windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
            // update this score.
            this.score = remainingPossibleMax;

        }
        if (processed) {
            // d. update topk
            IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);
        } else {
            // update

            // update remaining possible max.
            remainingPossibleMax = IndexedUtils.computePossibleMax(wps, maxPartitionNum);
            windowMinScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
            // update this score.
            this.score = remainingPossibleMax;

        }
    }


    public PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>,
            String, IndexedPartitionPayload> getPartitionWindow() {
        return partitionWindow;
    }

    public void setPartitionWindow(PartitionWindow<String, PayloadIntervals,
            Node<String, PayloadIntervals>, String, IndexedPartitionPayload> partitionWindow) {
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
