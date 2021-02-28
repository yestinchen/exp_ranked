package com.topk.online.processors.bitset;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.bitset.measure.Measurement;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.utils.BitSetUtils;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;
import java.util.stream.Collectors;

public class WorkingPW {

    private Logger LOG = LogManager.getLogger(WorkingPW.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
            BitsetPartitionPayload<PayloadWMaskDewey>> pw;

    int remainingMax;

    // get all retrieved keys.
    Set<String> commonObjs;
    List<String> sortedCommonObjs;

    List<WorkingPartition2> wps;

    int maxPartitionNum;

    int[] windowScoreArr;
    int startWindow;
    int baseIdx;

    int partitionSize;
    int partitionNum;
    int w;

    int objNum;
    int minScore;

    boolean optimize;

    Set<BitSet> computedBitset = new HashSet<>();

    public WorkingPW(PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
            BitsetPartitionPayload<PayloadWMaskDewey>> pw, int maxPartitionNum,
                     int partitionSize, int partitionNum, int w, int objNum, boolean optimize) {
        this.optimize = optimize;
        this.pw = pw;
        this.wps = pw.getPartitions().stream().map(i ->
                new WorkingPartition2(i)).collect(Collectors.toList());

        // get all retrieved keys.
        commonObjs = pw.selectCommonObjects();
        sortedCommonObjs = new ArrayList<>(commonObjs);
        Collections.sort(sortedCommonObjs);
        // construct mapping
        for (WorkingPartition2 wp : wps) {
            BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>>
                    p = wp.getBasePartition();
            BitSet bitset = new BitSet(p.getObjs().size());
            for (int i= 0; i < p.getObjs().size(); i++) {
                bitset.set(i, commonObjs.contains(p.getObjs().get(i)));
            }
            wp.setCommonMask(bitset);
            // compute mapping.
            int[] mapArr = new int[p.getObjs().size()];
            for (int i =0; i < mapArr.length; i++) {
                String s = p.getObjs().get(i);
                mapArr[i] = sortedCommonObjs.indexOf(s);
            }
            wp.setMapping(mapArr);
        }
        this.maxPartitionNum = maxPartitionNum;
        // update remaining max.
        this.remainingMax = Utils.computePossibleMax(wps,maxPartitionNum);

        this.partitionNum = partitionNum;
        this.partitionSize = partitionSize;
        this.w = w;
//        this.startWindow = WindowComputer.computeStartWindow(pw.getStart(),
//                partitionSize, partitionNum, w);
        this.startWindow = 0;
        this.baseIdx = pw.getStart() + startWindow;
        windowScoreArr = IndexedUtils.initWindowScoreArr(pw, w, startWindow);

        this.objNum = objNum;
        this.minScore = 0;

//        System.out.println("starting idx:"+this.baseIdx);
//        System.out.println("window size:" + windowScoreArr.length);
    }

    public PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>,
            String, BitsetPartitionPayload<PayloadWMaskDewey>> getPw() {
        return pw;
    }

    public void setPw(PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>,
            String, BitsetPartitionPayload<PayloadWMaskDewey>> pw) {
        this.pw = pw;
    }

    public int getRemainingMax() {
        return remainingMax;
    }

    public void setRemainingMax(int remainingMax) {
        this.remainingMax = remainingMax;
    }

    public void workUntil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
//        System.out.println("working util");
        while(remainingMax >= stopScore && remainingMax > topkBookKeeper.getMin()) {
//            System.out.println("remainingmax:" + remainingMax);
            // sort the rest.
            Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

            // get the first one.
            WorkingPartition2 wp = wps.get(0);
            // get the first one.
            Node<String,PayloadWMaskDewey> currentNode = wp.nextNode(objNum);
            if (currentNode == null) {
                this.remainingMax = Utils.computePossibleMax(wps, maxPartitionNum);
//                System.out.println("b");
                break;
            }

            Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();

            boolean processed = false;

            if (commonObjs.contains(currentNode.getKey())) {
                // retrieve the same item from other partitions.
                List<String> objs = BitSetUtils.select(currentNode.getPayload().getMask(),
                        wp.getBasePartition().getObjs());
                LOG.debug("wp: [{}], processing {}", wp.getBasePartition().getStartFrame(), objs);
                // 1. filter
                objs.removeIf(i -> !commonObjs.contains(i));

                if (objs.size() >= objNum) {
                    processed = true;
                    Measurement.incProcessedCount();
                    // 1. split into multiple object sets.

                    BitSet currentMapped = BitSetUtils.mapTo(currentNode.getPayload().getMask(),
                            wp.getMapping());

                    if (computedBitset.contains(currentMapped)) {
                        continue;
                    }

                    List<Map<BitSet, IntervalListWCount>> resultList = new ArrayList<>();
                    // add the first one.
                    Map<BitSet, IntervalListWCount> map1 = new HashMap<>();
                    map1.put(currentMapped, new IntervalListWCount(currentNode.getPayload().getIntervals(),
                            currentNode.getPayload().getCount()) );
                    resultList.add(map1);

                    // retrieve the current key.
                    for(int i = 1; i < wps.size(); i++) {
                        WorkingPartition2 visitingWp = wps.get(i);
                        List<Node<String, PayloadWMaskDewey>> currentKeyNodes =
                                visitingWp.getNodesWithKey(currentNode.getKey());
                        if (currentKeyNodes == null) continue;

                        // get all the rest.
//                        Map<String, List<Node<String, PayloadWMaskDewey>>> otherObjMap = new HashMap<>();
                        List<Node<String, PayloadWMaskDewey>> allNodes = new ArrayList<>();
                        for (String obj: objs) {
                            List<Node<String, PayloadWMaskDewey>> list = visitingWp.getNodesWithKey(obj);
                            if (list == null) continue;
                            // add
                            // filter
                            for (Node<String, PayloadWMaskDewey> node: list) {
                                // mapping.
                                BitSet currentBitSet = node.getPayload().getMask();
                                if (currentBitSet.cardinality() < objNum || computedBitset.contains(currentBitSet)) continue;
                                allNodes.add(node);
                            }

                            // filter
//                            otherObjMap.put(obj, list);
                        }

//                        Collections.sort(allNodes,
//                                Comparator.comparing(x -> x.getPayload().getMask().cardinality()));

                        // compute bitset and.
                        LOG.debug("retrieving: {}", objs);

                        Map<BitSet, IntervalListWCount> thisMap = new HashMap<>();
                        resultList.add(thisMap);
                        for (Node<String, PayloadWMaskDewey> node: allNodes) {
                            // 3. compute bitset and.
                            BitSet currentBitSet = node.getPayload().getMask();
                            if (currentBitSet.cardinality() < objNum)  continue;
                            // compute intersections.
                            BitSet visitingMapped = BitSetUtils.mapTo(currentBitSet,
                                    visitingWp.getMapping());
                            // skip is processed.
                            if (computedBitset.contains(visitingMapped) ||
                                    visitingMapped.cardinality() < objNum) continue;

                            visitingMapped.and(currentMapped);

                            Measurement.incAndCount();

                            LOG.debug("[{}], generating bitset: {}", visitingWp.getBasePartition().getStartFrame(), visitingMapped);
//                                if (visitingMapped.cardinality() == currentMapped.cardinality()) {
                            if (visitingMapped.cardinality() >= objNum &&
                                    !computedBitset.contains(visitingMapped)) {
                                IntervalListWCount count = thisMap.get(visitingMapped);
                                if (count == null) {
                                    thisMap.put(visitingMapped, new IntervalListWCount(node.getPayload().getIntervals(),
                                            node.getPayload().getCount()));
                                } else if (count.getCount() < node.getPayload().getCount()){
                                    count.setCount(node.getPayload().getCount());
                                    count.setIntervals(node.getPayload().getIntervals());
                                }
                            }

                        }
//                        System.out.println("retrieved nodes: " + allNodes.size());
                        // 2. sort according to size.
                        // expecting bitset.
//                        System.out.println("processed nodes:" + processed);
                    }

                    // collect all object sets ?

                    List<List<Map.Entry<BitSet, IntervalListWCount>>> sortedLists = new ArrayList<>();
                    // concat.
                    for (int i = 0; i < resultList.size(); i++) {
                        // compute entries.
                        Map<BitSet, IntervalListWCount> map = resultList.get(i);
                        List<Map.Entry<BitSet, IntervalListWCount>> bitsetList = new ArrayList<>(map.entrySet());
                        Collections.sort(bitsetList, (x1, x2)-> Integer.compare(x2.getValue().getCount(), x1.getValue().getCount()));
                        sortedLists.add(bitsetList);
                    }
//                    Set<BitSet> computed = new HashSet<>();
                    Set<BitSet> computed = computedBitset;

                    // do we really need the following? seems so.
                    for (int i =0; i < resultList.size(); i++) {
                        // compute entries.
                        for (Map.Entry<BitSet, IntervalListWCount> entry: sortedLists.get(i)) {
                            if (optimize && entry.getKey().cardinality() != objNum) continue;
                            // 1. add to interval list.
                            if (!computed.contains(entry.getKey())) {
                                // process
                                List<String> keyObjs = BitSetUtils.select(entry.getKey(), sortedCommonObjs);
                                Set<String> keySet = new HashSet<>(keyObjs);
                                intervalMap.computeIfAbsent(keySet, x -> new ArrayList<>()).addAll(entry.getValue().getIntervals());

                                // compute the rest.
                                for (int j =0; j < resultList.size(); j++) {
                                    if (j == i) continue;
                                    for (Map.Entry<BitSet, IntervalListWCount> anotherEntry : sortedLists.get(j)) {
                                        BitSet bitSet = new BitSet();
                                        bitSet.or(anotherEntry.getKey());
                                        bitSet.and(entry.getKey());

                                        Measurement.incAndCount2();

                                        if (bitSet.cardinality() == entry.getKey().cardinality()) {
                                            intervalMap.get(keySet).addAll(anotherEntry.getValue().getIntervals());
                                            break;
                                        }
                                    }
                                }
                                // add to processed.
                                computed.add(entry.getKey());
                            }
                        }
                    }
                }
            }
            if (!processed && currentNode.getPayload().getMask().cardinality() == objNum) {
                Set<String> objs = new HashSet<>(
                        BitSetUtils.select(currentNode.getPayload().getMask(), wp.getBasePartition().getObjs())
                );
                // add
                Measurement.incProcessedCount();
                intervalMap.put(objs, currentNode.getPayload().getIntervals());
            }
            // sort & compute window score.
            List<IntervalListWCount> allList = new ArrayList<>();
            for (Map.Entry<Set<String>, List<Interval>> entry : intervalMap.entrySet()) {
                allList.add(new IntervalListWCount(entry.getKey(), entry.getValue()));
            }
            Collections.sort(allList, (x1, x2) -> Integer.compare(x2.getCount(), x1.getCount()));

//            if (this.pw.getStart() == 3000) {
//                System.out.println("INpsect");
//            }
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
//                        if (score == 7) {
//                            System.out.println("it");
//                        }
                    }
                    minScore = currentMin;
//                    minScore = IndexedUtils.getKthFromArray(windowScoreArr, 1);
                }
            }
            // update remaining count.
            this.remainingMax = Utils.computePossibleMax(wps, maxPartitionNum);
//            if (this.pw.getStart() == 3000) {
//                System.out.println("remaining max:" + this.remainingMax);
//            }
        }
        // update windowArr.
        IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);
    }

    public void reportRemaining() {
        for (WorkingPartition2 wp: wps) {
            System.out.println("p:"+wp.getBasePartition().getStartFrame() + ", remaining:" + wp.getRemainingCount());
        }
    }
}
