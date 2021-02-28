package com.topk.online.processors.bitset.multi;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.bitset.IntervalListWCount;
import com.topk.online.processors.bitset.Utils;
import com.topk.online.processors.bitset.WorkingPW;
import com.topk.online.processors.bitset.WorkingPartition2;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.utils.BitSetUtils;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class WorkingSPW {


    private Logger LOG = LogManager.getLogger(WorkingSPW.class);
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

    Set<BitSet> computedBitset = new HashSet<>();

    int objNum;

    public WorkingSPW(PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
            BitsetPartitionPayload<PayloadWMaskDewey>> pw, int maxPartitionNum, int objNum) {
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
            if (p.getObjs() == null) {
                wp.setCommonMask(new BitSet());
                wp.setMapping(new int[0]);
            } else {
                BitSet bitset = new BitSet(p.getObjs().size());
                for (int i = 0; i < p.getObjs().size(); i++) {
                    bitset.set(i, commonObjs.contains(p.getObjs().get(i)));
                }
                wp.setCommonMask(bitset);
                // compute mapping.
                int[] mapArr = new int[p.getObjs().size()];
                for (int i = 0; i < mapArr.length; i++) {
                    String s = p.getObjs().get(i);
                    mapArr[i] = sortedCommonObjs.indexOf(s);
                }
                wp.setMapping(mapArr);
            }
        }
        this.maxPartitionNum = maxPartitionNum;
        // update remaining max.
        this.remainingMax = Utils.computePossibleMax(wps,maxPartitionNum);


        this.objNum = objNum;
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

    public Map<Set<String>, List<Interval>> next() {
        Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
        // sort the rest.
        Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

        // get the first one.
        WorkingPartition2 wp = wps.get(0);
        // get the first one.
        Node<String,PayloadWMaskDewey> currentNode = wp.nextNode(objNum);
        if (currentNode == null) return null;

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
                // 1. split into multiple object sets.

                BitSet currentMapped = BitSetUtils.mapTo(currentNode.getPayload().getMask(),
                        wp.getMapping());

                List<Map<BitSet, List<Interval>>> resultList = new ArrayList<>();
                // add the first one.
                Map<BitSet, List<Interval>> map1 = new HashMap<>();
                map1.put(currentMapped, currentNode.getPayload().getIntervals());
                resultList.add(map1);

                // retrieve the current key.
                for(int i = 1; i < wps.size(); i++) {
                    WorkingPartition2 visitingWp = wps.get(i);
                    List<Node<String, PayloadWMaskDewey>> currentKeyNodes =
                            visitingWp.getNodesWithKey(currentNode.getKey());
                    if (currentKeyNodes == null) continue;
                    // get all the rest.
                    Map<String, List<Node<String, PayloadWMaskDewey>>> otherObjMap = new HashMap<>();
                    for (String obj: objs) {
                        List<Node<String, PayloadWMaskDewey>> list = visitingWp.getNodesWithKey(obj);
                        // filter
                        otherObjMap.put(obj, list);
                    }

                    // compute bitset and.
                    LOG.debug("retrieving: {}", objs);
                    // 1. all all objects together.
                    List<Node<String, PayloadWMaskDewey>> allNodes = new ArrayList<>();
                    allNodes.addAll(currentKeyNodes);
                    for (Map.Entry<String, List<Node<String, PayloadWMaskDewey>>> entry: otherObjMap.entrySet()) {
                        if (entry.getValue() != null) {
                            List<Node<String, PayloadWMaskDewey>> list = entry.getValue();
                            for (Node<String, PayloadWMaskDewey> node : list) {
                                if (node.getPayload().getMask().cardinality() >= objNum) {
                                    allNodes.add(node);
                                }
                            }
                        }
                    }
                    // 2. sort according to size.
                    Collections.sort(allNodes,
                            Comparator.comparing(x -> x.getPayload().getMask().cardinality()));
                    // expecting bitset.
                    Set<BitSet> genedSet = new HashSet<>();
                    Map<BitSet, List<Interval>> thisMap = new HashMap<>();
                    resultList.add(thisMap);
                    // 3. compute bitset and.
                    for (Node<String, PayloadWMaskDewey> n : allNodes) {
                        BitSet currentBitSet = n.getPayload().getMask();
                        if (currentBitSet.cardinality() >= objNum) {
                            // compute intersections.
                            BitSet visitingMapped = BitSetUtils.mapTo(currentBitSet,
                                    visitingWp.getMapping());
                            visitingMapped.and(currentMapped);
                            LOG.debug("[{}], generating bitset: {}", visitingWp.getBasePartition().getStartFrame(), visitingMapped);
//                                if (visitingMapped.cardinality() == currentMapped.cardinality()) {
                            if (visitingMapped.cardinality() >= objNum) {
                                if (!genedSet.contains(visitingMapped)) {
                                    thisMap.put(visitingMapped, n.getPayload().getIntervals());
                                    genedSet.add(visitingMapped);
                                }
                            }
                        }
                    }
                }

                List<List<Map.Entry<BitSet, List<Interval>>>> sortedLists = new ArrayList<>();
                // concat.
                for (int i = 0; i < resultList.size(); i++) {
                    // compute entries.
                    Map<BitSet, List<Interval>> map = resultList.get(i);
                    List<Map.Entry<BitSet, List<Interval>>> bitsetList = new ArrayList<>(map.entrySet());
                    Collections.sort(bitsetList, (x1, x2)-> IntervalUtils.count(x2.getValue()) - IntervalUtils.count(x1.getValue()));
                    sortedLists.add(bitsetList);
                }
                for (int i =0; i < resultList.size(); i++) {
                    // compute entries.
                    for (Map.Entry<BitSet, List<Interval>> entry: sortedLists.get(i)) {
                        // 1. add to interval list.
                        if (!computedBitset.contains(entry.getKey())) {
                            // process
                            List<String> keyObjs = BitSetUtils.select(entry.getKey(), sortedCommonObjs);
                            Set<String> keySet = new HashSet<>(keyObjs);
                            intervalMap.computeIfAbsent(keySet, x -> new ArrayList<>()).addAll(entry.getValue());

                            // compute the rest.
                            for (int j =0; j < resultList.size(); j++) {
                                if (j == i) continue;
                                for (Map.Entry<BitSet, List<Interval>> anotherEntry : sortedLists.get(j)) {
                                    BitSet bitSet = new BitSet();
                                    bitSet.or(anotherEntry.getKey());
                                    bitSet.and(entry.getKey());
                                    if (bitSet.cardinality() == entry.getKey().cardinality()) {
                                        intervalMap.get(keySet).addAll(anotherEntry.getValue());
                                        break;
                                    }
                                }
                            }
                            // add to processed.
                            computedBitset.add(entry.getKey());
                        }
                    }
                }
            }
        }
        if (!processed && currentNode.getPayload().getMask().cardinality() == objNum) {
            // add
            intervalMap.put(new HashSet<>(
                    BitSetUtils.select(currentNode.getPayload().getMask(), wp.getBasePartition().getObjs())
            ), currentNode.getPayload().getIntervals());
        }

        this.remainingMax = Utils.computePossibleMax(wps, maxPartitionNum);
        return intervalMap;
    }
}
