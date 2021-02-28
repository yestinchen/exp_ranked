package com.topk.online.processors.bitset;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.PWGenWGroup;
import com.topk.online.component.WindowComputer;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import com.topk.utils.BitSetUtils;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class BitsetSingleTypeProcessor {

    private static Logger LOG = LogManager.getLogger(BitsetSingleTypeProcessor.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>> partitions;
    int partitionSize;

    public BitsetSingleTypeProcessor(List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>> partitions,
                                     int partitionSize) {
        this.partitions = partitions;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, int objNum) {
        //
        int partitionNum = (int) Math.ceil(w*1.0/partitionSize);

        List<PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>>> partitionWindows =
                PWGenWGroup.genPWs(partitions, objNum, partitionNum, partitionSize);
        for (PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>> pw : partitionWindows) {
            pw.estimateScore(objNum);
        }
        Collections.sort(partitionWindows, (x1, x2)->  - Integer.compare(x1.getScore(), x2.getScore()));

        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);
        for (PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>> pw: partitionWindows) {
            if (pw.getScore() <= topkBookKeeper.getMin()) {
                break;
            }
            // process this partition window.

            // 1. obtain all object sets with the same
            List<WorkingPartition2> wps = pw.getPartitions().stream().map(i ->
                    new WorkingPartition2(i)).collect(Collectors.toList());

            // get all retrieved keys.
            Set<String> commonObjs = pw.selectCommonObjects();
            // get mask for each partition.
            // get the mapping list

            List<String> sortedCommonObjs = new ArrayList<>(commonObjs);
            Collections.sort(sortedCommonObjs);
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

            int remaingPossibleMax = wps.stream().mapToInt(WorkingPartition2::getRemainingCount).sum();

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

            while (remaingPossibleMax > minScore) {
                LOG.debug("remaing possible max: {}, minScore: {}", remaingPossibleMax, minScore);
                // start working.
                Collections.sort(wps, (x1, x2) -> Integer.compare(x2.getRemainingCount(), x1.getRemainingCount()));

                WorkingPartition2 wp = wps.get(0);
                // get the first one.
                Node<String,PayloadWMaskDewey> currentNode = wp.nextNode(objNum);
                if (currentNode == null) break;

                Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
                if (commonObjs.contains(currentNode.getKey())) {
                    // retrieve the same item from other partitions.
                    List<String> objs = BitSetUtils.select(currentNode.getPayload().getMask(),
                            wp.getBasePartition().getObjs());
                    LOG.debug("wp: [{}], processing {}", wp.getBasePartition().getStartFrame(), objs);
                    // 1. filter
                    objs.removeIf(i -> !commonObjs.contains(i));

                    if (objs.size() >= objNum) {
                        // 1. split into multiple object sets.

                        BitSet currentMapped = BitSetUtils.mapTo(currentNode.getPayload().getMask(),
                                wp.getMapping());
                        // retrieve the current key.
                        for(int i = 1; i < wps.size(); i++) {
                            WorkingPartition2 visitingWp = wps.get(i);
                            List<Node<String, PayloadWMaskDewey>> currentKeyNodes =
                                    visitingWp.getNodesWithKey(currentNode.getKey());
                            if (currentKeyNodes == null) continue;
                            // get all the rest.
                            Map<String, List<Node<String, PayloadWMaskDewey>>> otherObjMap = new HashMap<>();
                            for (String obj: objs) {
                                otherObjMap.put(obj, visitingWp.getNodesWithKey(obj));
                            }

                            // compute bitset and.
                            LOG.debug("retrieving: {}", objs);
                            // 1. all all objects together.
                            List<Node<String, PayloadWMaskDewey>> allNodes = new ArrayList<>();
                            allNodes.addAll(currentKeyNodes);
                            for (Map.Entry<String, List<Node<String, PayloadWMaskDewey>>> entry: otherObjMap.entrySet()) {
                                if (entry.getValue() != null) {
                                    allNodes.addAll(entry.getValue());
                                }
                            }
                            // 2. sort according to size.
                            Collections.sort(allNodes,
                                    Comparator.comparing(x -> x.getPayload().getMask().cardinality()));
                            // 3. compute bitset and.
                            for (Node<String, PayloadWMaskDewey> n : allNodes) {
                                BitSet currentBitSet = n.getPayload().getMask();
                                if (currentBitSet.cardinality() >= objNum) {
                                    // compute intersections.
                                    BitSet visitingMapped = BitSetUtils.mapTo(currentBitSet,
                                            visitingWp.getMapping());
                                    visitingMapped.and(currentMapped);
                                    if (visitingMapped.cardinality() >= objNum) {
                                        // result.
                                        Set<String> set = new HashSet<>(BitSetUtils.select(visitingMapped, sortedCommonObjs));
                                        set.add(currentNode.getKey());
                                        if (intervalMap.get(set) == null) {
                                            // put
                                            intervalMap.put(set, new ArrayList<>());
                                            intervalMap.get(set).addAll(currentNode.getPayload().getIntervals());
                                        }
                                        intervalMap.get(set).addAll(n.getPayload().getIntervals());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else if (currentNode.getPayload().getMask().cardinality() == objNum) {
                    // add
                    intervalMap.put(new HashSet<>(
                            BitSetUtils.select(currentNode.getPayload().getMask(), wp.getBasePartition().getObjs())
                    ), currentNode.getPayload().getIntervals());
                }
                // sort & compute window score.
                List<IntervalListWCount> allList = new ArrayList<>();
                for (Map.Entry<Set<String>, List<Interval>> entry : intervalMap.entrySet()) {
                    allList.add(new IntervalListWCount(entry.getKey(), entry.getValue()));
                }
                Collections.sort(allList, (x1, x2) -> Integer.compare(x2.getCount(), x2.getCount()));
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
//                        if (score == 7) {
//                            System.out.println("it");
//                        }
                        }
                        minScore = currentMin;
                    }
                }

                // update remaining possible max.
                remaingPossibleMax = wps.stream().mapToInt(WorkingPartition2::getRemainingCount).sum();
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

}
