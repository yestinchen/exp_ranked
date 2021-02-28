package com.topk.offline.builder.partition;

import com.topk.offline.bean.*;
import com.topk.utils.Comparator;

import java.util.*;

public class BitsetCompositePartitionCreatorSim
        implements PartitionCreator<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>,BitsetPartitionPayload<PayloadWMaskDewey>>{

    Map<CLabel, List<String>> orderedObjs;
    List<String> allOrderedObjs;

    public BitsetCompositePartitionCreatorSim(
            Map<CLabel, List<String>> orderedObjs,
            List<String> allOrderedObjs) {
        this.orderedObjs = orderedObjs;
        this.allOrderedObjs = allOrderedObjs;
    }

    @Override
    public BasePartition<Node<String, PayloadWMaskDewey>,BitsetPartitionPayload<PayloadWMaskDewey>> createPartition(
            List<Node<String, PayloadWMaskDewey>> roots, List<Set<String>> connectedObjList,
            List<Set<String>> list, Map<Integer, Integer> top1Map, int startFrame, int size) {
        BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>> partition =
                new BasePartition<>();
        // construct map
        Map<CLabel, BitSet> bitSetMap = new HashMap<>();
        for (CLabel label: orderedObjs.keySet()) {
            BitSet bitSet = new BitSet();
            for (String s : orderedObjs.get(label)) {
                bitSet.set(allOrderedObjs.indexOf(s), true);
            }
            bitSetMap.put(label, bitSet);
        }
        partition.setTypeBitSetMap(bitSetMap);
        partition.setObjs(allOrderedObjs);
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);

        List<Node<String, PayloadWMaskDewey>> sortedNodes = new ArrayList<>();

        // construct the index.
        Map<String, List<Node<String, PayloadWMaskDewey>>> nodeMap = new HashMap<>();
        List<Node<String, PayloadWMaskDewey>> toVisit = new ArrayList<>();
        toVisit.addAll(roots);
        while (toVisit.size() > 0) {
            Node<String,  PayloadWMaskDewey> n = toVisit.remove(0);
            nodeMap.computeIfAbsent(n.getKey(), x -> new ArrayList<>()).add(n);
            sortedNodes.add(n);
            if (n.getNext() != null) {
                toVisit.addAll(n.getNext());
            }
        }
        BitsetPartitionPayload<PayloadWMaskDewey> payload = new BitsetPartitionPayload<>();
        // sort node.
        for (String key: nodeMap.keySet()) {
            Collections.sort(nodeMap.get(key), (x1, x2) ->
                    Comparator.compareDeweyNumbers(x1.getPayload().getDeweyNumber(),
                            x2.getPayload().getDeweyNumber()));
        }
        payload.setMap(nodeMap);
        // sort all nodes.
        Collections.sort(sortedNodes, (x1, x2)->
            -Integer.compare(x1.getPayload().getCount(), x2.getPayload().getCount())
        );
        partition.setRoots(sortedNodes);
        partition.setPayload(payload);
        return partition;
    }

}
