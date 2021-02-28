package com.topk.offline.builder.partition;

import com.topk.offline.bean.*;
import com.topk.utils.Comparator;

import java.util.*;

public class BitsetCompositePartitionCreator
        implements PartitionCreator<String, PayloadWMaskMap, Node<String, PayloadWMaskMap>,BitsetPartitionPayload<PayloadWMaskMap>>{

    Map<CLabel, List<String>> orderedObjs;

    public BitsetCompositePartitionCreator(Map<CLabel, List<String>> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public BasePartition<Node<String, PayloadWMaskMap>,BitsetPartitionPayload<PayloadWMaskMap>> createPartition(
            List<Node<String, PayloadWMaskMap>> roots, List<Set<String>> connectedObjList,
            List<Set<String>> list, Map<Integer, Integer> top1Map, int startFrame, int size) {
        BasePartition<Node<String, PayloadWMaskMap>, BitsetPartitionPayload<PayloadWMaskMap>> partition = new BasePartition<>();
//        partition.setObjsMap(orderedObjs);
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);

        List<Node<String, PayloadWMaskMap>> sortedNodes = new ArrayList<>();

        // construct the index.
        Map<String, List<Node<String, PayloadWMaskMap>>> nodeMap = new HashMap<>();
        List<Node<String, PayloadWMaskMap>> toVisit = new ArrayList<>();
        toVisit.addAll(roots);
        while (toVisit.size() > 0) {
            Node<String,  PayloadWMaskMap> n = toVisit.remove(0);
            nodeMap.computeIfAbsent(n.getKey(), x -> new ArrayList<>()).add(n);
            sortedNodes.add(n);
            if (n.getNext() != null) {
                toVisit.addAll(n.getNext());
            }
        }
        BitsetPartitionPayload<PayloadWMaskMap> payload = new BitsetPartitionPayload<>();
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
