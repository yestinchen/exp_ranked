package com.topk.offline.builder.partition;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWDewey;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.utils.Comparator;

import java.util.*;

public class CompactBitsetPartitionCreator
        implements PartitionCreator<List<String>, PayloadWMaskDewey, Node<List<String>, PayloadWMaskDewey>,
        CompactBitsetPartitionPayload<PayloadWMaskDewey>>{

    List<String> orderedObjs;

    public CompactBitsetPartitionCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public BasePartition<Node<List<String>, PayloadWMaskDewey>, CompactBitsetPartitionPayload<PayloadWMaskDewey>> createPartition(
            List<Node<List<String>, PayloadWMaskDewey>> roots, List<Set<String>> connectedObjList,
            List<Set<String>> list, Map<Integer, Integer> top1Map, int startFrame, int size) {
//        if (startFrame == 3000) {
//            System.out.println("ok");
//        }
        BasePartition<Node<List<String>, PayloadWMaskDewey>, CompactBitsetPartitionPayload<PayloadWMaskDewey>>
                partition = new BasePartition<>();
        partition.setObjs(orderedObjs);
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);

        List<Node<List<String>, PayloadWMaskDewey>> sortedNodes = new ArrayList<>();

        // construct the index.
        Map<String, List<Node<List<String>, PayloadWMaskDewey>>> nodeMap = new HashMap<>();
        List<Node<List<String>, PayloadWMaskDewey>> toVisit = new ArrayList<>();
        toVisit.addAll(roots);
        while (toVisit.size() > 0) {
            Node<List<String>,  PayloadWMaskDewey> n = toVisit.remove(0);
            for (String key: n.getKey()) {
                nodeMap.computeIfAbsent(key, x -> new ArrayList<>()).add(n);
            }
            sortedNodes.add(n);
            if (n.getNext() != null) {
                toVisit.addAll(n.getNext());
            }
        }
        CompactBitsetPartitionPayload<PayloadWMaskDewey> payload = new CompactBitsetPartitionPayload<>();
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
        int id = 0;
        for (Node<List<String>, PayloadWMaskDewey>  node: sortedNodes) {
            node.setId(id++);
        }
        partition.setRoots(sortedNodes);
        partition.setPayload(payload);
        return partition;
    }

}
