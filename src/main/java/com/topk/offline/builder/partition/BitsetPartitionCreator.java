package com.topk.offline.builder.partition;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWDewey;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.utils.Comparator;

import java.util.*;

public class BitsetPartitionCreator
        implements PartitionCreator<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>,BitsetPartitionPayload<PayloadWMaskDewey>>{

    List<String> orderedObjs;
    boolean keepGraph = false;

    public BitsetPartitionCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    public BitsetPartitionCreator(List<String> orderedObjs, boolean keepGraph) {
        this.orderedObjs = orderedObjs;
        this.keepGraph = keepGraph;
    }

    @Override
    public BasePartition<Node<String, PayloadWMaskDewey>,BitsetPartitionPayload<PayloadWMaskDewey>> createPartition(
            List<Node<String, PayloadWMaskDewey>> roots, List<Set<String>> connectedObjList,
            List<Set<String>> list, Map<Integer, Integer> top1Map, int startFrame, int size) {
//        if (startFrame == 3000) {
//            System.out.println("ok");
//        }
        BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>> partition = new BasePartition<>();
        partition.setObjs(orderedObjs);
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
        int id = 0;
        for (Node<String, PayloadWMaskDewey>  node: sortedNodes) {
            node.setId(id++);
        }
        if (keepGraph) {
            partition.setRoots(roots);
        } else {
            partition.setRoots(sortedNodes);
        }
        partition.setPayload(payload);
        return partition;
    }

}
