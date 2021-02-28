package com.topk.offline.bitset;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.STNodeWDeweyCreator;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.offline.builder.partition.BitsetPartitionCreator;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class BitsetSingleIndexBuilder {

    boolean keepGraph = false;

    public BitsetSingleIndexBuilder() { }

    public BitsetSingleIndexBuilder(Boolean keepGraph) {
        this.keepGraph = keepGraph;
    }

    public List<BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>>> build(
            List<Set<String>> list, int partitionSize) {
        List<BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += partitionSize) {
            int bound = Math.min(list.size(), i+partitionSize);
            // get all objects.
            Set<String> allObjs = new HashSet<>();
            for (Set<String> objs: list.subList(i, bound)) {
                allObjs.addAll(objs);
            }
            List<String> orderedObjs = new ArrayList<>(allObjs);
            Collections.sort(orderedObjs);

            PartitionIndexBootstrap<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>,
                    BitsetPartitionPayload<PayloadWMaskDewey>> pi =
                    new PartitionIndexBootstrap<>(i, new STNodeWDeweyCreator(orderedObjs),
                            new BitsetPartitionCreator(orderedObjs, keepGraph));

            partitions.add(pi.build(list.subList(i, bound)));
        }
        return partitions;
    }

    public static void main(String[] args) {

        BitsetSingleIndexBuilder indexBuilder = new BitsetSingleIndexBuilder();
        List<BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>>> partitions = indexBuilder.build(Arrays.asList(
//                new HashSet<>(Arrays.asList("A", "B", "C")),
//                new HashSet<>(Arrays.asList("A", "B", "D")),
//                new HashSet<>(Arrays.asList("B", "D", "E")),
//                new HashSet<>(Arrays.asList("D", "E", "F"))
                new HashSet<>(Arrays.asList("a", "c", "d", "e", "f", "g")),
                new HashSet<>(Arrays.asList("a", "b", "d")),
                new HashSet<>(Arrays.asList("a", "c"))
        ), 3);

        System.out.println("partitions:" + partitions.size());
        for (BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>> p: partitions) {
            for (Node<String, PayloadWMaskDewey> node: p.getRoots()) {
                System.out.println("Root:" + node.getKey()+";"+ IntervalUtils.count(node.getPayload().getIntervals()));
            }
        }
    }

}
