package com.topk.offline.bitset;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.STCompactNodeWDeweyCreator;
import com.topk.offline.builder.node.STNodeWDeweyCreator;
import com.topk.offline.builder.partition.BitsetPartitionCreator;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.offline.builder.partition.CompactBitsetPartitionCreator;
import com.topk.offline.builder.partition.CompactBitsetPartitionPayload;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class BitsetCompactSingleIndexBuilder {

    public List<BasePartition<Node<List<String>, PayloadWMaskDewey>, CompactBitsetPartitionPayload<PayloadWMaskDewey>>> build(
            List<Set<String>> list, int partitionSize) {
        List<BasePartition<Node<List<String>, PayloadWMaskDewey>, CompactBitsetPartitionPayload<PayloadWMaskDewey>>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += partitionSize) {
            int bound = Math.min(list.size(), i+partitionSize);
            // get all objects.
            Set<String> allObjs = new HashSet<>();
            for (Set<String> objs: list.subList(i, bound)) {
                allObjs.addAll(objs);
            }
            List<String> orderedObjs = new ArrayList<>(allObjs);
            Collections.sort(orderedObjs);

            PartitionIndexBootstrap<List<String>, PayloadWMaskDewey, Node<List<String>, PayloadWMaskDewey>,
                    CompactBitsetPartitionPayload<PayloadWMaskDewey>> pi =
                    new PartitionIndexBootstrap<>(i, new STCompactNodeWDeweyCreator(orderedObjs),
                            new CompactBitsetPartitionCreator(orderedObjs));

            partitions.add(pi.build(list.subList(i, bound)));
        }
        return partitions;
    }

    public static void main(String[] args) {

        BitsetCompactSingleIndexBuilder indexBuilder = new BitsetCompactSingleIndexBuilder();
        List<BasePartition<Node<List<String>, PayloadWMaskDewey>, CompactBitsetPartitionPayload<PayloadWMaskDewey>>> partitions = indexBuilder.build(Arrays.asList(
                new HashSet<>(Arrays.asList("a", "c", "d", "e", "f", "g")),
                new HashSet<>(Arrays.asList("a", "b", "d")),
                new HashSet<>(Arrays.asList("a", "c"))
        ), 3);

        System.out.println("partitions:" + partitions.size());
        for (BasePartition<Node<List<String>, PayloadWMaskDewey>,
                CompactBitsetPartitionPayload<PayloadWMaskDewey>> p: partitions) {
            for (Node<List<String>, PayloadWMaskDewey> node: p.getRoots()) {
                System.out.println("Root:" + node.getKey()+";"+ IntervalUtils.count(node.getPayload().getIntervals()));
            }
        }
    }
}
