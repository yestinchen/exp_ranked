package com.topk.offline.builder.indexed;

import com.topk.offline.bean.*;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.CTNodeWIntervalCreator;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.offline.builder.partition.IntervalIndexedPartitionCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IntervalIndexedCompositeBuilder {

    public List<BasePartition<Node<String, PayloadClassIntervals>, IndexedPartitionPayload>> build(
            List<Set<String>> list, Map<String, CLabel> typeMap, int partitionSize) {
        List<BasePartition<Node<String, PayloadClassIntervals>, IndexedPartitionPayload>> partitions =
                new ArrayList<>();
        for (int i=0; i < list.size(); i+= partitionSize) {
            PartitionIndexBootstrap<String, PayloadClassIntervals,
                    Node<String, PayloadClassIntervals>, IndexedPartitionPayload> pi =
                    new PartitionIndexBootstrap<>(i,
                            new CTNodeWIntervalCreator(), new IntervalIndexedPartitionCreator());
            int bound = Math.min(list.size(), i + partitionSize);
            partitions.add(pi.build(list.subList(i, bound), typeMap));
        }
        return partitions;
    }

}
