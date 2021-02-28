package com.topk.offline.builder.indexed;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.STNodeWIntervalCreator;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.offline.builder.partition.IntervalIndexedPartitionCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class IntervalIndexedSingleBuilder {

    public List<BasePartition<Node<String, PayloadIntervals>,
            IndexedPartitionPayload>> build(List<Set<String>> list, int partitionSize) {
        List<BasePartition<Node<String, PayloadIntervals>,
                IndexedPartitionPayload>> partitions = new ArrayList<>();
        for (int i =0; i < list.size(); i+=partitionSize) {
            PartitionIndexBootstrap<String, PayloadIntervals, Node<String, PayloadIntervals>,
                    IndexedPartitionPayload> pi = new PartitionIndexBootstrap<>(
                    i, new STNodeWIntervalCreator(), new IntervalIndexedPartitionCreator()
            );
            int bound = Math.min(list.size(), i+partitionSize);
            partitions.add(pi.build(list.subList(i, bound)));
        }
        return partitions;
    }
}
