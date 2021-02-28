package com.topk.offline.composite;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.builder.node.CTNodeWIntervalCreator;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.partition.CommonPartitionCreator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompositeIndexBuilder {

    public List<BasePartition<Node<String, PayloadClassIntervals>, Byte>> build(
            List<Set<String>> list, Map<String, CLabel> typeMap, int partitionSize) {
        List<BasePartition<Node<String, PayloadClassIntervals>, Byte>> partitions = new ArrayList<>();
        for (int i =0; i < list.size(); i+=partitionSize) {
            PartitionIndexBootstrap<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>, Byte> pi =
                    new PartitionIndexBootstrap<>(i, new CTNodeWIntervalCreator(), new CommonPartitionCreator<>());
            int bound = Math.min(list.size(), i+partitionSize);
            partitions.add(pi.build(list.subList(i, bound), typeMap));
        }
        return partitions;
    }

}
