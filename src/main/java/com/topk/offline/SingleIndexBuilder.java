package com.topk.offline;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.STNodeWIntervalCreator;
import com.topk.offline.builder.partition.CommonPartitionCreator;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class SingleIndexBuilder {

    public List<BasePartition<Node<String, PayloadIntervals>, Byte>> build(List<Set<String>> list, int partitionSize) {
        List<BasePartition<Node<String, PayloadIntervals>, Byte>> partitions = new ArrayList<>();
        for (int i =0; i < list.size(); i+=partitionSize) {
            PartitionIndexBootstrap<String, PayloadIntervals, Node<String, PayloadIntervals>, Byte> pi = new PartitionIndexBootstrap<>(
                    i, new STNodeWIntervalCreator(), new CommonPartitionCreator<>()
            );
            int bound = Math.min(list.size(), i+partitionSize);
            partitions.add(pi.build(list.subList(i, bound)));
        }
        return partitions;
    }

    public static void main(String[] args) {
        SingleIndexBuilder indexBuilder = new SingleIndexBuilder();
        List<BasePartition<Node<String, PayloadIntervals>, Byte>> partitions = indexBuilder.build(Arrays.asList(
                new HashSet<>(Arrays.asList("A", "B", "C")),
                new HashSet<>(Arrays.asList("A", "B", "D")),
                new HashSet<>(Arrays.asList("B", "D", "E")),
                new HashSet<>(Arrays.asList("D", "E", "F"))
        ), 2);

        System.out.println("partitions:" + partitions.size());
        for (BasePartition<Node<String, PayloadIntervals>, Byte> p: partitions) {
            for (Node<String, PayloadIntervals> node: p.getRoots()) {
                System.out.println("Root:" + node.getKey()+";"+ IntervalUtils.count(node.getPayload().getIntervals()));
            }
        }
    }
}
