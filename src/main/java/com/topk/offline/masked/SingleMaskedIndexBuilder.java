package com.topk.offline.masked;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervalsWMask;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.STNodeWMaskCreator;
import com.topk.offline.builder.partition.MaskedPartitionCreator;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class SingleMaskedIndexBuilder {

    public List<BasePartition<Node<String, PayloadIntervalsWMask>, Byte>> build(List<Set<String>> list, int partitionSize) {
        List<BasePartition<Node<String, PayloadIntervalsWMask>, Byte>> partitions = new ArrayList<>();
        for (int i =0; i < list.size(); i+=partitionSize) {
            int bound = Math.min(list.size(), i+partitionSize);
            // get all objects.
            Set<String> allObjs = new HashSet<>();
            for (Set<String> objs: list.subList(i, bound)) {
                allObjs.addAll(objs);
            }
            List<String> orderedObjs = new ArrayList<>(allObjs);
            Collections.sort(orderedObjs);

            PartitionIndexBootstrap<String, PayloadIntervalsWMask, Node<String, PayloadIntervalsWMask>, Byte> pi =
                    new PartitionIndexBootstrap<>(
                            i, new STNodeWMaskCreator(orderedObjs), new MaskedPartitionCreator<>(orderedObjs)
                    );
            partitions.add(pi.build(list.subList(i, bound)));
        }
        return partitions;
    }

    public static void main(String[] args) {
        SingleMaskedIndexBuilder indexBuilder = new SingleMaskedIndexBuilder();
        List<BasePartition<Node<String, PayloadIntervalsWMask>, Byte>> partitions = indexBuilder.build(Arrays.asList(
                new HashSet<>(Arrays.asList("A", "B", "C")),
                new HashSet<>(Arrays.asList("A", "B", "D")),
                new HashSet<>(Arrays.asList("B", "D", "E")),
                new HashSet<>(Arrays.asList("D", "E", "F"))
        ), 2);

        System.out.println("partitions:" + partitions.size());
        for (BasePartition<Node<String, PayloadIntervalsWMask>, Byte> p: partitions) {
            for (Node<String, PayloadIntervalsWMask> node: p.getRoots()) {
                System.out.println("Root:" + node.getKey()+";"+ IntervalUtils.count(node.getPayload().getIntervals()));
            }
        }
    }
}
