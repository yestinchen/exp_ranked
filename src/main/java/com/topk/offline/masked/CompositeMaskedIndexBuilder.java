package com.topk.offline.masked;

import com.common.bean.Tuple2;
import com.topk.offline.bean.*;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.CTNodeWMaskCreator;
import com.topk.offline.builder.partition.MaskedPartitionCreator;

import java.util.*;

public class CompositeMaskedIndexBuilder {

    public List<BasePartition<Node<String, PayloadClassIntervalsWMask>, Byte>> build(
            List<Set<String>> list, Map<String, CLabel> typeMap, int partitionSize) {
        List<BasePartition<Node<String, PayloadClassIntervalsWMask>, Byte>> partitions = new ArrayList<>();
        for (int i =0; i < list.size(); i+=partitionSize) {
            int bound = Math.min(list.size(), i+partitionSize);
            // get all objects.
            Set<String> allObjs = new HashSet<>();
            for (Set<String> objs: list.subList(i, bound)) {
                allObjs.addAll(objs);
            }
            List<String> orderedObjs = new ArrayList<>(allObjs);
            Collections.sort(orderedObjs);
            PartitionIndexBootstrap<String, PayloadClassIntervalsWMask, Node<String, PayloadClassIntervalsWMask>, Byte>
                    pi = new PartitionIndexBootstrap<>(i, new CTNodeWMaskCreator(orderedObjs), new MaskedPartitionCreator<>(orderedObjs));
            partitions.add(pi.build(list.subList(i, bound), typeMap));
        }
        return partitions;
    }

}
