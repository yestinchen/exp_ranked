package com.topk.offline.bitset;

import com.topk.offline.bean.*;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.CTNodeWMaskMapCreator;
import com.topk.offline.builder.partition.BitsetCompositePartitionCreator;
import com.topk.offline.builder.partition.BitsetPartitionCreator;
import com.topk.offline.builder.partition.BitsetPartitionPayload;

import java.util.*;

public class BitsetCompositeIndexBuilder {

    public List<BasePartition<Node<String, PayloadWMaskMap>, BitsetPartitionPayload<PayloadWMaskMap>>> build(
            List<Set<String>> list, Map<String, CLabel> typeMap, int partitionSize) {
        List<BasePartition<Node<String, PayloadWMaskMap>, BitsetPartitionPayload<PayloadWMaskMap>>> partitions =
                new ArrayList<>();
        for (int i =0; i < list.size(); i+= partitionSize) {
            int bound = Math.min(list.size(), i + partitionSize);
            List<Set<String>> subList = list.subList(i, bound);
            // group map
            Map<CLabel, Set<String>> typeObjMap = new HashMap<>();
            for (Set<String> frame: subList) {
                for (String obj: frame) {
                    typeObjMap.computeIfAbsent(typeMap.get(obj),
                            x -> new HashSet<>()).add(obj);
                }
            }
            Map<CLabel, List<String>> sortedObjMap = new HashMap<>();
            for (CLabel label : typeObjMap.keySet()) {
                List<String> objList = new ArrayList<>(typeObjMap.get(label));
                Collections.sort(objList);
                sortedObjMap.put(label, objList);
            }
            PartitionIndexBootstrap<String, PayloadWMaskMap, Node<String, PayloadWMaskMap>,
                    BitsetPartitionPayload<PayloadWMaskMap>> pi = new PartitionIndexBootstrap<>(i,
                    new CTNodeWMaskMapCreator(sortedObjMap), new BitsetCompositePartitionCreator(sortedObjMap));
            partitions.add(pi.build(subList, typeMap));
        }
        return partitions;
    }

}
