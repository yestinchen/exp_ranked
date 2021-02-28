package com.topk.test.builder;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.builder.PartitionIndexBootstrap;
import com.topk.offline.builder.node.CTNodeWIntervalCreator;
import com.topk.offline.builder.partition.CommonPartitionCreator;

import java.util.*;

public class PartitionBuilderTest1 {

    public static void main(String[] args) {


        PartitionIndexBootstrap<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>, Byte> partitionIndex =
                new PartitionIndexBootstrap<>(0, new CTNodeWIntervalCreator(), new CommonPartitionCreator<>());
        List<Set<String>> objFrams = Arrays.asList(
                new HashSet<>(Arrays.asList("A", "B", "C", "D")),
                new HashSet<>(Arrays.asList("A", "B", "C", "D")),
                new HashSet<>(Arrays.asList("A", "B", "C")),
                new HashSet<>(Arrays.asList("A", "B"))
        );
        Map<String, CLabel> typeMap = new HashMap<>();
        for (Set<String> ids : objFrams) {
            for (String id : ids) {
                if (Character.isUpperCase(id.charAt(0))) {
                    typeMap.put(id, CLabel.CAR);
                } else {
                    typeMap.put(id, CLabel.PERSON);
                }
            }
        }
        List<Node<String, PayloadClassIntervals>> roots= partitionIndex.build(objFrams, typeMap).getRoots();
//        List<NodeWithIntervals> roots= partitionIndex.build(Arrays.asList(
//                new HashSet<>(Arrays.asList("A", "C")),
//                new HashSet<>(Arrays.asList("A", "B")),
//                new HashSet<>(Arrays.asList("B", "D"))
//        )).getRoots();
        for (Node<String, PayloadClassIntervals> node: roots) {
            System.out.println("Root:" + node.getKey()+";"+ node.getPayload().getCount());
        }
    }
}
