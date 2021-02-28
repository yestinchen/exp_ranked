package com.topk.offline.bean;

import java.util.ArrayList;
import java.util.List;

public class PartitionCount {

    public static int countPartitionsNode(List<BasePartition<Node<String, PayloadClassIntervals>, Byte>> ps) {
        int c = 0;
        for (BasePartition<? extends Node, Byte> p : ps) {
            c += countPartitionNode(p);
        }
        return c;
    }

    public static int countPartitionNode(BasePartition<? extends Node, Byte> p) {
        List<Node> visiting = new ArrayList<>(p.roots);
        int count =0;
        while(visiting.size() != 0) {
            Node n = visiting.remove(0);
            count ++;
            if (n.getNext() != null) {
                visiting.addAll(n.getNext());
            }
        }
        return count;
    }
}
