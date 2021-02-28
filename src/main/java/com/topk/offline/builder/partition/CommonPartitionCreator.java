package com.topk.offline.builder.partition;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;

import java.util.*;

public class CommonPartitionCreator<T, F> implements PartitionCreator<T, F, Node<T, F>, Byte>{

    @Override
    public BasePartition<Node<T, F>, Byte> createPartition(List<Node<T, F>> roots, List<Set<String>> connectedObjList,
                                                           List<Set<String>> list, Map<Integer, Integer> top1Map, int startFrame, int size) {

        BasePartition<Node<T, F>, Byte> partition = new BasePartition<>();
        partition.setRoots(roots);
        Set<String> allObjs = new HashSet<>();
        for (Set<String> s : list) {
            allObjs.addAll(s);
        }
        partition.setObjs(new ArrayList<>(allObjs));
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);
        return partition;
    }
}
