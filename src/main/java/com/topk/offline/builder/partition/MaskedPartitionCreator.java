package com.topk.offline.builder.partition;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;

import java.util.*;

public class MaskedPartitionCreator<T, F> implements PartitionCreator<T, F, Node<T, F>, Byte> {

    List<String> orderedObjs;

    public MaskedPartitionCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public BasePartition<Node<T, F>, Byte> createPartition(List<Node<T, F>> roots, List<Set<String>> connectedObjList, List<Set<String>> list,
                                                           Map<Integer, Integer> top1Map, int startFrame, int size) {

        BasePartition<Node<T, F>, Byte> partition = new BasePartition<>();
        // compute bitset for each root.
        List<Node<T, F>> tuples = new ArrayList<>(roots);
        partition.setRoots(tuples);
        partition.setObjs(orderedObjs);
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);
        return partition;
    }
}
