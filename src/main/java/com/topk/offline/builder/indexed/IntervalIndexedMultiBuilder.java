package com.topk.offline.builder.indexed;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;

import java.util.*;

public class IntervalIndexedMultiBuilder {

    Map<CLabel, List<BasePartition<Node<String, PayloadIntervals>,
                IndexedPartitionPayload>>> typeMap = new HashMap<>();

    IntervalIndexedSingleBuilder singleBuilder = new IntervalIndexedSingleBuilder();

    int partitionSize;

    public IntervalIndexedMultiBuilder(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    public void build(CLabel type, List<Set<String>> frames) {
        typeMap.put(type, singleBuilder.build(frames, partitionSize));
    }

    public List<List<BasePartition<Node<String, PayloadIntervals>,
            IndexedPartitionPayload>>> retrieve(List<String> types) {
        List<List<BasePartition<Node<String, PayloadIntervals>, IndexedPartitionPayload>>> list = new ArrayList<>();
        for (String type: types) {
            list.add(typeMap.get(type));
        }
        return list;
    }
}
