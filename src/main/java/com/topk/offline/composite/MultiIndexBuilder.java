package com.topk.offline.composite;

import com.topk.offline.*;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;

import java.util.*;

public class MultiIndexBuilder {

    Map<CLabel, List<BasePartition<Node<String, PayloadIntervals>, Byte>>> typeMap = new HashMap<>();

    SingleIndexBuilder indexBuilder = new SingleIndexBuilder();

    int partitionSize;

    public MultiIndexBuilder(int partitionSize) {
        this.partitionSize = partitionSize;
    }

    public void build(CLabel type, List<Set<String>> frames) {
        typeMap.put(type, indexBuilder.build(frames, partitionSize));
    }

    public List<List<BasePartition<Node<String, PayloadIntervals>, Byte>>> retrieve(List<String> types) {
        List<List<BasePartition<Node<String, PayloadIntervals>, Byte>>> list = new ArrayList<>();
        for (String type: types) {
            list.add(typeMap.get(type));
        }
        return list;
    }

    public Map<CLabel, List<BasePartition<Node<String, PayloadIntervals>, Byte>>> getTypeMap() {
        return typeMap;
    }
}
