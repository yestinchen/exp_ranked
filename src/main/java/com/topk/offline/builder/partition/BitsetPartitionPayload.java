package com.topk.offline.builder.partition;

import com.topk.offline.bean.Node;

import java.util.List;
import java.util.Map;

public class BitsetPartitionPayload<T> {

    Map<String, List<Node<String, T>>> map;

    public Map<String, List<Node<String, T>>> getMap() {
        return map;
    }

    public void setMap(Map<String, List<Node<String, T>>> map) {
        this.map = map;
    }
}
