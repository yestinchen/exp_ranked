package com.topk.offline.builder.partition;

import com.topk.offline.bean.Node;

import java.util.List;
import java.util.Map;

public class CompactBitsetPartitionPayload <T> {

    Map<String, List<Node<List<String>, T>>> map;

    public Map<String, List<Node<List<String>, T>>> getMap() {
        return map;
    }

    public void setMap(Map<String, List<Node<List<String>, T>>> map) {
        this.map = map;
    }
}
