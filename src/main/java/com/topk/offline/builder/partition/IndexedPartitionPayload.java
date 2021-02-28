package com.topk.offline.builder.partition;

import com.topk.bean.Interval;

import java.util.List;
import java.util.Map;

public class IndexedPartitionPayload {
    Map<String, List<Interval>> intervalMap;

    Map<String, Integer> nodeCountMap;

    public Map<String, List<Interval>> getIntervalMap() {
        return intervalMap;
    }

    public void setIntervalMap(Map<String, List<Interval>> intervalMap) {
        this.intervalMap = intervalMap;
    }

    public Map<String, Integer> getNodeCountMap() {
        return nodeCountMap;
    }

    public void setNodeCountMap(Map<String, Integer> nodeCountMap) {
        this.nodeCountMap = nodeCountMap;
    }
}
