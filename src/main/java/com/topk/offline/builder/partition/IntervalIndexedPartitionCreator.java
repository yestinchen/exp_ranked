package com.topk.offline.builder.partition;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class IntervalIndexedPartitionCreator<T extends PayloadIntervals> implements
        PartitionCreator<String, T, Node<String, T>, IndexedPartitionPayload> {

    @Override
    public BasePartition<Node<String, T>, IndexedPartitionPayload> createPartition(
            List<Node<String, T>> roots, List<Set<String>> connectedObjList, List<Set<String>> list,
            Map<Integer, Integer> top1Map, int startFrame, int size) {
        BasePartition<Node<String, T>, IndexedPartitionPayload> partition = new BasePartition<>();
        partition.setRoots(roots);
        Set<String> allObjs = new HashSet<>();
        for (Set<String> s : list) {
            allObjs.addAll(s);
        }
        partition.setObjs(new ArrayList<>(allObjs));
        partition.setTop1Map(top1Map);
        partition.setStartFrame(startFrame);
        partition.setSize(size);

        // payload: object -> interval list.
        Map<String, List<Interval>> nodeMap = new HashMap<>();
        List<Node<String, T>> toVisit = new ArrayList<>();
        toVisit.addAll(roots);
        Map<String, Integer> countMap = new HashMap<>();
        while(toVisit.size() > 0) {
            Node<String, T> n = toVisit.remove(0);
            int count = countMap.getOrDefault(n.getKey(), 0);
            count ++;
            countMap.put(n.getKey(), count);
            List<Interval> existingOne = nodeMap.get(n.getKey());
            if (existingOne == null ||
                    IntervalUtils.count(existingOne) < IntervalUtils.count(n.getPayload().getIntervals())) {
                nodeMap.put(n.getKey(), n.getPayload().getIntervals());
            }
            if (n.getNext() != null) {
                toVisit.addAll(n.getNext());
            }
        }
        IndexedPartitionPayload payload = new IndexedPartitionPayload();
        payload.setIntervalMap(nodeMap);
        payload.setNodeCountMap(countMap);

        partition.setPayload(payload);
        return partition;
    }
}
