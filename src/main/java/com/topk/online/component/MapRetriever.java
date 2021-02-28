package com.topk.online.component;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.PartitionWindow;
import com.topk.online.PrefixList;
import com.topk.online.interval.PrefixListWithInterval;

import java.util.*;

public class MapRetriever<K> {


    Map<Set<K>, List<Interval>> intervalMap = null;
    // a: compute the score for partitions in the middle.
    // compute base count for all ... partitions [start partition, ..., end partition]
    Map<Set<K>, Integer> baseCountMap = null;


    public <T, F extends PayloadIntervals, P, Q> void retrieveMaps(PartitionWindow<T, F, P, K, Q> pw,
                             Set<Set<K>> retrievedPrefixes,
                             int partitionNum, int partitionSize, int w) {
        intervalMap = new HashMap<>();
        baseCountMap = new HashMap<>();
        int maxCountPartition = 0; // the max value for frames in the middle partitions.

        // 1. generate x & y.
        int px = w / partitionSize - 1;
        int py = partitionNum - px - 1;

        // split
        int xframe = pw.getStart() + px * partitionSize, yframe = pw.getStart() + py * partitionSize;
        for (int j =0; j < pw.getPartitions().size(); j++) {
            BasePartition<P, Q> p = pw.getPartitions().get(j);
            if (p.getStartFrame() >= yframe && p.getStartFrame() <= xframe) {
                // compute scoreMap
                Map<Set<K>, PrefixList<K>> map = pw.getPrefixMaps().get(j);
                for (Map.Entry<Set<K>, PrefixList<K>> entry: map.entrySet()) {
                    if (!retrievedPrefixes.contains(entry.getKey())) continue;
                    for (Map.Entry<Set<K>, Integer> e : entry.getValue().getObjValue().entrySet()) {
                        Set<K> objSet = new HashSet<>();
                        objSet.addAll(entry.getKey());
                        objSet.addAll(e.getKey());
                        int v = baseCountMap.getOrDefault(objSet, 0);
                        v += e.getValue();
                        baseCountMap.put(objSet, v);
                        if (v > maxCountPartition) maxCountPartition = v;
                    }
                }
            } else {
                // compute interval list.
                Map<Set<K>, PrefixList<K>> map = pw.getPrefixMaps().get(j);
                for (Map.Entry<Set<K>, PrefixList<K>> entry: map.entrySet()) {
                    if (!retrievedPrefixes.contains(entry.getKey())) continue;
                    Map<Set<K>, List<Interval>> im = ((PrefixListWithInterval<K>) entry.getValue()).getIntervalMap();
                    for (Map.Entry<Set<K>, List<Interval>> e : im.entrySet()) {
                        Set<K> objSet = new HashSet<>();
                        objSet.addAll(entry.getKey());
                        objSet.addAll(e.getKey());
                        List<Interval> ilist = intervalMap.computeIfAbsent(objSet, x -> new ArrayList<>());
                        // add all
                        if (e.getValue() == null) {
                            System.out.println("gotya");
                        }
                        ilist.addAll(e.getValue());
                    }
                }
            }
        }
    }

    public Map<Set<K>, List<Interval>> getIntervalMap() {
        return intervalMap;
    }

    public Map<Set<K>, Integer> getBaseCountMap() {
        return baseCountMap;
    }

}
