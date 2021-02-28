package com.topk.online.processors.bitset;

import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.processors.indexed.IndexedWorkingPartition2;
import com.topk.online.processors.indexed.utils.BasePartitionHolder;

import java.util.*;

public class Utils {

    public static <T extends BasePartitionHolder> int computePossibleMax(
            List<T> wps, int maxPartitionNum) {
        // in order.
        Map<Integer, T> pwMap = new HashMap<>();
        for (T wp: wps) {
            pwMap.put(wp.getBasePartition().getStartFrame(), wp);
        }
        List<Map.Entry<Integer, T>> sorted = new ArrayList<>(pwMap.entrySet());
        Collections.sort(sorted, Comparator.comparingInt(Map.Entry::getKey));
        // map to remaining count.
        int[] countList = sorted.stream().mapToInt(i -> i.getValue().getRemainingCount()).toArray();
        List<Integer> values = new ArrayList<>();
//        LOG.debug("remaining possible , count list: {}", Arrays.toString(countList));
        int sum = 0;
        for (int i=0; i < sorted.size(); i++) {
            sum += countList[i];
            if (i + 1 >= maxPartitionNum) {
                values.add(sum);
                sum -= countList[i - maxPartitionNum + 1];
            }
        }
        if (sorted.size() < maxPartitionNum) {
            values.add(sum);
        }
//        LOG.debug("remaining possible max arr: {}", values);
        return Collections.max(values);
    }

}
