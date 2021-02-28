package com.topk.online.processors.indexed.utils;

import com.topk.bean.Interval;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.bean.PayloadCount;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.PartitionWindow;
import com.topk.online.processors.indexed.IndexedWorkingPartition2;
import com.topk.online.processors.indexed.IntervalListWCount;
import com.topk.online.processors.indexed.composite.CompositeWorkingPartition;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;

public class IndexedUtils {

    static Logger LOG = LogManager.getLogger(IndexedUtils.class);

    static {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    public static <T extends PayloadClassIntervals> int computePossibleMaxComposite(
            List<CompositeWorkingPartition<T>> wps, int maxPartitionNum) {
        // in order.
        Map<Integer, CompositeWorkingPartition<T>> pwMap = new HashMap<>();
        for (CompositeWorkingPartition<T> wp: wps) {
            pwMap.put(wp.getBasePartition().getStartFrame(), wp);
        }
        List<Map.Entry<Integer, CompositeWorkingPartition<T>>> sorted = new ArrayList<>(pwMap.entrySet());
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
//        LOG.debug("remaining possible max arr: {}", values);
        return Collections.max(values);
    }
    public static <T extends PayloadIntervals> int computePossibleMax(
            List<IndexedWorkingPartition2<T>> wps, int maxPartitionNum) {
        // in order.
        Map<Integer, IndexedWorkingPartition2<T>> pwMap = new HashMap<>();
        for (IndexedWorkingPartition2<T> wp: wps) {
            pwMap.put(wp.getBasePartition().getStartFrame(), wp);
        }
        List<Map.Entry<Integer, IndexedWorkingPartition2<T>>> sorted = new ArrayList<>(pwMap.entrySet());
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

    public static <A, B extends PayloadCount, C, D, E> int[] initWindowScoreArr(
            PartitionWindow<A, B, C, D, E> pw, int w, int startWindow ) {
        int size = pw.getEnd() - pw.getStart() - w + 2 - startWindow;
        int[] windowScoreArr;
        if (size > 0) {
            windowScoreArr = new int[size];
        } else {
            windowScoreArr = new int[1];
        }
        return windowScoreArr;
    }

    public static void updateTopk(TopkBookKeeperBreakTie topkBookKeeper,
                                  int[] windowScoreArr, int baseIdx, int w) {
        for (int i =0; i < windowScoreArr.length; i++) {
            if (windowScoreArr[i] >= topkBookKeeper.getMin()) {
                WindowWithScore wws = new WindowWithScore();
                wws.setWindow(new Interval(baseIdx + i, baseIdx + i + w - 1));
                wws.setScore(windowScoreArr[i]);
                topkBookKeeper.update(wws);
            }
        }
    }

    public static int getKthFromArray(int[] windowArr, int k) {
        int[] copiedArr = new int[windowArr.length];
        System.arraycopy(windowArr, 0, copiedArr, 0, windowArr.length);
        Arrays.sort(copiedArr);
        int pos = k < windowArr.length ? k : windowArr.length -1;
        return copiedArr[pos];
    }

    public static boolean updateWindowAccordingToIntervalList(
            Map<Set<String>, List<Interval>> intervalMap, int windowMinScore,
            int[] windowScoreArr, int baseIdx, int w) {
        boolean processed = false;
        if (LOG.isDebugEnabled()) {
            for (Set<String> key : intervalMap.keySet()) {
                LOG.debug("interval map item, {} : {}", key, intervalMap.get(key));
            }
        }

        // sort & compute window score.
        List<IntervalListWCount> allList = new ArrayList<>();
        for (Map.Entry<Set<String>, List<Interval>> entry : intervalMap.entrySet()) {
            allList.add(new IntervalListWCount(entry.getKey(), entry.getValue()));
        }

        // desc order.
        Collections.sort(allList, (x1, x2) -> Integer.compare(x2.getCount(), x1.getCount()));

        // go go go.
        for (IntervalListWCount entry: allList) {
            if (entry.getCount() < windowMinScore) break;
            Collections.sort(entry.getIntervals(), Comparator.comparingInt(Interval::getStart));
            // fill in window arr.

            // process key.
            List<Interval> intervals = entry.getIntervals();

            if (!intervals.isEmpty()) {
                // N^2, could be improved.
                int currentMin = Integer.MAX_VALUE;
                for (int j =0; j < windowScoreArr.length; j++) {
                    int start = j + baseIdx;
                    int end = start + w -1;
                    int score = 0;
                    for (Interval inter: intervals) {
                        if (inter.getEnd() < start) continue;
                        if (inter.getStart() > end) break; // we assume ordered. list
                        int maxStart = Math.max(start, inter.getStart());
                        int minEnd = Math.min(end, inter.getEnd());
                        if (minEnd >= maxStart) {
                            score += minEnd - maxStart + 1;
                        }
                    }
                    if (score > windowScoreArr[j]) {
                        if (!processed) {
                            processed = true;
                        }
//                        if (score == 144) {
//                            System.out.println("what");
//                        }
                        windowScoreArr[j] = score;
                    }
                    if (windowScoreArr[j] < currentMin) {
                        currentMin = windowScoreArr[j];
                    }
                }
//                        windowMinScore = currentMin;
            }
        }
        return processed;
    }


    public static <T extends PayloadIntervals> void logWPsInfo(
            List<IndexedWorkingPartition2<T>> wps, int[] windowScoreArr,
            int remainingPossibleMax, int windowMinScore ) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("remaining possible max: {}, current min: {}", remainingPossibleMax, windowMinScore);
            LOG.debug(" current score window: {}", Arrays.toString(windowScoreArr));
            int totalCount = 0;
            for (IndexedWorkingPartition2<T> wp : wps) {
                totalCount += wp.getProcessedCount();
                LOG.debug("p: {}, computed node count: {}, remaining max: {}",
                        wp.getBasePartition().getStartFrame(), wp.getProcessedCount(),
                        wp.getRemainingCount());
            }
            LOG.debug("total processed: {}, current min: {}", totalCount, windowMinScore);
        }
    }


    public static <T extends PayloadClassIntervals> void logCompositeWPsInfo(
            List<CompositeWorkingPartition<T>> wps, int[] windowScoreArr,
            int remainingPossibleMax, int windowMinScore ) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("remaining possible max: {}, current min: {}", remainingPossibleMax, windowMinScore);
            LOG.debug(" current score window: {}", Arrays.toString(windowScoreArr));
            int totalCount = 0;
            for (CompositeWorkingPartition<T> wp : wps) {
                totalCount += wp.getProcessedCount();
                LOG.debug("p: {}, computed node count: {}, remaining max: {}",
                        wp.getBasePartition().getStartFrame(), wp.getProcessedCount(),
                        wp.getRemainingCount());
            }
            LOG.debug("total processed: {}, current min: {}", totalCount, windowMinScore);
        }
    }
}
