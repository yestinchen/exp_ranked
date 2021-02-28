package com.topk.online.component;

import com.topk.bean.Interval;
import com.topk.online.result.TopkBookKeeper;

import java.util.*;

public class WindowComputer {

    public static int computeStartWindow(int pwStart, int partitionSize, int w_m, int w) {
        if (pwStart != 0) {
            return w_m * partitionSize - w + 1;
        }
        return 0;
    }

    public static <T> int[] computeWindowScore(Map<Set<T>, List<Interval>> intervalMap,
                                           Map<Set<T>, Integer> baseCountMap,
                                           int pwStart, int pwEnd,
                                           int w, int startWindow,
                                           TopkBookKeeper topkBookKeeper) {
        // generate max count map.
        Map<Set<T>, Integer> maxScoreMap = new HashMap<>();
        for (Map.Entry<Set<T>,Integer> baseCountEntry: baseCountMap.entrySet()) {
            maxScoreMap.put(baseCountEntry.getKey(), baseCountEntry.getValue());
        }
        for (Map.Entry<Set<T>, List<Interval>> intervalEntry: intervalMap.entrySet()) {
            int maxCount = maxScoreMap.getOrDefault(intervalEntry.getKey(), 0);
            for (Interval inter: intervalEntry.getValue()) {
                maxCount += inter.getCount();
            }
            maxScoreMap.put(intervalEntry.getKey(), maxCount);
        }

        // sort max count map.
        List<Map.Entry<Set<T>, Integer>> maxScoreEntries = new ArrayList<>(maxScoreMap.entrySet());
        Collections.sort(maxScoreEntries, (x1, x2)-> - x1.getValue().compareTo(x2.getValue()));


        // windowMap.
        // how many windows? (end - start + 1) -w + 1
        // check if is the first one.
        int[] windowScoreArr;
        int baseIdx = pwStart + startWindow;
        int size = pwEnd - pwStart -w + 2 - startWindow;
        if (size > 0) {
            windowScoreArr = new int[size];
        } else {
            windowScoreArr = new int[1];
        }
        int windowMinScore =0;

        for (Map.Entry<Set<T>, Integer> maxScoreEntry : maxScoreEntries) {
            if (maxScoreEntry.getValue() < topkBookKeeper.getMin() || maxScoreEntry.getValue() < windowMinScore) break;
            Set<T> key = maxScoreEntry.getKey();

            // process key.
            List<Interval> intervals = intervalMap.getOrDefault(key, Collections.emptyList());
            int baseScore = baseCountMap.getOrDefault(key, 0);

            if (!intervals.isEmpty()) {
                // N^2, could be improved.
                int currentMin = Integer.MAX_VALUE;
                for (int j =0; j < windowScoreArr.length; j++) {
                    int start = j + baseIdx;
                    int end = start + w -1;
                    int score =baseScore;
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
                        windowScoreArr[j] = score;
                    }
                    if (windowScoreArr[j] < currentMin) {
                        currentMin = windowScoreArr[j];
                    }
//                        if (score == 7) {
//                            System.out.println("it");
//                        }
                }
                windowMinScore = currentMin;
            } else if (baseScore > 0) {
                // update all windows.
//                    if (baseScore == 7) {
//                        System.out.println("it");
//                    }
                for (int j = 0; j < windowScoreArr.length; j++) {
                    if (windowScoreArr[j] < baseScore) {
                        windowScoreArr[j] = baseScore;
                    }
                    if (windowMinScore < baseScore) {
                        windowMinScore = baseScore;
                    }
                }
            }

        }
        return windowScoreArr;
    }
}
