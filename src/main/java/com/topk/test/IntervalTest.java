package com.topk.test;

import com.topk.bean.Interval;

import java.util.Arrays;
import java.util.List;

public class IntervalTest {

    public static int[] intervalToWindow(List<Interval> intervals, int w, int s) {
//        return intervalToWindowN2(intervals, w, s);
        return intervalToWindowTwoCursor(intervals, w, s);
    }

    public static int[] intervalToWindowN2(List<Interval> intervals, int w, int s) {
        int[] windows = new int[s];
        for (int i =0; i < s; i++) {
            int start = i;
            int end = i +w -1;
            int score =0;
            for (Interval inter: intervals) {
                if (inter.getEnd() < start) continue;
                if (inter.getStart() > end) break; // we assume ordered. list
                int maxStart = Math.max(start, inter.getStart());
                int minEnd = Math.min(end, inter.getEnd());
                if (minEnd >= maxStart) {
                    score += minEnd - maxStart + 1;
                }
            }
            windows[i] = score;
        }
        return windows;
    }

    public static int[] intervalToWindowTwoCursor(List<Interval> intervals, int w, int s) {
        int[] windows = new int[s];
        int sId = 0, eId = 0;
        int score =0;
        // 1st.
        while(eId< intervals.size() && intervals.get(eId).getEnd() <= w-1) {
            score += intervals.get(eId).getCount();
            eId++;
        }
        // deal with the last.
        if (eId < intervals.size() && intervals.get(eId).getStart() <= w-1) {
            score += w- intervals.get(eId).getStart();
        }
        windows[0] =  score;
        boolean sInclude = intervals.get(sId).getStart() == 0;
        for (int i =1; i < s; i++) {
            // slide.
            // if last start is included, dec score.
            if (sInclude) {
                score --;
                // reset to false.
                sInclude = false;
            }

            if (sId < intervals.size()) {
                Interval sInterval = intervals.get(sId);

                if (sInterval.getStart() <= i && sInterval.getEnd() >= i) {
                    // inc score.
                    sInclude = true;
                } else if (sInterval.getEnd() < i) {
                    // to next.
                    sId++;
                    if (sId < intervals.size()) {
                        if (intervals.get(sId).getStart() <= i) {
                            sInclude = true;
                        }
                    }
                }
            }

            if (eId < intervals.size()) {
                Interval eInterval = intervals.get(eId);
                int lastFrame = i + w -1;
                if (eInterval.getEnd() < lastFrame) {
                    eId ++;
                    if (eId < intervals.size()) {
                        if (intervals.get(eId).getStart() <= lastFrame && intervals.get(eId).getEnd() >= lastFrame) {
                            score ++;
                        }
                    }
                } else if (eInterval.getStart() <= lastFrame && eInterval.getEnd() > lastFrame) {
                    score ++;
                } else if (eInterval.getEnd() == lastFrame) {
                    // next.
                    score ++;
                    eId ++;
                }
            }
            windows[i] = score;
        }
        return windows;
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(
                intervalToWindow(Arrays.asList(
                        new Interval(1,2),
                        new Interval(3,4)
                ), 2, 5)
        ));
    }
}
