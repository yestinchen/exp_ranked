package com.topk.utils;

import com.interval.bean.SETuple;
import com.interval.bean.TupleType;
import com.topk.bean.Interval;
import com.topk.online.ps.PSTuple;

import java.util.*;
import java.util.Comparator;

public class IntervalUtils {

    public static List<Interval> toInterval(List<Integer> frames) {
        if (frames == null) return null;
        List<Interval> list = new ArrayList<>();
        Interval interval = null;
        int last = -2;
        for (int f : frames) {
            if (last + 1 < f) {
                if (interval != null) {
                    interval.setEnd(last);
                }
                interval = new Interval();
                list.add(interval);
                interval.setStart(f);
            }
            last = f;
        }
        interval.setEnd(last);
        return list;
    }

    public static int getSpan(List<Interval> intervals) {
        if (intervals.size() == 0) return -1;
        return intervals.get(intervals.size() -1).getEnd() - intervals.get(0).getStart() + 1;
    }

    public static int count(List<Interval> intervals) {
        int count =0;
        for (Interval i : intervals) {
            count += i.getCount();
        }
        return count;
    }

//    public static List<Interval> intersect(List<Interval> l1, List<Interval> l2) {
//        // 1. convert to tuples.
//        List<PSTuple> tuples = new ArrayList<>();
//        for (Interval inter: l1) {
//            tuples.add(new )
//        }
//    }

    public static List<Interval> uniqueIntervals(List<Interval> intervals) {
        // 1. convert to tuples.
        List<SETuple> tuples = new ArrayList<>();
        for (Interval i : intervals) {
            tuples.add(new SETuple(i.getStart(), TupleType.S));
            tuples.add(new SETuple(i.getEnd(), TupleType.E));
        }
        Collections.sort(tuples, (x1, x2)-> {
            int cpv = Integer.compare(x1.value, x2.value);
            if (cpv == 0) {
                if (x1.type == x2.type) {
                    cpv = 0;
                } else {
                    if (x1.type == TupleType.S) {
                        cpv = -1;
                    } else {
                        cpv = 1;
                    }
                }
            }
            return cpv;
        });

        List<Interval> result = new ArrayList<>();
        int count =0;
        int lastStart = -1;
        for (SETuple tuple: tuples) {
            switch(tuple.type) {
                case S:
                    if (lastStart <0) {
                        lastStart = tuple.value;
                    }
                    count ++;
                    break;
                case E:
                    count --;
                    if (count == 0) {
                        // gen one.
                        result.add(new Interval(lastStart, tuple.value));
                        lastStart = -1;
                    }
            }
        }
        return result;
    }

    public static void main(String[] args) {
//        System.out.println(toInterval(Arrays.asList(1,2,3)));
//        System.out.println(getSpan(toInterval(Arrays.asList(1,2,3))));
        System.out.println(uniqueIntervals(Arrays.asList(
                new Interval(1, 5),
                new Interval(1, 8)
        )));
    }
}
