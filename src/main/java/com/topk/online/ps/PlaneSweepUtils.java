package com.topk.online.ps;

import com.interval.bean.SETuple;
import com.interval.bean.TupleType;
import com.topk.bean.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PlaneSweepUtils {

    public static List<Interval> planeSweep(List<List<Interval>> intervalLists) {
        // convert to ps tuple.
        List<PSTuple> tuples = new ArrayList<>();
        for (List<Interval> intervals : intervalLists) {
            for (Interval i : intervals) {
                tuples.add(new PSTuple(-1, i.getStart(), PSTuple.PSTType.Start, null));
                tuples.add(new PSTuple(-1, i.getEnd(), PSTuple.PSTType.End, null));
            }
        }
        Collections.sort(tuples,
                Comparator.comparingInt((PSTuple x) -> x.value)
                        .thenComparing(x -> x.type));
        int count =0;
        int lastV = -1;
        List<Interval> resultIntervals = new ArrayList<>();
        for (PSTuple t : tuples) {
            if (t.type == PSTuple.PSTType.Start) {
                count ++;
                lastV = t.value;
            } else {
                if (count == intervalLists.size()) {
                    Interval interval = new Interval(lastV, t.getValue());
                    resultIntervals.add(interval);
                }
                count --;
            }
        }
        return resultIntervals;
    }
}
