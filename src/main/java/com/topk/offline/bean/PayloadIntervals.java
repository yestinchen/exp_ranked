package com.topk.offline.bean;

import com.topk.bean.Interval;

import java.util.List;

public class PayloadIntervals extends PayloadCount {

    private static final long serialVersionUID = 2143218884809726635L;

    List<Interval> intervals;

    public PayloadIntervals() {
    }


    public List<Interval> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
        this.intervals = intervals;
    }

    @Override
    public String toString() {
        return "PayloadIntervals{" +
                "intervals=" + intervals +
                ", count=" + count +
                '}';
    }
}
