package com.topk.online.processors.indexed;

import com.topk.bean.Interval;
import com.topk.utils.IntervalUtils;

import java.util.List;
import java.util.Set;

public class IntervalListWCount {
    Set<String> set;
    List<Interval> intervals;
    int count;

    public IntervalListWCount(Set<String> set, List<Interval> intervals) {
        this.set = set;
        this.intervals = intervals;
        this.count = IntervalUtils.count(intervals);
    }

    public List<Interval> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
        this.intervals = intervals;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Set<String> getSet() {
        return set;
    }

    public void setSet(Set<String> set) {
        this.set = set;
    }
}