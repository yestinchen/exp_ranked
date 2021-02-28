package com.interval.bean;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectInterval {
    String obj;
    List<Interval> intervals;

    public ObjectInterval() {
    }

    public ObjectInterval(String obj, List<Interval> intervals) {
        this.obj = obj;
        this.intervals = intervals;
    }

    public String getObj() {
        return obj;
    }

    public void setObj(String obj) {
        this.obj = obj;
    }

    public List<Interval> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
        this.intervals = intervals;
    }

    public ObjectIntervalSE toObjectIntervalSE() {
        List<SETuple> tuples = intervals.stream().flatMap( i ->
                Arrays.asList(new SETuple(i.start, TupleType.S),
                        new SETuple(i.end, TupleType.E)).stream()).collect(Collectors.toList());
        return new ObjectIntervalSE(obj, tuples);
    }

    @Override
    public String toString() {
        return obj + ':' + intervals ;
    }
}
