package com.interval.bean;

import java.util.List;
import java.util.Set;

public class ObjSetInterval {
    Set<String> objs;
    List<Interval> intervals;

    public ObjSetInterval() {
    }

    public ObjSetInterval(Set<String> objs, List<Interval> intervals) {
        this.objs = objs;
        this.intervals = intervals;
    }

    public Set<String> getObjs() {
        return objs;
    }

    public void setObjs(Set<String> objs) {
        this.objs = objs;
    }

    public List<Interval> getIntervals() {
        return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
        this.intervals = intervals;
    }

    @Override
    public String toString() {
        return "ObjSetInterval{" +
                "objs=" + objs +
                ", intervals=" + intervals +
                '}';
    }
}
