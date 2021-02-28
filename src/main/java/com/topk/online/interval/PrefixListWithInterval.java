package com.topk.online.interval;

import com.topk.bean.Interval;
import com.topk.online.PrefixList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrefixListWithInterval<T> extends PrefixList<T> {
    Map<Set<T>, List<Interval>> intervalMap = new HashMap<>();

    public Map<Set<T>, List<Interval>> getIntervalMap() {
        return intervalMap;
    }

    public void setIntervalMap(Map<Set<T>, List<Interval>> intervalMap) {
        this.intervalMap = intervalMap;
    }
}
