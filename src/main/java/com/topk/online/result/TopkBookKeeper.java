package com.topk.online.result;

import java.util.*;

public class TopkBookKeeper {
    Map<Integer, List<WindowWithScore>> bound = new HashMap<>();
    int k;
    int min = Integer.MAX_VALUE;

    public TopkBookKeeper(int k) {
        this.k = k;
    }

    public void update(WindowWithScore window) {
        if (bound.size() < k) {
            List<WindowWithScore> rlist = bound.computeIfAbsent(window.score, x-> new ArrayList<>());
            rlist.add(window);
            min = Integer.min(min, window.score);
        } else if (!bound.containsKey(window.score)) {
            if (window.score > min) {
                bound.remove(min);
                List<WindowWithScore> rlist = bound.computeIfAbsent(window.score, x-> new ArrayList<>());
                rlist.add(window);
                min = Collections.min(bound.keySet());
            }
        } else {
            // update rlist
            bound.get(window.score).add(window);
        }
    }

    public int getMin() {
        if (bound.size() < k) return Integer.MIN_VALUE;
        return min;
    }

    public Collection<List<WindowWithScore>> getTopkResults() {
        return bound.values();
    }
}
