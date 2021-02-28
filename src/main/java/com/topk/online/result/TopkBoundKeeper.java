package com.topk.online.result;

import java.util.HashSet;
import java.util.Set;

public class TopkBoundKeeper {
    Set<Integer> bound = new HashSet<>();
    int k;
    int min = Integer.MAX_VALUE;

    public TopkBoundKeeper(int k) {
        this.k = k;
    }

    public void update(int score) {
        if (bound.size() < k) {
            bound.add(score);
            min = Integer.min(min, score);
        } else if (!bound.contains(score)) {
            if (score > min) {
                bound.remove(min);
                min = score;
            }
        }
    }

    public int getMin() {
        if (bound.size() < k) return Integer.MIN_VALUE;
        return min;
    }
}
