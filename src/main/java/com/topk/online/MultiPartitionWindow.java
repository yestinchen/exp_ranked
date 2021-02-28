package com.topk.online;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.PayloadCount;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.processors.ConditionItem;

import java.util.List;
import java.util.Map;

public class MultiPartitionWindow<T, F extends PayloadCount, P, K, Q>{

    Map<CLabel, PartitionWindow<T, F, P, K, Q>> pwMap;
    int score = -1;

    public Map<CLabel, PartitionWindow<T, F, P, K, Q>> getPwMap() {
        return pwMap;
    }

    public MultiPartitionWindow(Map<CLabel, PartitionWindow<T, F, P, K, Q>> pwMap) {
        this.pwMap = pwMap;
    }

    public int getScore() {
        return score;
    }

    public void computeScore(List<List<ConditionItem>> conditions) {
        if (score > 0) return;
        int minScore = Integer.MAX_VALUE;
        for (List<ConditionItem> cis: conditions) {
            // and. use min.
            int maxScore = 0;
            for (ConditionItem ci : cis) {
                if (pwMap.get(ci.getType()) != null) {
                    maxScore = Integer.max(pwMap.get(ci.getType()).getScore(), maxScore);
                }
            }
            minScore = Integer.min(minScore, maxScore);
        }
        score = minScore;

    }
}
