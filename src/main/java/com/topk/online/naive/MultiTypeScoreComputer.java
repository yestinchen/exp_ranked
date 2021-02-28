package com.topk.online.naive;

import com.interval.util.SetUtils;
import com.topk.offline.bean.CLabel;
import com.topk.online.processors.ConditionItem;

import java.util.*;

public class MultiTypeScoreComputer {

    public Map<Integer, Integer> scoreFor(List<Set<String>> frames,
                         Map<String, CLabel> typeMap,
                         List<List<ConditionItem>> conditions,
                         int w) {
        Map<Integer, Integer> windowScoreMap = new HashMap<>();
        Map<Set<String>, Set<Integer>> scoreMap = new HashMap<>();
        int fid = 0;
        for (int i =0; i < frames.size(); i++) {
            int maxScore = 0;
            Set<String> frame = frames.get(i);
            if (i >= w - 1) {
                // clear.
                for (Set<String> key: new ArrayList<>(scoreMap.keySet())) {
                    // remove oldest time,
                    Set<Integer> currentSet = scoreMap.get(key);
                    currentSet.remove(fid - w);
                    if (currentSet.size() == 0) {
                        scoreMap.remove(key);
                    } else if (currentSet.size() > maxScore) {
                        maxScore = currentSet.size();
                    }
                }
            }

            for (Set<String> key: new ArrayList<>(scoreMap.keySet())) {
                Set<Integer> frameSet = scoreMap.get(key);
                Set<String> result= SetUtils.intersect(frame, key);
                // check condition.
                if (satisfyCondition(result, typeMap, conditions)) {
                    Set<Integer> newSet = scoreMap.computeIfAbsent(result, x -> new HashSet<>());
                    newSet.addAll(frameSet);
                    newSet.add(fid);
                    if (newSet.size() > maxScore) {
                        maxScore = newSet.size();
                    }
                }
            }
            if (satisfyCondition(frame, typeMap, conditions)) {
                Set<Integer> set = scoreMap.computeIfAbsent(frame, x -> new HashSet<>());
                set.add(fid);
                if (set.size() > maxScore && satisfyCondition(frame, typeMap, conditions)) {
                    maxScore = set.size();
                }
            }
            if (fid -w + 1 >= 0) {
                windowScoreMap.put(fid-w + 1, maxScore);
            }
            fid ++;
        }
        return windowScoreMap;
    }

    boolean satisfyCondition(Set<String> objs, Map<String, CLabel> typeMap,
                             List<List<ConditionItem>> conditions) {
        // agg.
        Map<CLabel, Integer> countMap = new HashMap<>();
        for (String obj: objs) {
            CLabel type = typeMap.get(obj);
            int v = countMap.getOrDefault(type, 0);
            countMap.put(type, v+1);
        }
        // eval
        for (List<ConditionItem> cil :conditions) {
            boolean satisfied = false;
            for (ConditionItem ci : cil) {
                if (countMap.getOrDefault(ci.getType(),0) >= ci.getObjNum()) {
                    satisfied = true; break;
                }
            }
            if (!satisfied) return false;
        }
        return true;
    }
}
