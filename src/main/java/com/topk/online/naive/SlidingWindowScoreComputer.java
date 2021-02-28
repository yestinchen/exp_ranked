package com.topk.online.naive;


import com.interval.util.SetUtils;

import java.util.*;

public class SlidingWindowScoreComputer {
    public Map<Integer, Integer> scoreFor(List<Set<String>> frames, int objNum, int w) {

        Map<Set<String>, Set<Integer>> scoreMap = new HashMap<>();
        int fid = 0;
        Map<Integer, Integer> windowScores = new HashMap<>();
        for (int i =0; i < frames.size(); i++) {
//            if (i == 5) {
//                System.out.println("break");
//            }
            int maxScore = 0;
            Set<String> maxKey = null;

            Set<String> frame = frames.get(i);
            if (i >= w-1) {
                // clear
                for (Set<String> key: new ArrayList<>(scoreMap.keySet())) {
                    // remove oldest time,
                    Set<Integer> currentSet = scoreMap.get(key);
                    currentSet.remove(fid - w);
                    if (currentSet.size() == 0) {
                        scoreMap.remove(key);
                    } else if (currentSet.size() > maxScore) {
                        maxScore = currentSet.size();
                        maxKey = key;
                    }
                }
            }
            for (Set<String> key : new ArrayList<>(scoreMap.keySet())) {
                Set<Integer> frameSet = scoreMap.get(key);
                Set<String> result= SetUtils.intersect(frame, key);
                if (result.size() >= objNum) {
                    Set<Integer> newSet = scoreMap.computeIfAbsent(result, x -> new HashSet<>());
                    newSet.addAll(frameSet);
                    newSet.add(fid);
                    if (result.size() >= objNum && newSet.size() > maxScore) {
                        maxScore = newSet.size();
                        maxKey = key;
                    }
                }
            }

            if (!frame.isEmpty() && frame.size() >= objNum) {
                Set<Integer> set = scoreMap.computeIfAbsent(frame, x -> new HashSet<>());
                set.add(fid);
                if (frame.size() >= objNum && set.size() > maxScore) {
                    maxScore = set.size();
                    maxKey = frame;
                }
            }

//            if (fid - w + 1 == 3358) {
//                System.out.println("score:"+maxScore);
//                System.out.println("key:" + maxKey);
//            }


            if (fid - w + 1 >= 0) {
                windowScores.put(fid - w + 1, maxScore);
            }
            fid ++;
        }
        return windowScores;
    }
}
