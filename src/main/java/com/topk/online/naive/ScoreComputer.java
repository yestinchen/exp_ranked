package com.topk.online.naive;

import com.interval.util.SetUtils;

import java.util.*;

/**
 * compute score for simple records.
 */
public class ScoreComputer {


    public int scoreFor(List<Set<String>> frames, int objNum) {
        Map<Set<String>, Set<Integer>> scoreMap = new HashMap<>();
        int maxScore = 0;
        int fid = 0;
        for (Set<String> frame: frames) {
            for (Set<String> key : new ArrayList<>(scoreMap.keySet())) {
                Set<Integer> frameSet = scoreMap.get(key);
                Set<String> result= SetUtils.intersect(frame, key);
                Set<Integer> newSet = scoreMap.computeIfAbsent(result, x -> new HashSet<>());
                newSet.addAll(frameSet);
                newSet.add(fid);
                if (result.size() >= objNum && newSet.size() > maxScore) {
                    maxScore = newSet.size();
                    System.out.println("score: "+ maxScore+", objs:" + result);
                }
            }
            Set<Integer> set = scoreMap.computeIfAbsent(frame, x -> new HashSet<>());
            set.add(fid);
            if (frame.size() >= objNum && set.size() > maxScore) {
                maxScore = set.size();
                System.out.println("score: "+ maxScore+", objs:" + frame);
            }
            fid ++;
        }
        return maxScore;
    }

    public static void main(String[] args) {
        ScoreComputer sc = new ScoreComputer();
        int score = sc.scoreFor(Arrays.asList(
                new HashSet<>(Arrays.asList("A", "B", "C")),
                new HashSet<>(Arrays.asList("A", "B", "D")),
                new HashSet<>(Arrays.asList("A", "C", "D")),
                new HashSet<>(Arrays.asList("A", "B", "C")),
                new HashSet<>(Arrays.asList("A", "B", "D"))
        ), 2);
        System.out.println("score;" +score);
    }
}
