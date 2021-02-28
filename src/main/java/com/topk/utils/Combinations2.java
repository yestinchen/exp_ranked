package com.topk.utils;

import java.util.*;

public class Combinations2 {

    /**
     * compute all combinations between items in the list (size of n).
     * Each result should contain n object sets from each position of the list.
     * @param list
     * @param minValue
     * @return
     */
    public static Map<Set<String>, Integer> combinations(List<Map<Set<String>, Integer>> list, int minValue) {
        Map<Set<String>, Integer> map = new HashMap<>();
        if (list.size() == 1) {
            for (Map.Entry<Set<String>, Integer> entry: list.get(0).entrySet()) {
                map.put(entry.getKey(), Math.max(minValue, entry.getValue()));
            }
            return map;
        } else {
            List<Set<String>> currentOne = new ArrayList<>(list.get(0).keySet());
            for (Set<String> s : currentOne) {
                int value = list.get(0).get(s);
                Map<Set<String>, Integer> result = combinations(list.subList(1, list.size()),
                        Math.max(minValue, value));
                for (Map.Entry<Set<String>, Integer> entry: result.entrySet()) {
                    Set<String> ns = new HashSet<>(entry.getKey());
                    ns.addAll(s);
                    map.put(ns, entry.getValue());
                }
            }
            return map;
        }
    }
}
