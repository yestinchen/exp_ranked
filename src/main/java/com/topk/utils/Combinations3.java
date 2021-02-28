package com.topk.utils;

import java.util.*;

public class Combinations3 {

    /**
     * Take each element from each list to generate all possible combinations.
     * @param list
     * @return
     */
    public static <K> List<List<K>> combinations(List<List<List<K>>> list) {
        if (list.size() == 1) {
            return list.get(0);
        }
        List<List<K>> result = new ArrayList<>();
        // get one.
        List<List<K>> currentList = list.get(0);
        for (List<K> l : currentList) {
            List<List<K>> otherResult= combinations(list.subList(1, list.size()));
            for (List<K> or: otherResult) {
                List<K> r1 = new ArrayList<>();
                r1.addAll(or);
                r1.addAll(l);
                result.add(r1);
            }
        }
        return result;
    }

    public static void main(String[] args) {
        List<List<String>> result = combinations(Arrays.asList(
                Arrays.asList(
                        Arrays.asList("A")
                ),Arrays.asList(
                        Arrays.asList("B"),
                        Arrays.asList("D")
                ),Arrays.asList(
                        Arrays.asList("C")
                )
        ));
        for (List<String> r : result) {
            System.out.println(r);
        }
    }
}
