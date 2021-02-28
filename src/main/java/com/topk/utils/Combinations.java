package com.topk.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Combinations {

    public static <T> List<List<T>>  combinations(List<T> givenIds, int k) {
        int n = givenIds.size();
        List<List<T>> combos = new ArrayList<>();
        if (k == 0) {
            combos.add(new ArrayList<>());
            return combos;
        }
        if (n < k || n == 0)
            return combos;
        T last = givenIds.get(n-1);
        combos.addAll(combinations(givenIds.subList(0, n-1), k));
        for (List<T> subCombo : combinations(givenIds.subList(0, n-1), k-1)) {
            List<T> newOne = new ArrayList<>();
            newOne.addAll(subCombo);
            newOne.add(last);
            combos.add(newOne);
        }

        return combos;
    }

    public static void main(String[] args) {
        for (List<String> comb: combinations(Arrays.asList("A", "B", "C"), 2)) {
            System.out.println(comb);
        }
    }
}
