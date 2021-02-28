package com.topk.utils;

import java.util.Arrays;
import java.util.List;

public class Comparator {

    public static int compareDeweyNumbers(List<Integer> dw1, List<Integer> dw2) {
        int comp = Integer.compare(dw1.size(), dw2.size());
        if (comp != 0) return comp;
        // compare each number.
        for (int i=0; i < dw1.size(); i++) {
             comp = Integer.compare(dw1.get(i), dw2.get(i));
             if (comp != 0) return comp;
        }
        return comp;
    }

    /**
     * check if dw2 is compatible with dw1. i.e. dw2 shares the same prefix as dw1.
     * @param dw1
     * @param dw2
     * @return
     */
    public static boolean isCompatible(List<Integer> dw1, List<Integer> dw2) {
        if (dw1.size() > dw2.size()) return false;
        for (int i=0; i < dw1.size(); i++) {
            if (!dw1.get(i).equals(dw2.get(i))) return false;
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(compareDeweyNumbers(Arrays.asList(1), Arrays.asList(1, 1)));
        System.out.println(compareDeweyNumbers(Arrays.asList(1,2), Arrays.asList(1, 1)));
    }
}
