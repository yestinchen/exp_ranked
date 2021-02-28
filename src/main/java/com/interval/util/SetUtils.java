package com.interval.util;

import java.util.HashSet;
import java.util.Set;

public class SetUtils {

    public static <T> Set<T> intersect(Set<T> set1, Set<T> set2) {
        Set<T> result = new HashSet<>();
        for (T t : set1) {
            if (set2.contains(t)) {
                result.add(t);
            }
        }
        return result;
    }
}
