package com.topk.utils;

import java.util.*;

public class BitSetUtils {

    public static BitSet mapTo(BitSet givenBitSet, int[] map) {
        BitSet bitSet = new BitSet();
        for (int i=0; i < map.length; i++) {
            if (map[i] >= 0) {
                bitSet.set(map[i], givenBitSet.get(i));
            }
        }
        return bitSet;
    }

    public static List<String> select(BitSet bitSet, List<String> list) {
        List<String> result = new ArrayList<>();
        int start = 0;
        for (int i =0; i < bitSet.cardinality(); i++) {
            start = bitSet.nextSetBit(start);
            result.add(list.get(start));
            start ++;
        }
        return result;
    }

    public static void main(String[] args) {
//        BitSet givenOne = new BitSet(3);
//        for (int i=0; i < 3; i++) {
//            givenOne.set(i, true);
//        }
//        System.out.println(givenOne);
//        System.out.println(mapTo(givenOne, new int[]{1, -1, 5}, 6));

        BitSet givenOne = new BitSet();
        givenOne.set(0, true);
        givenOne.set(3, true);
        System.out.println(select(givenOne, Arrays.asList("a","b","c","d","e")));
    }
}
