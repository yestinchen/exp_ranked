package com.topk.test;

import java.util.BitSet;

public class BitSetTest {

    public static void main(String[] args) {

        BitSet bitSet = new BitSet(4);
        bitSet.set(1, true);

        System.out.println(bitSet.nextClearBit(0));
        System.out.println(bitSet.nextSetBit(0));

        System.out.println(bitSet.length());

        bitSet.set(1, false);
        System.out.println(bitSet.nextSetBit(0));
        System.out.println(bitSet.nextClearBit(0));
    }
}
