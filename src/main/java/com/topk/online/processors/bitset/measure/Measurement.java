package com.topk.online.processors.bitset.measure;

public class Measurement {

    static int processedCount = 0;
    static int andCount =0;
    static int andCount2 = 0;

    public static void reset() {
        processedCount = 0;
        andCount = 0;
        andCount2 = 0;
    }

    public static void incProcessedCount() {
        processedCount += 1;
    }

    public static int getProcessedCount() {
        return processedCount;
    }

    public static void incAndCount() {
        andCount += 1;
    }

    public static int getAndCount() {
        return andCount;
    }

    public static void incAndCount2() {
        andCount2 += 1;
    }

    public static int getAndCount2() {
        return andCount2;
    }
}
