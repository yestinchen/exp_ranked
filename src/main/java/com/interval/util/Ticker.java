package com.interval.util;

public class Ticker {

    long start;
    long end;

    public void start() {
        start = System.currentTimeMillis();
    }

    public void end() {
        end = System.currentTimeMillis();
    }

    public long report() {
        return end - start;
    }
}
