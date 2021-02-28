package com.topk.offline.bean;

import java.util.BitSet;

public class PayloadIntervalsWMask extends PayloadIntervals implements PayloadWMask {
    BitSet mask;

    public BitSet getMask() {
        return mask;
    }

    public void setMask(BitSet mask) {
        this.mask = mask;
    }

    @Override
    public String toString() {
        return "PayloadIntervalsWMask{" +
                "mask=" + mask +
                ", intervals=" + intervals +
                ", count=" + count +
                '}';
    }
}
