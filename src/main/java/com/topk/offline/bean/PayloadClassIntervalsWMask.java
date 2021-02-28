package com.topk.offline.bean;

import java.util.BitSet;

public class PayloadClassIntervalsWMask extends PayloadClassIntervals implements PayloadWMask {
    BitSet mask;

    public BitSet getMask() {
        return mask;
    }

    public void setMask(BitSet mask) {
        this.mask = mask;
    }

    @Override
    public String toString() {
        return "PayloadClassIntervalsWMask{" +
                "mask=" + mask +
                ", label=" + label +
                ", intervals=" + intervals +
                ", count=" + count +
                '}';
    }
}
