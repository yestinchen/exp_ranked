package com.topk.offline.bean;

import java.util.List;

public class PayloadWMaskDewey extends PayloadIntervalsWMask implements PayloadWDewey {

    List<Integer> deweyNumber;

    @Override
    public List<Integer> getDeweyNumber() {
        return deweyNumber;
    }

    @Override
    public void setDeweyNumber(List<Integer> list) {
        this.deweyNumber = list;
    }

    @Override
    public String toString() {
        return "PayloadWMaskDewey{" +
                "deweyNumber=" + deweyNumber +
                ", mask=" + mask +
                ", intervals=" + intervals +
                ", count=" + count +
                '}';
    }
}
