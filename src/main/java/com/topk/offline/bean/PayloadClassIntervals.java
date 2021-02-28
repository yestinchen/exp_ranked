package com.topk.offline.bean;

public class PayloadClassIntervals extends PayloadIntervals {

    CLabel label;

    public CLabel getLabel() {
        return label;
    }

    public void setLabel(CLabel label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return "PayloadClassIntervals{" +
                "label=" + label +
                ", intervals=" + intervals +
                '}';
    }
}
