package com.topk.offline.bean;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class PayloadWMaskMap extends PayloadIntervals implements PayloadWDewey  {

    Map<CLabel, BitSet> maskMap;

    CLabel label;

    List<Integer> deweyNumber;

    @Override
    public List<Integer> getDeweyNumber() {
        return deweyNumber;
    }

    public CLabel getLabel() {
        return label;
    }

    public void setLabel(CLabel label) {
        this.label = label;
    }

    @Override
    public void setDeweyNumber(List<Integer> list) {
        this.deweyNumber = list;
    }

    public Map<CLabel, BitSet> getMaskMap() {
        return maskMap;
    }

    public void setMaskMap(Map<CLabel, BitSet> maskMap) {
        this.maskMap = maskMap;
    }

    @Override
    public String toString() {
        return "PayloadWMaskMap{" +
                "maskMap=" + maskMap +
                ", label=" + label +
                ", deweyNumber=" + deweyNumber +
                ", intervals=" + intervals +
                ", count=" + count +
                '}';
    }
}
