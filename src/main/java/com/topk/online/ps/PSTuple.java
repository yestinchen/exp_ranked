package com.topk.online.ps;

import java.util.Set;

public class PSTuple {
    int rId;
    int value;
    PSTType type;
    Set<String> objs;

    public PSTuple(int rId, int value, PSTType type, Set<String> objs) {
        this.rId = rId;
        this.value = value;
        this.type = type;
        this.objs = objs;
    }

    public Set<String> getObjs() {
        return objs;
    }

    public void setObjs(Set<String> objs) {
        this.objs = objs;
    }

    public int getrId() {
        return rId;
    }

    public void setrId(int rId) {
        this.rId = rId;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public PSTType getType() {
        return type;
    }

    public void setType(PSTType type) {
        this.type = type;
    }

    public static enum PSTType {
        Start, End;
    }
}
