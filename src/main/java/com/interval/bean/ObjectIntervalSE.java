package com.interval.bean;

import java.util.List;

public class ObjectIntervalSE {
    String obj;
    List<SETuple> tuples;

    public ObjectIntervalSE() {
    }

    public ObjectIntervalSE(String obj, List<SETuple> tuples) {
        this.obj = obj;
        this.tuples = tuples;
    }

    public String getObj() {
        return obj;
    }

    public void setObj(String obj) {
        this.obj = obj;
    }

    public List<SETuple> getTuples() {
        return tuples;
    }

    public void setTuples(List<SETuple> tuples) {
        this.tuples = tuples;
    }

    @Override
    public String toString() {
        return "ObjectIntervalSE{" +
                "obj=" + obj +
                ", tuples=" + tuples +
                '}';
    }
}
