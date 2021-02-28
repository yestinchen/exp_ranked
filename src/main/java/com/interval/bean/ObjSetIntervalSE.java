package com.interval.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ObjSetIntervalSE {
    static int idgen = 0;
    int id = idgen++;
    Set<String> objs;
    List<SETuple> tuples;
    public ObjSetIntervalSE() {
        tuples = new ArrayList<>();
    }

    public ObjSetIntervalSE(Set<String> objs, List<SETuple> tuples) {
        this.objs = objs;
        this.tuples = tuples;
    }

    public Set<String> getObjs() {
        return objs;
    }

    public void setObjs(Set<String> objs) {
        this.objs = objs;
    }

    public List<SETuple> getTuples() {
        return tuples;
    }

    public void setTuples(List<SETuple> tuples) {
        this.tuples = tuples;
    }

    public ObjSetInterval toObjSetInterval() {
        List<Interval> intervals = new ArrayList<>();
//        System.out.println("tuples:" + tuples);
        for (int i =0; i < tuples.size(); i+=2) {
            intervals.add(new Interval(tuples.get(i).value, tuples.get(i+1).value));
        }
        return new ObjSetInterval(objs, intervals);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjSetIntervalSE that = (ObjSetIntervalSE) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "ObjSetIntervalSE{" +
                "objs=" + objs +
                ", tuples=" + tuples +
                '}';
    }
}
