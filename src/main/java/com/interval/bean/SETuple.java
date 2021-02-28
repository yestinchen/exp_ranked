package com.interval.bean;

import java.util.Objects;

public class SETuple implements Comparable<SETuple>{
    public int value;
    public TupleType type;

    public SETuple() {
    }

    public SETuple(int value, TupleType type) {
        this.value = value;
        this.type = type;
    }

    @Override
    public String toString() {
        return "SETuple{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SETuple seTuple = (SETuple) o;
        return value == seTuple.value &&
                type == seTuple.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }

    @Override
    public int compareTo(SETuple o) {
        int v1= Integer.compare(value, o.value);
        if (v1 != 0) return v1;
        v1 = type.compareTo(o.type);
        return v1;
    }

}
