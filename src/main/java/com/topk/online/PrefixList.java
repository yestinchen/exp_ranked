package com.topk.online;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PrefixList<T> {

    int value = -1;

    Map<Set<T>, Integer> objValue = new HashMap<>();

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public Map<Set<T>, Integer> getObjValue() {
        return objValue;
    }

    public void setObjValue(Map<Set<T>, Integer> objValue) {
        this.objValue = objValue;
    }

    @Override
    public String toString() {
        return "PrefixList{" +
                "value=" + value +
                ", objValue=" + objValue +
                '}';
    }
}
