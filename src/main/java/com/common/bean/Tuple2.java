package com.common.bean;

public class Tuple2<T,F> {
    T _1;
    F _2;

    public Tuple2() {
    }

    public Tuple2(T _1, F _2) {
        this._1 = _1;
        this._2 = _2;
    }

    public T get_1() {
        return _1;
    }

    public void set_1(T _1) {
        this._1 = _1;
    }

    public F get_2() {
        return _2;
    }

    public void set_2(F _2) {
        this._2 = _2;
    }
}
