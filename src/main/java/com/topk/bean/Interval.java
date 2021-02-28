package com.topk.bean;

import java.io.Serializable;

public class Interval implements Serializable {

    private static final long serialVersionUID = -537252103115287483L;
    int start;
    int end;

    public Interval() {
    }

    public Interval(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public int getCount(){
        return end - start + 1;
    }

    @Override
    public String toString() {
        return "[" + start +
                "," + end +
                ']';
    }
}
