package com.topk.online.result;

import com.topk.bean.Interval;

import java.util.Set;

public class WindowWithScore {
    Interval window;
    Integer score;
    Set<String> objects;

    public Interval getWindow() {
        return window;
    }

    public void setWindow(Interval window) {
        this.window = window;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public Set<String> getObjects() {
        return objects;
    }

    public void setObjects(Set<String> objects) {
        this.objects = objects;
    }

    @Override
    public String toString() {
        return window +
                ": " + score +"("+ objects +")" ;
    }
}
