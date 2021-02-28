package com.topk.offline.bean;

import java.io.Serializable;
import java.util.List;

public class PayloadFrames implements Serializable {

    private static final long serialVersionUID = 3933375479323433107L;

    List<Integer> frames;

    public List<Integer> getFrames() {
        return frames;
    }

    public void setFrames(List<Integer> frames) {
        this.frames = frames;
    }
}
