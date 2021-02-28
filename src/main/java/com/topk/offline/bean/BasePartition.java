package com.topk.offline.bean;

import java.io.Serializable;
import java.util.*;

public class BasePartition<T, F> implements Serializable {

    private static final long serialVersionUID = -7250445070642786038L;

    int startFrame;
    int size;

    List<T> roots;

    List<String> objs;

    Map<CLabel, BitSet> typeBitSetMap;

    Map<Integer, Integer> top1Map = new HashMap<>();

    F payload;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public List<T> getRoots() {
        return roots;
    }

    public void setRoots(List<T> roots) {
        this.roots = roots;
    }

    public List<String> getObjs() {
        return objs;
    }

    public void setObjs(List<String> objs) {
        this.objs = objs;
    }

    public Map<Integer, Integer> getTop1Map() {
        return top1Map;
    }

    public int getStartFrame() {
        return startFrame;
    }

    public void setStartFrame(int startFrame) {
        this.startFrame = startFrame;
    }

    public void setTop1Map(Map<Integer, Integer> top1Map) {
        this.top1Map = top1Map;
    }

    public F getPayload() {
        return payload;
    }

    public void setPayload(F payload) {
        this.payload = payload;
    }

    public Map<CLabel, BitSet> getTypeBitSetMap() {
        return typeBitSetMap;
    }

    public void setTypeBitSetMap(Map<CLabel, BitSet> typeBitSetMap) {
        this.typeBitSetMap = typeBitSetMap;
    }
}
