package com.topk.offline.io;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PMetaObj implements Serializable {

    private static final long serialVersionUID = -7340077418898564899L;

    int startFrame;
    int size;

    List<String> objs;

    Map<Integer, Integer> top1Map = null;

    long startPos;
    int blockSize;

    public int getStartFrame() {
        return startFrame;
    }

    public void setStartFrame(int startFrame) {
        this.startFrame = startFrame;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
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

    public void setTop1Map(Map<Integer, Integer> top1Map) {
        this.top1Map = top1Map;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public String toString() {
        return "PMetaObj{" +
                "startFrame=" + startFrame +
                ", size=" + size +
                ", objs=" + objs +
                ", top1Map=" + top1Map +
                ", startPos=" + startPos +
                ", blockSize=" + blockSize +
                '}';
    }
}
