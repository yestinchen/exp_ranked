package com.common.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VideoSequence {

    List<VideoFrame> frames = new ArrayList<>();

    Map<String, String> idToType = new HashMap<>();

    public void addFrame(VideoFrame frame) {
        frames.add(frame);
    }

    public void addIdMapping(String id, String type) {
        idToType.put(id, type);
    }

    public boolean hasIdType(String id) {
        return idToType.containsKey(id);
    }

    public String getType(String id) {
        return idToType.get(id);
    }

    public List<VideoFrame> getFrames() {
        return frames;
    }

    public void setFrames(List<VideoFrame> frames) {
        this.frames = frames;
    }

    public Map<String, String> getIdToType() {
        return idToType;
    }

    public void setIdToType(Map<String, String> idToType) {
        this.idToType = idToType;
    }
}
