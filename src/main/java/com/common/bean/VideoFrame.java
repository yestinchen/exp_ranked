package com.common.bean;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VideoFrame {
    Set<String> ids = new HashSet<>();

    public Set<String> getIds() {
        return ids;
    }

    public void setIds(Set<String> ids) {
        this.ids = ids;
    }

    public void add(String id) {
        ids.add(id);
    }
}
