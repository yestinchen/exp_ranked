package com.topk.online.retriever;

import java.util.*;

public class KeyMapperInt implements KeyMapper<Integer, String> {

    List<String> orderedObjs;

    public KeyMapperInt(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public String map(Integer key) {
        return orderedObjs.get(key);
    }
}
