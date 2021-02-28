package com.topk.online.retriever;

public class KeyMapperDummy<T> implements KeyMapper<T, T>{
    @Override
    public T map(T key) {
        return key;
    }
}
