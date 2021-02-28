package com.topk.online.retriever;

public interface KeyMapper<T, K> {

    K map(T key);
}
