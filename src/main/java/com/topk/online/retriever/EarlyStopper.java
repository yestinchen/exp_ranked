package com.topk.online.retriever;

import com.topk.offline.bean.Node;

public interface EarlyStopper<T, F> {

    boolean shouldStop(Node<T, F> node);
}
