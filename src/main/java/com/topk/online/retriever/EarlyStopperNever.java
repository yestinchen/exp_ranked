package com.topk.online.retriever;

import com.topk.offline.bean.Node;

public class EarlyStopperNever<T, F> implements EarlyStopper<T, F>{

    @Override
    public boolean shouldStop(Node<T, F> node) {
        return false;
    }
}
