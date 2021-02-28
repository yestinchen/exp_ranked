package com.topk.online.retriever;

import com.topk.offline.bean.Node;

public interface NodeAssertion<T> {

    boolean isSelected(Node<T, ?> node);
}
