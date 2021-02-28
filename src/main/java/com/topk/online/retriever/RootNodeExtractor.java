package com.topk.online.retriever;

import com.topk.offline.bean.Node;

public interface RootNodeExtractor<T, F, P> {

    Node<T, F> extract(P root);
}
