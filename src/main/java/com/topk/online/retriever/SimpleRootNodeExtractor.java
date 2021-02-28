package com.topk.online.retriever;

import com.topk.offline.bean.Node;

public class SimpleRootNodeExtractor<T, F> implements RootNodeExtractor<T, F, Node<T, F>>{
    @Override
    public Node<T, F> extract(Node<T, F> root) {
        return root;
    }
}
