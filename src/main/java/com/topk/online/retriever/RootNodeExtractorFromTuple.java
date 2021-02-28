package com.topk.online.retriever;

import com.common.bean.Tuple2;
import com.topk.offline.bean.Node;

public class RootNodeExtractorFromTuple<T, F, K> implements RootNodeExtractor<T, F, Tuple2<Node<T, F>, K>>{
    @Override
    public Node<T, F> extract(Tuple2<Node<T, F>, K> root) {
        return root.get_1();
    }
}
