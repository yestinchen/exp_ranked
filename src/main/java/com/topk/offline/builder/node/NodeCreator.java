package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;

import java.util.List;
import java.util.Set;

public interface NodeCreator<T, F> {

    Node<T, F> createNewNode(String key, int count,
                             List<Integer> frames, Set<String> remainingObjs,
                             List<Set<String>> frameList,
                             int startFrameId,
                             Node<T, F> prevNode, int indexInCurrentLevel,
                             CLabel label);

}
