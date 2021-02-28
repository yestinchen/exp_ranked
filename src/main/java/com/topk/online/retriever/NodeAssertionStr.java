package com.topk.online.retriever;

import com.topk.offline.bean.Node;

import java.util.Set;

public class NodeAssertionStr implements NodeAssertion<String> {

    Set<String> selectedObjs;

    public NodeAssertionStr(Set<String> selectedObjs) {
        this.selectedObjs = selectedObjs;
    }

    @Override
    public boolean isSelected(Node<String, ?> node) {
        return selectedObjs.contains(node.getKey());
    }
}
