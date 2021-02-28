package com.topk.online.retriever;

import com.topk.offline.bean.Node;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NodeAssertionInteger implements NodeAssertion<Integer>{

    Set<Integer> selectedObjs;

    public NodeAssertionInteger(List<String> orderedObjs, Set<String> selectedObjsStr) {
        selectedObjs = new HashSet<>();
        for (String obj: selectedObjsStr) {
            selectedObjs.add(orderedObjs.indexOf(obj));
        }
    }


    @Override
    public boolean isSelected(Node<Integer, ?> node) {
        return selectedObjs.contains(node.getKey());
    }
}
