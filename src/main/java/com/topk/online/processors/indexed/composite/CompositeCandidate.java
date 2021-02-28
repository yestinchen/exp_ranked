package com.topk.online.processors.indexed.composite;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;

import java.util.List;
import java.util.Map;

public class CompositeCandidate<T> {
    Node<String, T> node;
    Map<CLabel, List<String>> prefixMap;
    boolean satisfied;
    boolean first = true;

    public Node<String, T> getNode() {
        return node;
    }

    public void setNode(Node<String, T> node) {
        this.node = node;
    }

    public Map<CLabel, List<String>> getPrefixMap() {
        return prefixMap;
    }

    public void setPrefixMap(Map<CLabel, List<String>> prefixMap) {
        this.prefixMap = prefixMap;
    }

    public boolean isSatisfied() {
        return satisfied;
    }

    public void setSatisfied(boolean satisfied) {
        this.satisfied = satisfied;
    }

    public boolean isFirst() {
        return first;
    }

    public void setFirst(boolean first) {
        this.first = first;
    }
}
