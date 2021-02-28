package com.topk.online.processors.indexed;

import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;

import java.util.List;

public class Candidate<T> {
    Node<String, T> node;
    List<String> prefix;
    boolean satisfied;
    boolean first = true;

    public Node<String, T> getNode() {
        return node;
    }

    public void setNode(Node<String, T> node) {
        this.node = node;
    }

    public List<String> getPrefix() {
        return prefix;
    }

    public void setPrefix(List<String> prefix) {
        this.prefix = prefix;
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