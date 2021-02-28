package com.topk.offline.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @param <T> the node key
 * @param <F> the payload of this node.
 */
public class Node<T, F> implements Serializable {
    private static final long serialVersionUID = -8146453349519185531L;
    List<Node<T, F>> next;
    T key;
    F payload;
    int id;

    public List<Node<T, F>> getNext() {
        return next;
    }

    public void setNext(List<Node<T, F>> next) {
        this.next = next;
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public F getPayload() {
        return payload;
    }

    public void setPayload(F payload) {
        this.payload = payload;
    }

    public void addNext(Node node) {
        if (next == null) next = new ArrayList<>();
        next.add(node);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
