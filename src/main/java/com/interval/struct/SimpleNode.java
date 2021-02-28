package com.interval.struct;

import java.util.List;
import java.util.Objects;

public class SimpleNode<T> {

    static int idGen = 0;

    int nodeId = idGen++;
    T obj;

    List<SimpleNode<T>> children;
    List<SimpleNode<T>> parents;
    boolean visited;

    public SimpleNode() {
    }

    public SimpleNode(T obj, List<SimpleNode<T>> children) {
        this.obj = obj;
        this.children = children;
    }

    public T getObj() {
        return obj;
    }

    public void setObj(T obj) {
        this.obj = obj;
    }

    public List<SimpleNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<SimpleNode<T>> children) {
        this.children = children;
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public List<SimpleNode<T>> getParents() {
        return parents;
    }

    public void setParents(List<SimpleNode<T>> parents) {
        this.parents = parents;
    }

    public boolean isVisited() {
        return visited;
    }

    public void setVisited(boolean visited) {
        this.visited = visited;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleNode<?> that = (SimpleNode<?>) o;
        return nodeId == that.nodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "SimpleNode{" +
                "nodeId=" + nodeId +
                ", obj=" + obj +
                '}';
    }
}
