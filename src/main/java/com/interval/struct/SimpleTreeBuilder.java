package com.interval.struct;

import java.util.*;

public class SimpleTreeBuilder {

    public static <T extends Set> SimpleNode<T> buildTree(List<T> dataList, Map<T, SimpleNode<T>> index) {
        // sort in size reverse
        Collections.sort(dataList, (x1, x2)-> -Integer.compare(x1.size(), x2.size()));

        SimpleNode<T> root = new SimpleNode<>(); // root is empty.

        int count = 0;
        for (T data : dataList) {
            count++;
            System.out.println("building:"+count);
            SimpleNode<T> newNode = new SimpleNode<>(data, null);
            index.put(data, newNode);
            List<SimpleNode<T>> candidates = new ArrayList<>();
            List<SimpleNode<T>> buffer = new ArrayList<>();
            buffer.add(root);
            while(!buffer.isEmpty()) {
                // get one and check its children.
                SimpleNode<T> candidate = buffer.remove(0);
                boolean progress= false;
                if (candidate.children != null && !candidate.children.isEmpty()) {
                    for (SimpleNode<T> node : candidate.children) {
                        if (node.obj.containsAll(data)) {
                            // can process.
                            progress = true;
                            buffer.add(node);
                        }
                    }
                }
                if (!progress) {
                    candidates.add(candidate);
                }
            }
            for (SimpleNode<T> c : candidates) {
                // candidate is the root of new data.
                if (c.children != null && !c.children.isEmpty()) {
                    for (int i = c.children.size() - 1; i >= 0; i--) {
                        SimpleNode<T> child = c.children.get(i);
                        if (data.containsAll(child.obj)) {
                            // remove it.
                            child.parents.remove(c);
                            c.children.remove(i);
                        }
                    }
                }
                if (c.children == null) {
                    c.children = new ArrayList<>();
                }
                c.children.add(newNode);
                if (newNode.parents == null) {
                    newNode.parents = new ArrayList<>();
                }
                newNode.parents.add(c);
            }


        }

        // pick.
        if (root.children.size() == 1) {
            root.children.get(0).parents = null;
            return root.children.get(0);
        }
        return root;
    }

//    public static <T extends Set> SimpleNode<T> insert(SimpleNode<T> tree, T obj) {
//        if (tree == null) {
//            SimpleNode<T> node = new SimpleNode<>(obj, new ArrayList<>());
//            return node;
//        }
//        // else insert the obj and return the new root.
//
////        if (tree.obj == null) {
////            boolean hasCandidate = false;
////            for (SimpleNode<T> node : tree.children) {
////                if (node.obj.containsAll(obj)) {
////
////                }
////            }
////        }
//    }

}
