package com.interval.struct;

public class SimpleNoteIO {

    public static <T> void printTree(SimpleNode<T> simpleNode) {
        int space =0;
        printNode(simpleNode, space);
    }

    static <T> void printNode(SimpleNode<T> simpleNode, int space) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < space; i++) {
            if (i == space - 1) {
                sb.append("+");
                sb.append("-");
            } else {
                sb.append(" ");
            }
        }
        sb.append(simpleNode.obj);
        System.out.println(sb);
        if (simpleNode.children != null) {
            for (SimpleNode<T> c : simpleNode.children) {
                printNode(c, space + 1);
            }
        }
    }
}
