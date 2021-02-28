package com.interval.struct;

import org.junit.Test;

import java.util.*;

public class SimpleTreeBuilderTest {

    @Test
    public void testBuildTree1() {
        List<Set<String>> dataList = new ArrayList<>(
            Arrays.asList(
                new HashSet<>(Arrays.asList("A", "B", "C")),
                    new HashSet<>(Arrays.asList("A", "C")),
                    new HashSet<>(Arrays.asList("C")),
                new HashSet<>(Arrays.asList("A", "B"))
            )
        );
        SimpleNode<Set<String>> root = SimpleTreeBuilder.buildTree(dataList, new HashMap<>());

        SimpleNoteIO.printTree(root);
    }

    @Test
    public void testBuildTree2() {
        List<Set<String>> dataList = new ArrayList<>(
                Arrays.asList(
                        new HashSet<>(Arrays.asList("A", "B", "D")),
                        new HashSet<>(Arrays.asList("A", "B", "C")),
                        new HashSet<>(Arrays.asList("A", "C")),
                        new HashSet<>(Arrays.asList("C")),
                        new HashSet<>(Arrays.asList("A", "B"))
                )
        );
        SimpleNode<Set<String>> root = SimpleTreeBuilder.buildTree(dataList, new HashMap<>());

        SimpleNoteIO.printTree(root);
    }
}
