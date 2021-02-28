package com.topk.offline.builder;

import com.topk.offline.bean.*;
import com.topk.offline.builder.node.CTNodeWIntervalCreator;
import com.topk.offline.builder.node.NodeCreator;
import com.topk.offline.builder.partition.CommonPartitionCreator;
import com.topk.offline.builder.partition.PartitionCreator;

import java.util.*;

/**
 * Contain the basic logic for structuring the index.
 */
public class PartitionIndexBootstrap<T, F, P, Q> {

    int startFrameId = 0;

    NodeCreator<T, F> nodeCreator;

    PartitionCreator<T, F, P, Q> partitionCreator;

    public PartitionIndexBootstrap(int startFrameId,
                                   NodeCreator<T, F> nodeCreator,
                                   PartitionCreator<T, F, P, Q> partitionCreator) {
        this.startFrameId = startFrameId;
        this.nodeCreator = nodeCreator;
        this.partitionCreator = partitionCreator;
    }

    public BasePartition<P, Q> build(List<Set<String>> list) {
        return build(list, null);
    }

    public BasePartition<P, Q> build(List<Set<String>> list, Map<String, CLabel> typeMap) {
        Map<String, Integer> count = new HashMap<>();
        Map<String, List<Integer>> framesMap = new HashMap<>();
        int frameId = startFrameId;
        for (Set<String> set : list) {
            for (String id: set) {
                int v  =count.computeIfAbsent(id, x-> 0);
                count.put(id, v+1);
                List<Integer> frameList = framesMap.computeIfAbsent(id, x-> new ArrayList<>());
                frameList.add(frameId);
            }
            frameId ++;
        }
        int size = frameId - startFrameId;

        Map<Integer, Integer> top1Map = new HashMap<>();
        List<Node<T, F>> roots  = new ArrayList<>();
        List<Set<String>> connectedObjList = new ArrayList<>();
        // add the count map for the current level.
        List<Map<String, Integer>> countMaps = new ArrayList<>();
        countMaps.add(count);

        int indexInCurrentLevel = 0;
        // select one & build
        while(!count.isEmpty()) {
            indexInCurrentLevel ++;
            // select top one && build.
            List<Map.Entry<String, Integer>> allEntries = new ArrayList<>(count.entrySet());
            sortCountMap(allEntries);
            Map.Entry<String, Integer> entry = allEntries.get(0);
            // remove it from count.
            int currentCount = count.remove(entry.getKey());
            List<Integer> currentFrames = framesMap.remove(entry.getKey());
            Node<T, F> node = nodeCreator.createNewNode(entry.getKey(), entry.getValue(),
                    currentFrames, new HashSet<>(count.keySet()),
                    list, startFrameId,
                    null, indexInCurrentLevel,
                    typeMap == null ? null : typeMap.get(entry.getKey()));
            Set<String> connectedObjs = new HashSet<>();
            roots.add(node);
            connectedObjs.add(entry.getKey());
            connectedObjList.add(connectedObjs);

            int v= top1Map.getOrDefault(1, 0);
            if (v < entry.getValue()) {
                top1Map.put(1, entry.getValue());
            }

            buildRecursively(countMaps, count, list, typeMap, new HashSet<>(Arrays.asList(entry.getKey())), node, top1Map, connectedObjs);
        }
        return partitionCreator.createPartition(roots, connectedObjList, list, top1Map, startFrameId, size);
    }

    public void buildRecursively(List<Map<String, Integer>> allCountMaps, Map<String, Integer> count, List<Set<String>> list,
                                 Map<String, CLabel> typeMap, Set<String> prefix, Node<T, F> parentNode,
                                 Map<Integer, Integer> top1Map, Set<String> connectedObjs) {
        // 1. filter all frames containing the prefix
//        List<Set<String>> filteredList = new ArrayList<>();
        //2. collect other object counts.
        Map<String, Integer> filteredCount = new HashMap<>();
        // 3. current frames.
//        List<Integer> currentFrames = new ArrayList<>();
        Map<String, List<Integer>> filteredFrameMap = new HashMap<>();
        int frameId = startFrameId;
        for (Set<String> frame:list) {
            if (frame.containsAll(prefix)) {
//                filteredList.add(frame);
//                currentFrames.add(frameId);
                for (String id : frame) {
                    if (!prefix.contains(id) && count.containsKey(id)) {
                        int v = filteredCount.computeIfAbsent(id, x-> 0);
                        filteredCount.put(id, v+1);

                        List<Integer> frameList = filteredFrameMap.computeIfAbsent(id, x-> new ArrayList<>());
                        frameList.add(frameId);
                    }
                }
            }
            frameId ++;
        }
        // add current map.
        allCountMaps.add(filteredCount);

        int indexInCurrentLevel = 0;
        while(!filteredCount.isEmpty()) {
            indexInCurrentLevel ++;
            // sort.
            List<Map.Entry<String, Integer>> allEntries = new ArrayList<>(filteredCount.entrySet());
            sortCountMap(allEntries);
            Map.Entry<String, Integer> top = allEntries.get(0);
            int currentCount = filteredCount.get(top.getKey());


            List<Integer> currentFrames = filteredFrameMap.remove(top.getKey());
            Node<T, F> node = nodeCreator.createNewNode(top.getKey(), top.getValue(),
                    currentFrames, new HashSet<>(filteredCount.keySet()),
                    list, startFrameId,
                    parentNode, indexInCurrentLevel,
                    typeMap == null ? null : typeMap.get(top.getKey()));
            connectedObjs.add(top.getKey());
            if (node != null) {
                parentNode.addNext(node);
            } else {
                node = parentNode;
            }

            // also remove count from all count Maps
            for (Map<String, Integer> countMap : allCountMaps) {
                if (countMap.get(top.getKey()) == currentCount) {
                    // remove it.
                    countMap.remove(top.getKey());
                }
            }

            Set<String> newPrefix = new HashSet<>(prefix);
            newPrefix.add(top.getKey());

            int v = top1Map.getOrDefault(newPrefix.size(), 0);
            if (v < top.getValue()) {
                top1Map.put(newPrefix.size(), top.getValue());
            }

            buildRecursively(allCountMaps, filteredCount, list, typeMap, newPrefix, node, top1Map, connectedObjs);
        }

        // remove the current one.
        allCountMaps.remove(filteredCount);
    }

    void sortCountMap(List<Map.Entry<String, Integer>> allEntries) {
        Collections.sort(allEntries, (x1,x2)-> {
            int v = -x1.getValue().compareTo(x2.getValue());
            if (v == 0)
                return x1.getKey().compareTo(x2.getKey());
            else return v;
        });
    }

    public static void main(String[] args) {
        PartitionIndexBootstrap<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>, Byte> partitionIndex =
                new PartitionIndexBootstrap<>(0, new CTNodeWIntervalCreator(), new CommonPartitionCreator<>());
        List<Set<String>>  objFrams = Arrays.asList(
//                new HashSet<>(Arrays.asList("A", "B", "C", "X", "a", "b", "c")),
//                new HashSet<>(Arrays.asList("A", "B", "D", "X", "a", "b", "d")),
//                new HashSet<>(Arrays.asList("A", "C", "D", "X", "a", "c", "d"))
//                new HashSet<>(Arrays.asList("A", "B", "C", "Y", "a", "b", "c")),
//                new HashSet<>(Arrays.asList("A", "B", "F", "Y", "a", "c", "d")),
//                new HashSet<>(Arrays.asList("A", "D", "F", "Y", "a", "c"))
//                new HashSet<>(Arrays.asList("A", "D", "Z", "a", "c", "d")),
//                new HashSet<>(Arrays.asList("C", "D", "Z",  "a", "b", "d")),
//                new HashSet<>(Arrays.asList("A", "C", "D", "a", "c"))
//                new HashSet<>(Arrays.asList("a", "c", "d")),
//                new HashSet<>(Arrays.asList("a", "b", "d")),
//                new HashSet<>(Arrays.asList("a", "c"))
                new HashSet<>(Arrays.asList("a", "c", "d", "e", "f", "g")),
                new HashSet<>(Arrays.asList("a", "b", "d")),
                new HashSet<>(Arrays.asList("a", "c"))
        );
        Map<String, CLabel> typeMap = new HashMap<>();
        for (Set<String> ids : objFrams) {
            for (String id : ids) {
                if (Character.isUpperCase(id.charAt(0))) {
                    typeMap.put(id, CLabel.CAR);
                } else {
                    typeMap.put(id, CLabel.PERSON);
                }
            }
        }
        List<Node<String, PayloadClassIntervals>> roots= partitionIndex.build(objFrams, typeMap).getRoots();
//        List<NodeWithIntervals> roots= partitionIndex.build(Arrays.asList(
//                new HashSet<>(Arrays.asList("A", "C")),
//                new HashSet<>(Arrays.asList("A", "B")),
//                new HashSet<>(Arrays.asList("B", "D"))
//        )).getRoots();
        for (Node<String, PayloadClassIntervals> node: roots) {
            System.out.println("Root:" + node.getKey()+";"+ node.getPayload().getCount());
        }
    }

}
