package com.topk.online.retriever;

import com.topk.offline.*;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadCount;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.PrefixList;
import com.topk.online.interval.PrefixListWithInterval;
import com.topk.utils.Combinations;

import java.util.*;

public class PartitionRetriever<T, F extends PayloadCount, P, K, Q> {

    RootNodeExtractor<T, F, P> extractor;
    NodeAssertion<T> nodeAssertion;
    KeyMapper<T, K> keyMapper;
    EarlyStopper<T, F> earlyStopper;

//    public PartitionRetriever(RootNodeExtractor<T, F, P> extractor, NodeAssertion<T> nodeAssertion) {
//        this(extractor, nodeAssertion, new BitSetGenNull<>(), null);
//    }

    public PartitionRetriever(RootNodeExtractor<T, F, P> extractor, NodeAssertion<T> nodeAssertion,
                              KeyMapper<T, K> keyMapper, EarlyStopper<T, F> earlyStopper) {
        this.extractor = extractor;
        this.nodeAssertion = nodeAssertion;
        this.keyMapper = keyMapper;
        this.earlyStopper = earlyStopper;
    }

    public Map<Set<K>, PrefixList<K>> retrieve(BasePartition<P, Q> p, int num, int lambda) {
        Map<Set<K>, PrefixList<K>> map = new HashMap<>();
        for (P root : p.getRoots()) {
            collectObjectSets(extractor.extract(root), num, lambda, new ArrayList<>(),
                    new ArrayList<>(), map);
        }
        return map;
    }

    void collectObjectSets(Node<T, F> node, int num, int lambda, List<K> prefix, List<K> selectedPrefix,
                           Map<Set<K>, PrefixList<K>> map) {
        boolean isSelected =nodeAssertion.isSelected(node);
        // process
        int minValue = node.getPayload().getCount();
        K mappedKey = keyMapper.map(node.getKey());
        // 1. select selected objs.
        if (isSelected) {
            // select any num -1 ids from prefix.
            List<List<K>> prefixes = Combinations.combinations(selectedPrefix, num - 1);
            for (List<K> p : prefixes) {
                List<K> newList = new ArrayList<>(p);
                newList.add(mappedKey);
                // sort.
                newList.sort(null);
                Set<K> key = new HashSet<>(newList.subList(0, newList.size()-lambda));
                Set<K> lastId = new HashSet<>(newList.subList(newList.size() -lambda, newList.size()));
                PrefixList<K> prefixList = map.computeIfAbsent(key, x -> createPrefixList(node.getPayload()));

                //FIXME: will different path share the same object set? Could be
//                if (prefixList.getObjValue().containsKey(lastId)) {
//                    System.err.println("PANIC!!!!");
//                } else {
//                }
                Integer lastMinValue = prefixList.getObjValue().get(lastId);
                if (lastMinValue == null || lastMinValue < minValue) {
//                    prefixList.getObjValue().put(lastId, minValue);
                    putNodeToPrefixList(prefixList, node, lastId);
                }
                if (minValue > prefixList.getValue()) {
                    prefixList.setValue(minValue);
                }
                // add
            }

            List<K> newPrefix = new ArrayList<>(selectedPrefix);
            newPrefix.add(mappedKey);

            selectedPrefix = newPrefix;

        }
        if (prefix.size() + 1 < num) {
            // continue.
            List<K> newPrefix = new ArrayList<>(prefix);
            newPrefix.add(mappedKey);

            prefix = newPrefix;
        } else if (prefix.size() + 1 == num) {
            // collect if not exist
            if(selectedPrefix.size() != prefix.size() || !isSelected) {
                List<K> newList = new ArrayList<>(prefix);
                newList.add(mappedKey);
                // sort.
                newList.sort(null);
                Set<K> key = new HashSet<>(newList.subList(0, newList.size()-lambda));
                Set<K> lastId = new HashSet<>(newList.subList(newList.size() -lambda, newList.size()));
                PrefixList<K> prefixList = map.computeIfAbsent(key, x -> createPrefixList(node.getPayload()));
                Integer lastMinValue = prefixList.getObjValue().get(lastId);
                if (lastMinValue == null || lastMinValue < minValue) {
//                    prefixList.getObjValue().put(lastId, minValue);
                    putNodeToPrefixList(prefixList, node, lastId);
                }
                if (minValue > prefixList.getValue()) {
                    prefixList.setValue(minValue);
                }
            }
        }

        if (earlyStopper.shouldStop(node)) return;

        // collect next.
        if (node.getNext() != null && node.getNext().size() != 0) {
            for (Node<T, F> n : node.getNext()) {
                collectObjectSets(n, num, lambda, prefix, selectedPrefix, map);
            }
        }

    }

    protected PrefixList<K> createPrefixList(F payload) {
        if (payload instanceof PayloadIntervals) {
            return new PrefixListWithInterval<>();
        } else {
            return new PrefixList<>();
        }
    }

    protected void putNodeToPrefixList(PrefixList<K> prefixList, Node<T, F> node, Set<K> id) {
        if (node.getPayload() instanceof PayloadIntervals) {
            PrefixListWithInterval<K> plwi = (PrefixListWithInterval<K>) prefixList;
            PayloadIntervals payloadI = (PayloadIntervals) node.getPayload();
            prefixList.getObjValue().put(id, payloadI.getCount());
            plwi.getIntervalMap().put(id, payloadI.getIntervals());
        } else {
            prefixList.getObjValue().put(id, node.getPayload().getCount());
        }
    }

//    public static void main(String[] args) {
//        PartitionIndex pi = new PartitionIndex();
////        Partition<Node> partition = pi.build(Arrays.asList(
////                new HashSet<>(Arrays.asList("A", "B", "C", "X")),
////                new HashSet<>(Arrays.asList("A", "B", "D", "X")),
////                new HashSet<>(Arrays.asList("A", "C", "D", "X"))
////        ));
//        BasePartition<Node<String, PayloadCount>, Byte> partition = pi.build(Arrays.asList(
//                new HashSet<>(Arrays.asList("A", "B", "C")),
//                new HashSet<>(Arrays.asList("A", "B", "D"))
//        ));
//        PartitionRetriever<String, PayloadCount, Node<String, PayloadCount>, String, Byte> partitionRetriever =
//                new PartitionRetriever<>(new SimpleRootNodeExtractor<>(),
//                        new NodeAssertionStr( new HashSet<>(Arrays.asList("B", "D"))),
//                        new KeyMapperDummy<>(), new EarlyStopperNever<>());
//        Map<Set<String>, PrefixList<String>> map =
////                partitionRetriever.retrieve(partition, new HashSet<>(Arrays.asList("A", "B", "C", "D", "X")), 2);
//                partitionRetriever.retrieve(partition, 2, 1);
//        for (Set<String> key: map.keySet()) {
//            System.out.println(key+" => " + map.get(key));
//        }
//    }
}
