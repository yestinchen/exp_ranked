package com.topk.online;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.PayloadCount;
import com.topk.online.retriever.*;
import com.topk.utils.BitSetUtils;

import java.util.*;
import java.util.function.BiFunction;

/**
 *
 * @param <T> the key type in each node
 * @param <F> the payload type in each node
 * @param <P> the root obj type in each partition.
 */
public class PartitionWindow<T, F extends PayloadCount, P, K, Q> {
    List<BasePartition<P, Q>> partitions = new ArrayList<>();
    List<Map.Entry<Set<K>, Integer>> sortedCountEntries = null;
    List<Map<Set<K>, PrefixList<K>>> prefixMaps = new ArrayList<>();

    int score = -1;
    int start;
    int end;

    public List<Map<Set<K>, PrefixList<K>>> getPrefixMaps() {
        return prefixMaps;
    }

    public List<BasePartition<P, Q>> getPartitions() {
        return partitions;
    }

    public int getScore() {
        return score;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public List<Map.Entry<Set<K>, Integer>> getSortedCountEntries() {
        return sortedCountEntries;
    }

    public void setPartitions(List<BasePartition<P, Q>> partitions) {
        this.partitions = partitions;
    }

    protected Map<Set<K>, PrefixList<K>> retrievePrefixMap(BasePartition<P, Q> partition, NodeAssertion<T> ns,
                                                           int objNum, int lambda, RootNodeExtractor<T, F, P> extractor,
                                                           KeyMapper<T, K> keyMapper,
                                                           EarlyStopper<T, F> earlyStopper) {
        if (partition.getRoots() == null || partition.getRoots().isEmpty()) {
            return Collections.emptyMap();
        }
        PartitionRetriever<T, F, P, K, Q> retriever = new PartitionRetriever<>(extractor, ns, keyMapper, earlyStopper);
        return retriever.retrieve(partition,  objNum, lambda);
    }

    public Set<String> selectCommonObjects() {
        Map<String, Integer> partitionCountMap = new HashMap<>();
        Set<String> selectObjs = new HashSet<>();
        for (BasePartition<P, Q> p : partitions) {
            if (p.getObjs() != null) {
                for (String obj : p.getObjs()) {
                    int v = partitionCountMap.getOrDefault(obj, 0);
                    v++;
                    partitionCountMap.put(obj, v);
                }
            }
        }

        for (Map.Entry<String, Integer> entry: partitionCountMap.entrySet()) {
            if (entry.getValue() >= 2) {
                selectObjs.add(entry.getKey());
            }
        }
        return selectObjs;
    }

    public Map<CLabel, Set<String>> selectCommonObjectsGroupByLabel() {
        Map<CLabel, Map<String, Integer>> labelMap = new HashMap<>();
        for (BasePartition<P, Q> p: partitions) {
            if (p.getTypeBitSetMap() != null) {
                Map<CLabel, BitSet> objMap = p.getTypeBitSetMap();
                for (CLabel label: objMap.keySet()) {
                    for (String obj: BitSetUtils.select(objMap.get(label), p.getObjs()) ) {
                        int v = labelMap.computeIfAbsent(label,
                                x -> new HashMap<>()).getOrDefault(obj, 0);
                        v ++;
                        labelMap.get(label).put(obj, v);
                    }
                }
            }
        }

        Map<CLabel, Set<String>> commonObjs = new HashMap<>();
        for (CLabel label: labelMap.keySet()) {
            for (Map.Entry<String, Integer> entry : labelMap.get(label).entrySet()) {
                if (entry.getValue() >= 2) {
                    commonObjs.computeIfAbsent(label, x -> new HashSet<>()).add(entry.getKey());
                }
            }
        }
        return commonObjs;
    }

    public int computeScore(int objNum, int lambda, Set<String> objs,
                            BiFunction<Set<String>, List<String>, NodeAssertion<T>> assertFunc,
                            RootNodeExtractor<T, F, P> extractor,
                            BiFunction<Set<String>, List<String>, KeyMapper<T, K>> keyMapperFunc,
                            BiFunction<Set<String>, List<String>, EarlyStopper<T, F>> earlyStopperFunc) {
        if (sortedCountEntries != null) return score;

        if (start == 0) {
            System.out.println("stop");
        }

        // 1. compute select objects.
        List<Set<String>> selectedList = new ArrayList<>();
        // select.
        for (BasePartition<P, Q> p : partitions) {
            if (p.getObjs() != null) {
                Set<String> selected = new HashSet<>();
                for (String obj : p.getObjs()) {
                    if (objs.contains(obj)) {
                        selected.add(obj);
                    }
                }
                selectedList.add(selected);
            } else {
                selectedList.add(Collections.emptySet());
            }
        }

        List<Map<Set<K>, PrefixList<K>>> maps = new ArrayList<>();
        // retrieve from each partition.
        for (int i =0; i < partitions.size(); i++) {
            BasePartition<P, Q> p = partitions.get(i);
            if (p.getRoots() != null) {
                // NULL means dummy partition, no need to process.
                Set<String> selectedObjs = selectedList.get(i);

                // FIXME: may delete some conditions?
                if (selectedObjs.size() >= objNum || p.getTop1Map().containsKey(objNum)) {
                    NodeAssertion<T> ns = assertFunc.apply(selectedObjs, p.getObjs());
                    KeyMapper<T, K> keyMapper = keyMapperFunc.apply(selectedObjs, p.getObjs());
                    EarlyStopper<T, F> earlyStopper = earlyStopperFunc.apply(selectedObjs, p.getObjs());
                    maps.add(retrievePrefixMap(p, ns, objNum, lambda, extractor,
                            keyMapper, earlyStopper));
                }
            } else {
                maps.add(Collections.emptyMap());
            }
        }

        Map<Set<K>, Integer> countMap = new HashMap<>();
        for (Map<Set<K>, PrefixList<K>> map: maps) {
            for (Map.Entry<Set<K>, PrefixList<K>> e : map.entrySet()) {
                int v = countMap.getOrDefault(e.getKey(), 0);
                countMap.put(e.getKey(), v + e.getValue().getValue());
            }
        }
        // sort accordingly.
        List<Map.Entry<Set<K>, Integer>> countEntries = new ArrayList<>(countMap.entrySet());
        Collections.sort(countEntries, (x1,x2)-> -Integer.compare(x1.getValue(), x2.getValue()));

        prefixMaps = maps;
        sortedCountEntries = countEntries;
        if (countEntries.size() > 0) {
            score = countEntries.get(0).getValue();
        }
        return score;
    }

    public int estimateScore(int objNum) {
        if (score > 0) return score;
        score = 0;
        for (BasePartition<P, Q> p : partitions) {
            score += p.getTop1Map().getOrDefault(objNum, 0);
        }
        return score;
    }

    public void updateEstimateScore(int score) {
        this.score = score;
    }

    public int computeScore(Map<Integer, Map<Set<K>, PrefixList<K>>> partitionPrefixMap) {

        if (sortedCountEntries != null) return score;

        List<Map<Set<K>, PrefixList<K>>> maps = new ArrayList<>();
        for (BasePartition<P, Q> p : partitions) {
            maps.add(partitionPrefixMap.get(p.getStartFrame()));
        }
        Map<Set<K>, Integer> countMap = new HashMap<>();
        for (Map<Set<K>, PrefixList<K>> map: maps) {
            for (Map.Entry<Set<K>, PrefixList<K>> e : map.entrySet()) {
                int v = countMap.getOrDefault(e.getKey(), 0);
                countMap.put(e.getKey(), v + e.getValue().getValue());
            }
        }
        // sort accordingly.
        List<Map.Entry<Set<K>, Integer>> countEntries = new ArrayList<>(countMap.entrySet());
        Collections.sort(countEntries, (x1,x2)-> -Integer.compare(x1.getValue(), x2.getValue()));

        prefixMaps = maps;
        sortedCountEntries = countEntries;
        if (countEntries.size() > 0) {
            score = countEntries.get(0).getValue();
        }
        return score;
    }
}
