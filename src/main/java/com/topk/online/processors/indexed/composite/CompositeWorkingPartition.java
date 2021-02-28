package com.topk.online.processors.indexed.composite;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.processors.indexed.Candidate;
import com.topk.online.processors.indexed.utils.BasePartitionHolder;
import com.topk.utils.Combinations;
import com.topk.utils.Combinations3;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;

public class CompositeWorkingPartition<T extends PayloadClassIntervals> {

    private static Logger LOG = LogManager.getLogger(CompositeWorkingPartition.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<CompositeCandidate<T>> toVisitNodes;

    int remainingCount = 0;

    int processedCount = 0;

    Map<String, List<Interval>> indexMap;
    Map<String, Integer> countMap;

    BasePartition<Node<String, T>, IndexedPartitionPayload> basePartition;

    Set<String> commonObjs;
    Map<CLabel, Integer> conditionMap;

    Set<Set<String>> computedSets;

    public CompositeWorkingPartition(BasePartition<Node<String, T>,
            IndexedPartitionPayload> basePartition,
            Set<String> commonObjs, Map<CLabel, Integer> conditionMap,
                                     Set<Set<String>> computedSets) {
        this.toVisitNodes = new ArrayList<>();
        for (Node<String, T> node: basePartition.getRoots()) {
            CompositeCandidate<T> cc = new CompositeCandidate<>();
            cc.node = node;
            cc.prefixMap = new HashMap<>();
            this.toVisitNodes.add(cc);
        }
        sortNodes();
        this.remainingCount = toVisitNodes.get(0).node.getPayload().getCount();
        this.indexMap = basePartition.getPayload().getIntervalMap();
        this.countMap = new HashMap<>();
        this.countMap.putAll(basePartition.getPayload().getNodeCountMap());
        this.basePartition = basePartition;

        this.commonObjs = commonObjs;
        this.conditionMap = conditionMap;
        this.computedSets = computedSets;
    }

    void sortNodes() {
        Collections.sort(toVisitNodes, (x1, x2)->
                Integer.compare(x2.node.getPayload().getCount(), x1.node.getPayload().getCount()));
    }

    void updateCountMap(String key) {
        int count = this.countMap.get(key);
        count --;
        if (count == 0) {
            this.countMap.remove(key);
        } else {
            this.countMap.put(key, count);
        }
    }

    boolean isSatisfied(Map<CLabel, List<String>> givenValues, CLabel currentLabel) {
        for (CLabel clabel: conditionMap.keySet()) {
            int expect = conditionMap.get(clabel);
            if (clabel == currentLabel) {
                expect --;
            }
            int given = givenValues.getOrDefault(clabel, Collections.emptyList()).size();
            if (given < expect) return false;
        }
//        LOG.debug("cond: [{}], given: [{}], current: [{}], isSatisfied: [{}]", conditionMap,
//                givenValues, currentLabel, true);
        return true;
    }

    List<List<String>> genObjectSets(Map<CLabel, List<String>> givenValues,
                                    CLabel currentLabel, String currentObj) {
        // 1. convert to a list of objs.
        List<List<List<String>>> genObjSetList = new ArrayList<>();
        // 1. gen all object sets from each type.
        for (CLabel type: conditionMap.keySet()) {
            if (type == currentLabel) {
                // remove the current node.
                List<String> prefixList = givenValues.get(type);
                List<List<String>> prefixes = Combinations.combinations(
                        prefixList, conditionMap.get(type) - 1);
                // gen all
                prefixes.forEach(i -> i.add(currentObj));
                genObjSetList.add(prefixes);
            } else {
                List<List<String>> prefixes = Combinations.combinations(
                        givenValues.get(type), conditionMap.get(type)
                );
                genObjSetList.add(prefixes);
            }
        }
        // gen composition across all lists.
        return Combinations3.combinations(genObjSetList);
    }

    Map<CLabel, List<String>> genPrefixMap(Map<CLabel, List<String>> previousMap, CLabel currentLabel, String currentKey) {
        Map<CLabel, List<String>> newMap = new HashMap<>();
        boolean added = false;
        for (Map.Entry<CLabel, List<String>> entry: previousMap.entrySet()) {
            if (entry.getKey() == currentLabel) {
                List<String> nl = new ArrayList<>(entry.getValue());
                nl.add(currentKey);
                newMap.put(entry.getKey(), nl);
                added = true;
            } else {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        if (!added) {
            newMap.put(currentLabel, new ArrayList<>(Arrays.asList(currentKey)));
        }
        return newMap;
    }

    void genObjectSetsAndPutResult(CompositeCandidate<T> current,
                                   Map<Set<String>, List<Interval>> resultMap,
                                   boolean checkSet) {
        // gen result map.
        List<List<String>> allCombs = genObjectSets(current.getPrefixMap(),
                current.node.getPayload().getLabel(), current.getNode().getKey());
        // add to next.
        for (List<String> comb: allCombs) {
            // add only if absent.
            Set<String> set = new HashSet<>(comb);
            if (!resultMap.containsKey(set)) {
                if (!checkSet || !computedSets.contains(set)) {
                    resultMap.put(set, current.getNode().getPayload().getIntervals());
                }
                computedSets.add(set);
            }
        }
    }


    public Map<Set<String>, List<Interval>> visitByLevel(int visitLevel) {
        // retrieve all objects with depth=visitLevel.
        Map<Set<String>, List<Interval>> resultMap = new HashMap<>();
        for (int i=0; i < visitLevel; i++) {
            List<CompositeCandidate<T>> newList = new ArrayList<>();
            while(toVisitNodes.size() > 0) {
                CompositeCandidate<T> current = toVisitNodes.remove(0);
                processCurrentCandidate(current, resultMap, newList, false);
            }
            toVisitNodes = newList;
        }
        // sort.
        sortNodes();
        if (toVisitNodes.size() == 0) {
            remainingCount = 0;
        } else {
            remainingCount = toVisitNodes.get(0).node.getPayload().getCount();
        }
        return resultMap;
    }

    void processCurrentCandidate(CompositeCandidate<T> current, Map<Set<String>, List<Interval>> resultMap,
                                 List<CompositeCandidate<T>> newList, boolean checkSet) {
        updateCountMap(current.node.getKey());
        Map<CLabel, List<String>> toAddPrefix = null;
        String currentKey = current.node.getKey();
        CLabel currentLabel = current.node.getPayload().getLabel();

        // add current prefixes.
        // check is satisfied.
        if (current.isSatisfied()) {
            LOG.debug("already satisfied: [{}], [{}]",
                    current.getPrefixMap(), current.getNode().getKey());
            if (commonObjs.contains(currentKey)) {
                LOG.debug("gen results");
                // gen result map.
                genObjectSetsAndPutResult(current, resultMap, checkSet);
                // gen
                toAddPrefix = genPrefixMap(current.prefixMap, currentLabel, currentKey);
            } else {
                LOG.debug("continue to next");
                // simply continue to next.
                toAddPrefix = current.prefixMap;
            }
        } else {
            //
            if (isSatisfied(current.getPrefixMap(), currentLabel)) {
                // first satisfied. remove objects that are not in common.
                if (current.first) {
                    LOG.debug("satisfied for the first time: [{}], [{}]",
                            current.getPrefixMap(), current.getNode().getKey());
                    // prune
                    toAddPrefix = new HashMap<>();
                    for (Map.Entry<CLabel, List<String>> entry: current.prefixMap.entrySet()) {
                        // NOTE: should not append directly to the values.
                        List<String> list = new ArrayList<>();
                        for (String s : entry.getValue()) {
                            if (commonObjs.contains(s)) {
                                list.add(s);
                            }
                        }
                        if (entry.getKey() == currentLabel) {
                            list.add(currentKey);
                        }
                        toAddPrefix.put(entry.getKey(), list);
                    }
                    current.first = false;
                } else {
                    LOG.debug("satisfied for the second time: [{}], [{}]",
                            current.getPrefixMap(), current.getNode().getKey());
                    toAddPrefix = genPrefixMap(current.prefixMap, currentLabel, currentKey);
                    // make satisfied.
                    current.satisfied = true;
                }
                // gen result map.
                genObjectSetsAndPutResult(current, resultMap, checkSet);
            } else {
                LOG.debug("still not satisfied: [{}], [{}]",
                        current.getPrefixMap(), current.getNode().getKey());
                // add direct to next.
                toAddPrefix = genPrefixMap(current.prefixMap, currentLabel, currentKey);
            }
        }
        if (current.node.getNext() != null) {
            for (Node<String, T> node: current.node.getNext()) {
                CompositeCandidate<T> cc = new CompositeCandidate<>();
                cc.node = node; cc.prefixMap = toAddPrefix;
                cc.satisfied = current.satisfied; cc.first = current.first;
                newList.add(cc);
                LOG.debug("add to next: [{}], [{}]", toAddPrefix, node.getKey());
            }
        }
    }

    public CompositeCandidate<T> nextNode(Map<Set<String>, List<Interval>> resultMap) {
        CompositeCandidate<T> current = null;
        if (toVisitNodes.size() > 0) {
            do {
                if (toVisitNodes.size() == 0) return current;
                current = toVisitNodes.remove(0);
                processCurrentCandidate(current, resultMap, toVisitNodes, true);

                sortNodes();
                // update remaining
                if (toVisitNodes.size() == 0) {
                    remainingCount = 0;
                } else {
                    remainingCount = toVisitNodes.get(0).node.getPayload().getCount();
                }
                processedCount ++;

            } while(resultMap.isEmpty());
        }
        return current;
    }

    public boolean hasNodeUnvisited(String key) {
        return countMap.containsKey(key);
    }

    public int getProcessedCount() {
        return processedCount;
    }

    public List<Interval> getIntervalsWithKey(String key) {
        return indexMap.get(key);
    }

    public int getRemainingCount() {
        return remainingCount;
    }

    public void setRemainingCount(int remainingCount) {
        this.remainingCount = remainingCount;
    }

    public BasePartition<Node<String, T>, IndexedPartitionPayload> getBasePartition() {
        return basePartition;
    }
}
