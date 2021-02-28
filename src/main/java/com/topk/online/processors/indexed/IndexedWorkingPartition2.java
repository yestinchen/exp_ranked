package com.topk.online.processors.indexed;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.processors.indexed.utils.BasePartitionHolder;
import com.topk.utils.Combinations;

import java.util.*;

public class IndexedWorkingPartition2<T extends PayloadIntervals> {

    List<Candidate<T>> toVisitNodes;

    int remainingCount = 0;

    int processedCount = 0;

    Map<String, List<Interval>> indexMap;
    Map<String, Integer> countMap;

    Set<String> commonObjs;
    int objNum;

    BasePartition<Node<String, T>,
            IndexedPartitionPayload> basePartition;

    public IndexedWorkingPartition2(BasePartition<Node<String, T>,
            IndexedPartitionPayload> basePartition, Set<String> commonObjs, int objNum) {
        this.toVisitNodes = new ArrayList<>();
        for (Node<String, T> node : basePartition.getRoots()) {
            Candidate<T> c = new Candidate<>();
            c.node = node;
            c.prefix = Collections.emptyList();
            this.toVisitNodes.add(c);
        }
        sortNodes();
        this.remainingCount = toVisitNodes.get(0).node.getPayload().getCount();
        this.indexMap = basePartition.getPayload().getIntervalMap();
        this.countMap = new HashMap<>();
        // copy
        this.countMap.putAll(basePartition.getPayload().getNodeCountMap());
        this.basePartition = basePartition;

        this.commonObjs = commonObjs;
        this.objNum = objNum;
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

    void genObjSetsAndPutResult(Candidate<T> current, Map<Set<String>, List<Interval>> resultMap) {
        List<List<String>> prefixLists;
        if (current.prefix.size() == objNum - 1) {
            prefixLists = Arrays.asList(current.prefix);
        } else {
            prefixLists = Combinations.combinations(current.prefix, objNum - 1);
        }
        for (List<String> list : prefixLists) {
            Set<String> set = new HashSet<>(list);
            set.add(current.getNode().getKey());
            if (!resultMap.containsKey(set)) {
                resultMap.put(set, current.getNode().getPayload().getIntervals());
            }
        }
    }

    void addNextCandidatesToList(Candidate<T> current, List<String> toAddPrefix, List<Candidate<T>> newList) {
        if (current.node.getNext() != null) {
            for (Node<String, T> node : current.node.getNext()) {
                Candidate<T> c = new Candidate<>();
                c.node = node;c.prefix = toAddPrefix;
                c.first = current.first; c.satisfied = current.satisfied;
                newList.add(c);
            }
        }
    }

    List<String> genPrefixListWithPruning(Candidate<T> current) {
        List<String> toAddPrefix = new ArrayList<>();
        for (String obj : current.prefix) {
            // filter.
            if (commonObjs.contains(obj)) {
                toAddPrefix.add(obj);
            }
        }
        return toAddPrefix;
    }

    private void visitOneCandidate(Candidate<T> current,
                                   Map<Set<String>, List<Interval>> resultMap,
                                   List<Candidate<T>> nextContainer) {
        updateCountMap(current.node.getKey());
        // remove one.
        List<String> toAddPrefix = null;
        if (current.isSatisfied()) {
            // continue.
            if (commonObjs.contains(current.getNode().getKey())) {
                // have to generate result.
                genObjSetsAndPutResult(current, resultMap);
                toAddPrefix = new ArrayList<>(current.prefix);
                toAddPrefix.add(current.node.getKey());
            } else {
                toAddPrefix = current.prefix;
            }
        } else {
            if (current.prefix.size() >= objNum -1) {
                if (current.first) {
                    toAddPrefix = genPrefixListWithPruning(current);
                    // only add if the current key is selected.
                    if (commonObjs.contains(current.getNode().getKey())) {
                        toAddPrefix.add(current.node.getKey());
                    }
                    current.first = false;
                } else {
                    // simple adds.
                    toAddPrefix = new ArrayList<>(current.prefix);
                    toAddPrefix.add(current.node.getKey());
                    current.satisfied = true;
                }
                genObjSetsAndPutResult(current, resultMap);
            } else {
                // still not satisfied.
                toAddPrefix = new ArrayList<>(current.prefix);
                toAddPrefix.add(current.node.getKey());
            }
        }
        // add next candidates.
        addNextCandidatesToList(current, toAddPrefix, nextContainer);
    }

    public Map<Set<String>, List<Interval>> visitByLevel(int visitLevel) {
        // retrieve all objects with depth=objNum
        Map<Set<String>, List<Interval>> resultMap = new HashMap<>();
        for (int i=0; i < visitLevel; i++) {
            List<Candidate<T>> newList = new ArrayList<>();
            while(toVisitNodes.size() > 0) {
                // visit node & add to new list.
                Candidate<T> current = toVisitNodes.remove(0);
                visitOneCandidate(current, resultMap, newList);
            }
            toVisitNodes = newList;
        }
        sortAndUpdateRemainingCount();
        return resultMap;
    }

    public Map<Set<String>, List<Interval>> visitByNum(int visitNum) {

        // retrieve all objects with depth=objNum
        Map<Set<String>, List<Interval>> resultMap = new HashMap<>();
        for (int i=0; i < visitNum; i++) {
            Candidate<T> current = nextNode(objNum);
            // process current.
            if (commonObjs.contains(current.getNode().getKey())) {
                genObjSetsAndPutResult(current, resultMap);
            }
        }
        return resultMap;
    }

    private void sortAndUpdateRemainingCount() {
        // sort & the rest.
        sortNodes();
        if (toVisitNodes.size() == 0) {
            remainingCount = 0;
        } else {
            remainingCount = toVisitNodes.get(0).node.getPayload().getCount();
        }
    }

    public Candidate<T> nextNode(int objNum) {
        Candidate<T> current= null;
        if (toVisitNodes.size() > 0) {
            do {
                if (toVisitNodes.size() == 0) return null;
                current = toVisitNodes.remove(0);

                updateCountMap(current.node.getKey());

                if (current.node.getNext() != null) {
                    // add the rest.
                    List<String> newList =new ArrayList<>(current.prefix);
                    newList.add(current.node.getKey());
                    addNextCandidatesToList(current, newList, toVisitNodes);
                }
                sortAndUpdateRemainingCount();
                processedCount ++;
            } while(current.prefix.size() < objNum - 1);
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
