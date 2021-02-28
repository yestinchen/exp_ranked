package com.topk.online.component;

import com.topk.offline.bean.*;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.PartitionWindow;
import com.topk.online.processors.ConditionItem;
import com.topk.online.retriever.*;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MPWGen {

    public static <T, F extends PayloadCount, P, K, Q> List<MultiPartitionWindow<T, F, P, K, Q>> genMPWs(
            Map<CLabel, List<BasePartition<P, Q>>> partitionMap,
            List<List<ConditionItem>> conditions,
            int partitionNum, int partitionSize,
            BiFunction<Set<String>, List<String>, NodeAssertion<T>> nodeAssertionFunction,
            RootNodeExtractor<T, F, P> extractor,
            BiFunction<Set<String>, List<String>, KeyMapper<T, K>> keyMapperFunc,
            BiFunction<Set<String>, List<String>, EarlyStopper<T, F>> earlyStopperFunc) {
        Map<CLabel, ConditionItem> typeConditions = new HashMap<>();
        for (List<ConditionItem> cil: conditions) {
            for (ConditionItem ci: cil) {
                if (typeConditions.containsKey(ci.getType())) {
                    System.err.println("ERROR! duplicated types:" + ci.getType());
                    System.exit(-1);
                }
                typeConditions.put(ci.getType(), ci);
            }
        }

        List<Map<CLabel, PartitionWindow<T, F, P, K, Q>>> partitionWindows = new ArrayList<>();

        // they should share the same # of partitions.
        int totalPartitions = partitionMap.values().iterator().next().size();


        // set up the partition windows.
        {
            Map<CLabel, PartitionWindow<T, F, P, K, Q>> pwMap = new HashMap<>();
            int i =0;
            for (CLabel type : typeConditions.keySet()) {
                PartitionWindow<T, F, P, K, Q> pw1 = new PartitionWindow<>();
                pw1.setStart(0);
                pwMap.put(type, pw1);
            }
            for (; i < partitionNum && i < totalPartitions; i++) {
                Set<CLabel> addedSet = new HashSet<>();
                int pstart = 0, psize = 0;
                for (ConditionItem ci : typeConditions.values()) {
                    BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                    PartitionWindow<T, F, P, K, Q> pw1 = pwMap.get(ci.getType());
                    if (p.getTop1Map().containsKey(ci.getObjNum())) {
                        pw1.getPartitions().add(p);
                        addedSet.add(ci.getType());
                    }
                    pw1.setEnd(p.getStartFrame()+p.getSize() - 1);
                    pstart = p.getStartFrame();
                    psize = p.getSize();
                }

                // add dummpy partition.
                BasePartition<P, Q> dummyPartition = new BasePartition<>();
                dummyPartition.setStartFrame(pstart);
                dummyPartition.setSize(psize);
                if (addedSet.size() < typeConditions.size() && addedSet.size() > 0) {
                    for (ConditionItem ci : typeConditions.values()) {
                        if (!addedSet.contains(ci.getType())) {
                            pwMap.get(ci.getType()).getPartitions().add(dummyPartition);
                        }
                    }
                }
            }
            // only keep non-empty ones.
            Map<CLabel, PartitionWindow<T, F, P, K, Q>> toAddMap = new HashMap<>();
            for (Map.Entry<CLabel, PartitionWindow<T, F, P, K, Q>> entry: pwMap.entrySet() ) {
                if (entry.getValue().getPartitions().size() > 0) {
                    toAddMap.put(entry.getKey(), entry.getValue());
                }
            }
            if (toAddMap.size() > 0) partitionWindows.add(toAddMap);

            // add the rest.
            Map<CLabel, PartitionWindow<T, F, P, K, Q>> lastPwMap = pwMap;
            for (; i < totalPartitions; i++) {
                pwMap = new HashMap<>();
                Set<CLabel> addedSet = new HashSet<>();
                int pstart = 0, psize = 0;
                for (ConditionItem ci : typeConditions.values()) {
                    PartitionWindow<T, F, P, K, Q> lastPw = lastPwMap.get(ci.getType());
                    PartitionWindow<T, F, P, K, Q> pwn = new PartitionWindow<>();
                    List<BasePartition<P, Q>> pwnList = new ArrayList<>(lastPw.getPartitions());
                    if (pwnList.size() > 0 && lastPw.getStart() == pwnList.get(0).getStartFrame()) {
                        // remove first one if needed.
                        pwnList.remove(0);
                    }
                    BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                    if (p.getTop1Map().containsKey(ci.getObjNum())) {
                        pwnList.add(p);
                        addedSet.add(ci.getType());
                    }
                    pwn.setPartitions(pwnList);
                    pwn.setStart((i - partitionNum + 1) *partitionSize);
                    pwn.setEnd(p.getStartFrame() + p.getSize() - 1);
                    pwMap.put(ci.getType(), pwn);
                    pstart = p.getStartFrame();
                    psize = p.getSize();
                }

                BasePartition<P, Q> dummyPartition = new BasePartition<>();
                dummyPartition.setSize(psize);
                dummyPartition.setStartFrame(pstart);
                if (addedSet.size() < typeConditions.size() && addedSet.size() > 0) {
                    for (ConditionItem ci : typeConditions.values()) {
                        if (!addedSet.contains(ci.getType())) {
                            pwMap.get(ci.getType()).getPartitions().add(dummyPartition);
                        }
                    }
                }
                // add them.
                toAddMap = new HashMap<>();
                for (Map.Entry<CLabel, PartitionWindow<T, F, P, K, Q>> entry: pwMap.entrySet()) {
                    if (entry.getValue().getPartitions().size() > 0) {
                        toAddMap.put(entry.getKey(), entry.getValue());
                    }
                }
                if (toAddMap.size() > 0) {
                    partitionWindows.add(toAddMap);
                }
                lastPwMap = pwMap;
            }
        }

        // retrieve prefix map for each partition window.
        List<MultiPartitionWindow<T, F, P, K, Q>> multiPartitionWindows = new ArrayList<>();
        for (Map<CLabel, PartitionWindow<T, F, P, K, Q>> pwm: partitionWindows) {
            for (Map.Entry<CLabel,PartitionWindow<T, F, P, K, Q>> entry : pwm.entrySet()) {
                ConditionItem ci = typeConditions.get(entry.getKey());
                entry.getValue().computeScore(ci.getObjNum(), ci.getLambda(),
                        entry.getValue().selectCommonObjects(),
                        nodeAssertionFunction, extractor, keyMapperFunc, earlyStopperFunc);
            }
            MultiPartitionWindow<T, F, P, K, Q> multiPartitionWindow = new MultiPartitionWindow<>(pwm);
            multiPartitionWindow.computeScore(conditions);
            multiPartitionWindows.add(multiPartitionWindow);
        }
        return multiPartitionWindows;
    }
}
