package com.topk.online.component;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.PayloadCount;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.PartitionWindow;
import com.topk.online.processors.ConditionItem;
import com.topk.online.retriever.EarlyStopper;
import com.topk.online.retriever.KeyMapper;
import com.topk.online.retriever.NodeAssertion;
import com.topk.online.retriever.RootNodeExtractor;

import java.util.*;
import java.util.function.BiFunction;

public class MPWGenWGroupAndOnly {

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
                PartitionWindow<T, F, P, K, Q>pw1 = new PartitionWindow<>();
                pw1.setStart(0);
                pwMap.put(type, pw1);
            }
            // 1st pw.
            for (; i < partitionNum * 2 && i < totalPartitions; i++) {
                Set<CLabel> addedSet = new HashSet<>();
                int pstart = 0, psize =0;
                boolean addThisIndex = true;
                for (ConditionItem ci : typeConditions.values()) {
                    BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                    if (!p.getTop1Map().containsKey(ci.getObjNum())) {
                        addThisIndex = false;
                        break;
                    }
                }
                if (!addThisIndex) continue;
                for (ConditionItem ci : typeConditions.values()) {
                    BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                    PartitionWindow<T, F, P, K, Q> pw1 = pwMap.get(ci.getType());
                    if (p.getTop1Map().containsKey(ci.getObjNum())) {
                        pw1.getPartitions().add(p);
                        addedSet.add(ci.getType());
                        pstart = p.getStartFrame();
                        psize = p.getSize();
                    }
                    pw1.setEnd(p.getStartFrame()+p.getSize() - 1);
                }
                if (addedSet.size() > 0 && addedSet.size() < typeConditions.size()) {
                    BasePartition<P, Q> dummyPartition = new BasePartition<>();
                    dummyPartition.setStartFrame(pstart);
                    dummyPartition.setSize(psize);
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
            pwMap = new HashMap<>();
            // init as the last one.
            for (ConditionItem ci : typeConditions.values()) {
                PartitionWindow<T, F, P, K, Q> pw = lastPwMap.get(ci.getType());
                List<BasePartition<P, Q>> pwnList = new ArrayList<>(pw.getPartitions());
                PartitionWindow<T, F, P, K, Q> newPw = new PartitionWindow<>();
                newPw.setPartitions(pwnList);
                newPw.setStart(partitionNum * partitionSize);
                pwMap.put(ci.getType(), newPw);
            }
            for (; i < totalPartitions; i++) {
                Set<CLabel> addedSet = new HashSet<>();
                int pstart = 0, psize =0;
                boolean addThisIndex = true;
                for (ConditionItem ci : typeConditions.values()) {
                    BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                    if (!p.getTop1Map().containsKey(ci.getObjNum())) {
                        addThisIndex = false; break;
                    }
                }
                if (addThisIndex) {

                    for (ConditionItem ci : typeConditions.values()) {
                        List<BasePartition<P, Q>> pwnList = pwMap.get(ci.getType()).getPartitions();
                        BasePartition<P, Q> p = partitionMap.get(ci.getType()).get(i);
                        if (p.getTop1Map().containsKey(ci.getObjNum())) {
                            pwnList.add(p);
                            addedSet.add(ci.getType());
                            pstart = p.getStartFrame();
                            psize = p.getSize();
                        }
                        pwMap.get(ci.getType()).setEnd(p.getStartFrame() + partitionSize - 1);
                    }

                    if (addedSet.size() > 0 && addedSet.size() < typeConditions.size()) {
                        BasePartition<P, Q> dummyPartition = new BasePartition<>();
                        dummyPartition.setStartFrame(pstart);
                        dummyPartition.setSize(psize);
                        for (ConditionItem ci : typeConditions.values()) {
                            if (!addedSet.contains(ci.getType())) {
                                pwMap.get(ci.getType()).getPartitions().add(dummyPartition);
                            }
                        }
                    }
                }

                if ((i+1) % partitionNum == 0) {
                    // add them.
                    toAddMap = new HashMap<>();
                    // construct the pwMap
                    for (ConditionItem ci : typeConditions.values()) {
                        PartitionWindow<T, F, P, K, Q> newPw = pwMap.get(ci.getType());
                        List<BasePartition<P, Q>> pwnList = pwMap.get(ci.getType()).getPartitions();
                        while(pwnList.size() > 0 && pwnList.get(0).getStartFrame() < newPw.getStart()) {
                            pwnList.remove(0);
                        }
                        if (pwnList.size() > 0) {
                            toAddMap.put(ci.getType(), newPw);
                        }
                    }
                    if (toAddMap.size() > 0) {
                        partitionWindows.add(toAddMap);
                    }
                    lastPwMap = pwMap;
                    pwMap = new HashMap<>();
                    // init.
                    for (ConditionItem ci: typeConditions.values()) {
                        PartitionWindow<T, F, P, K, Q> pw = lastPwMap.get(ci.getType());
                        List<BasePartition<P,Q>> pwnList = new ArrayList<>(pw.getPartitions());
                        PartitionWindow<T, F, P, K, Q> newPw = new PartitionWindow<>();
                        newPw.setPartitions(pwnList);
                        newPw.setStart((i+1-partitionNum) * partitionSize);
                        newPw.setEnd( (i+1 + 2 *partitionNum) * partitionSize -1);
                        pwMap.put(ci.getType(), newPw);
                    }
                }
            }
        }

        // retrieve prefix map for each partition window.
        List<MultiPartitionWindow<T, F, P, K, Q>> multiPartitionWindows = new ArrayList<>();
        for (Map<CLabel, PartitionWindow<T, F, P, K, Q>> pwm: partitionWindows) {
            for (Map.Entry<CLabel,PartitionWindow<T, F, P, K, Q>> entry : pwm.entrySet()) {
                ConditionItem ci = typeConditions.get(entry.getKey());
//                entry.getValue().computeScore(ci.getObjNum(), ci.getLambda(),
//                        entry.getValue().selectCommonObjects(), nodeAssertionFunction, extractor,
//                         keyMapperFunc, earlyStopperFunc);
                entry.getValue().estimateScore(ci.getObjNum());
            }
            MultiPartitionWindow<T, F, P, K, Q> multiPartitionWindow = new MultiPartitionWindow<>(pwm);
            multiPartitionWindow.computeScore(conditions);
            multiPartitionWindows.add(multiPartitionWindow);
        }
        return multiPartitionWindows;
    }
}
