package com.topk.online.ps;

import com.interval.bean.SETuple;
import com.topk.bean.Interval;
import com.topk.online.PrefixList;
import com.topk.online.interval.PrefixListWithInterval;
import com.topk.utils.Combinations;
import com.topk.utils.Combinations2;

import java.util.*;

public class PSAlgorithm {

    public Map<Set<String>, List<Interval>> planeSweepOnePartition(List<Map<Set<String>, List<Interval>>> objMaps) {
        // 1. convert to tuples.
        List<PSTuple> tuples = new ArrayList<>();
        int condId =0;
        for (Map<Set<String>, List<Interval>> objMap: objMaps) {
            for (Map.Entry<Set<String>, List<Interval>> entry : objMap.entrySet()) {
                for (Interval inter: entry.getValue()) {
                    tuples.add(new PSTuple(condId, inter.getStart(), PSTuple.PSTType.Start, entry.getKey()));
                    tuples.add(new PSTuple(condId, inter.getEnd(), PSTuple.PSTType.End, entry.getKey()));
                }
            }
            condId ++;
        }

        // 2. sort tuples.
        Collections.sort(tuples, (x1,x2) -> {
            int c = Integer.compare(x1.value, x2.value);
            if (c != 0) return c;
            if (x1.type == x2.type) return 0;
            if (x1.type == PSTuple.PSTType.Start) return -1;
            return 1;
        });

        // 3. scan.
        List<Map<Set<String>, Integer>> bufferMapList = new ArrayList<>();
        // init all conds.
        for (int i =0; i < condId; i++) {
            bufferMapList.add(new HashMap<>());
        }

        Map<Set<String>, List<Interval>> objInterMap = new HashMap<>();

        for (PSTuple tuple : tuples) {
            switch (tuple.type) {
                case Start:
                    // put
                    bufferMapList.get(tuple.rId).put(tuple.objs, tuple.value);
                    break;
                case End:
                    // generate.
                    boolean canGenerate = true;
                    int removedStart = -1;
                    List<Map<Set<String>, Integer>> otherMaps = new ArrayList<>();
                    for (int i =0; i < bufferMapList.size(); i++) {
                        if (i == tuple.rId) {
                            // remove
                            removedStart = bufferMapList.get(tuple.rId).remove(tuple.objs);
                        } else {
                            otherMaps.add(bufferMapList.get(i));
                            if (bufferMapList.get(i).size() == 0) {
                                canGenerate = false;
                            }
                        }
                    }
                    if (canGenerate) {
                        if (objMaps.size() == 1) {
                            // generate with itself.
                            List<Interval> l = objInterMap.computeIfAbsent(tuple.getObjs(), x -> new ArrayList<>());
                            l.add(new Interval(removedStart, tuple.getValue()));
                        } else {
                            // enumerate possible combinations between all the rest relations.
                            // choose one from
                            Map<Set<String>, Integer> genMap = Combinations2.combinations(otherMaps, removedStart);
                            // put all.
                            for (Map.Entry<Set<String>, Integer> entry : genMap.entrySet()) {
                                Set<String> newSet = new HashSet<>();
                                newSet.addAll(entry.getKey());
                                newSet.addAll(tuple.getObjs());
                                List<Interval> l = objInterMap.computeIfAbsent(newSet, x -> new ArrayList<>());
                                l.add(new Interval(entry.getValue(), tuple.getValue()));
                            }
                        }
                    }
                    break;
            }
        }
        return objInterMap;
    }

    public List<List<Map<Set<String>, List<Interval>>>> arrangeByPartition(List<List<Map<Set<String>, PrefixList<String>>>> prefixMapList) {
        // handle partition by partition.
        List<List<Map<Set<String>, List<Interval>>>> partitionList = new ArrayList<>();
        for (int cond = 0; cond < prefixMapList.size(); cond ++) {
            for (int partition=0; partition < prefixMapList.get(cond).size(); partition ++) {
                if (partition >= partitionList.size()) {
                    partitionList.add(new ArrayList<>());
                }
                Map<Set<String>, List<Interval>> currentMap = new HashMap<>();

                // generate all obj sets.
                for (Map.Entry<Set<String>, PrefixList<String>> entry: prefixMapList.get(cond).get(partition).entrySet()) {
                    PrefixListWithInterval<String> plwi = (PrefixListWithInterval<String>) entry.getValue();
                    for (Map.Entry<Set<String>, List<Interval>> e : plwi.getIntervalMap().entrySet()) {
                        Set<String> objSet = new HashSet<>();
                        objSet.addAll(entry.getKey());
                        objSet.addAll(e.getKey());

                        currentMap.put(objSet, e.getValue());
                    }
                }
                if (currentMap.size() > 0) {
                    partitionList.get(partition).add(currentMap);
                }
            }
        }
        return partitionList;
    }

    public Map<Set<String>, List<Interval>> planeSweep(List<List<Map<Set<String>, PrefixList<String>>>> prefixMapList, int conditionNum) {

        List<List<Map<Set<String>, List<Interval>>>> partitionList = arrangeByPartition(prefixMapList);

        Map<Set<String>, List<Interval>> result = new HashMap<>();
        // for each partition, plane sweep.
        for (List<Map<Set<String>, List<Interval>>> objMaps : partitionList) {
            if (objMaps.size() == conditionNum) {
                // process only when the # of conditions is complete.
                Map<Set<String>, List<Interval>> r1 = planeSweepOnePartition(objMaps);
                for (Map.Entry<Set<String>, List<Interval>> entry : r1.entrySet()) {
                    result.computeIfAbsent(entry.getKey(), x -> new ArrayList<>()).addAll(entry.getValue());
                }
            }
        }

        return result;
    }


    public static void testPlaneSweepOnePartition() {
        PSAlgorithm algorithm = new PSAlgorithm();
        Map<Set<String>, List<Interval>> map1 = new HashMap<>();
        map1.put(new HashSet<>(Arrays.asList("set1")), Arrays.asList(
                new Interval(1,3), new Interval(4,8)
        ));
        Map<Set<String>, List<Interval>> map2 = new HashMap<>();
        map2.put(new HashSet<>(Arrays.asList("SET1")), Arrays.asList(
                new Interval(1,2), new Interval(4,8)
        ));
        Map<Set<String>, List<Interval>> map3 = new HashMap<>();
        map3.put(new HashSet<>(Arrays.asList("Set3")), Arrays.asList(
                new Interval(1,4), new Interval(5,8)
        ));

        List<Map<Set<String>, List<Interval>>> firstOne = Arrays.asList(
                map1
//                , map2, map3
        );
        Map<Set<String>, List<Interval>> result= algorithm.planeSweepOnePartition(firstOne);
        for (Set<String> key: result.keySet()) {
            System.out.println(key+":"+result.get(key));
        }

    }

    public static void testArrangePartition() {

    }

    public static void main(String[] args) {
        testPlaneSweepOnePartition();
    }
}
