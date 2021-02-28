package com.topk.online.ps;

import com.interval.bean.SETuple;
import com.topk.bean.Interval;
import com.topk.utils.Combinations;
import com.topk.utils.Combinations2;

import java.util.*;

public class PurePSAlgorithm {


    public Map<Set<String>, List<Interval>> planeSweep(Map<String, List<Interval>> objMap, int objNum) {
        // 1. convert to tuples.
        List<TupleSE> tuples = new ArrayList<>();
        int condId =0;
        for (Map.Entry<String, List<Interval>> entry : objMap.entrySet()) {
            for (Interval inter: entry.getValue()) {
                tuples.add(new TupleSE(condId, inter.getStart(), TupleSE.SEType.Start, entry.getKey()));
                tuples.add(new TupleSE(condId, inter.getEnd(), TupleSE.SEType.End, entry.getKey()));
            }
        }

        // 2. sort tuples.
        Collections.sort(tuples, (x1, x2) -> {
            int c = Integer.compare(x1.value, x2.value);
            if (c != 0) return c;
            if (x1.type == x2.type) return 0;
            if (x1.type == TupleSE.SEType.Start) return -1;
            return 1;
        });

        // 3. scan.
        Map<String, Integer> bufferMapList = new HashMap<>();

        Map<Set<String>, List<Interval>> objInterMap = new HashMap<>();

        for (TupleSE tuple : tuples) {
            switch (tuple.type) {
                case Start:
                    // put
                    bufferMapList.put(tuple.objs, tuple.value);
                    break;
                case End:
                    // generate.
                    int startV = bufferMapList.remove(tuple.objs);
                    // try to generate.
                    if (bufferMapList.size() >= objNum - 1) {
                        List<List<String>> allCombs = Combinations.combinations(
                                new ArrayList<>(bufferMapList.keySet()), objNum - 1);
                        for (List<String> comb: allCombs) {
                            Set<String> newSet = new HashSet<>();
                            newSet.addAll(comb);
                            newSet.add(tuple.getObjs());
                            List<Interval> l = objInterMap.computeIfAbsent(newSet, x -> new ArrayList<>());
                            l.add(new Interval(startV, tuple.getValue()));
                        }
                    }
                    break;
            }
        }
        return objInterMap;
    }

    static class TupleSE {
        int rId;
        int value;
        SEType type;
        String objs;

        public TupleSE(int rId, int value, SEType type, String objs) {
            this.rId = rId;
            this.value = value;
            this.type = type;
            this.objs = objs;
        }

        public String getObjs() {
            return objs;
        }

        public void setObjs(String objs) {
            this.objs = objs;
        }

        public int getrId() {
            return rId;
        }

        public void setrId(int rId) {
            this.rId = rId;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public SEType getType() {
            return type;
        }

        public void setType(SEType type) {
            this.type = type;
        }

        enum SEType{
            Start, End;
        }
    }

}

