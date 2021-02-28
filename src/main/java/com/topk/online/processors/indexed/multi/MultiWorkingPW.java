package com.topk.online.processors.indexed.multi;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.indexed.IndexedWorkingPartition2;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.result.TopkBookKeeperBreakTie;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class MultiWorkingPW {

    private static final Logger LOG = LogManager.getLogger(MultiWorkingPW.class);

    MultiPartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload> partitionWindow;


    int score = 0;
    int remainingPossibleMax;
    int baseIdx;
    int[] windowScoreArr;
    int windowMinScore = 0;
    // put all computed sets.
    Set<Set<String>> computedSet = new HashSet<>();
    int objNum;
    int w;
    int maxPartitionNum;
    int k;
    boolean firstVisit = true;
    int firstRetrieveLevels = 0;

    List<Set<String>> commonObjMap;
    List<List<IndexedWorkingPartition2<PayloadIntervals>>> indexedWorkingPartitionsList;

    public MultiWorkingPW(
            MultiPartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                    IndexedPartitionPayload> pw,
            int maxPartitionNum, int partitionSize, int partitionNum, int w,
            int objNum, int k, int firstRetrieveLevels) {

        this.partitionWindow = pw;
        this.objNum = objNum;
        this.w = w;
        this.maxPartitionNum = maxPartitionNum;
        this.k = k;
        this.firstRetrieveLevels = firstRetrieveLevels;

        // get all retrieved keys.
        this.commonObjMap = new ArrayList<>();
        this.indexedWorkingPartitionsList = new ArrayList<>();

        for (Map.Entry<CLabel, PartitionWindow<String, PayloadIntervals,
                Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload>> entry: pw.getPwMap().entrySet()) {
            PartitionWindow<String, PayloadIntervals,
                    Node<String, PayloadIntervals>, String,
                    IndexedPartitionPayload> partitionWindow = entry.getValue();
            Set<String> thisCommonObjs = partitionWindow.selectCommonObjects();
            commonObjMap.add(thisCommonObjs);
            indexedWorkingPartitionsList.add(partitionWindow.getPartitions().stream().map(i -> new IndexedWorkingPartition2<>(i,
                            thisCommonObjs, objNum)).collect(Collectors.toList()));
        }

////        // 1. obtain all object sets with the same
//        this.wps = pw.getPartitions().stream().map(
//                i ->new IndexedWorkingPartition2<>(i, commonObjs, objNum)).collect(Collectors.toList());
//
        this.remainingPossibleMax = computePossibleMax();
//
        // get any one partition.
        PartitionWindow<String, PayloadIntervals,
                Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload> pw1=
                pw.getPwMap().values().iterator().next();
        int startWindow = WindowComputer.computeStartWindow(pw1.getStart(), partitionSize, partitionNum, w);
        this.baseIdx = pw1.getStart() + startWindow;
        this.windowScoreArr = IndexedUtils.initWindowScoreArr(pw1, w, startWindow);
    }

    <T extends PayloadIntervals> List<Integer> computeMaxes(List<IndexedWorkingPartition2<T>> wps) {
        Map<Integer, IndexedWorkingPartition2<T>> pwMap = new HashMap<>();
        for (IndexedWorkingPartition2<T> wp: wps) {
            pwMap.put(wp.getBasePartition().getStartFrame(), wp);
        }
        List<Map.Entry<Integer, IndexedWorkingPartition2<T>>> sorted = new ArrayList<>(pwMap.entrySet());
        Collections.sort(sorted, Comparator.comparingInt(Map.Entry::getKey));
        // map to remaining count.
        int[] countList = sorted.stream().mapToInt(i -> i.getValue().getRemainingCount()).toArray();
        List<Integer> values = new ArrayList<>();
//        LOG.debug("remaining possible , count list: {}", Arrays.toString(countList));
        int sum = 0;
        for (int i=0; i < sorted.size(); i++) {
            sum += countList[i];
            if (i + 1 >= maxPartitionNum) {
                values.add(sum);
                sum -= countList[i - maxPartitionNum + 1];
            }
        }
        return values;
    }

    int computePossibleMax() {
        List<Integer> list = new ArrayList<>();
        for (List<IndexedWorkingPartition2<PayloadIntervals>> wp : indexedWorkingPartitionsList) {
            List<Integer> maxList = computeMaxes(wp);
            for (int i =0; i < maxList.size(); i++) {
                if (list.size() <= i) list.add(maxList.get(i));
                else if (maxList.get(i) > list.get(i)){
                    // use the max one.
                    list.add(maxList.get(i));
                }
            }
        }
        return Collections.max(list);
    }

    public void workUtil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
        LOG.debug("stopScore: {}, remaining max: {}", stopScore, remainingPossibleMax);

        while (this.remainingPossibleMax >= stopScore && this.remainingPossibleMax >0
        && this.remainingPossibleMax > topkBookKeeper.getMin()) {
            // sort ?
//            Collections.sort();
        }
    }
}
