package com.topk.online.processors.bitset.multi;

import com.topk.bean.Interval;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.PartitionWindow;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.bitset.IntervalListWCount;
import com.topk.online.processors.bitset.WorkingPW;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.ps.PSAlgorithm;
import com.topk.online.result.TopkBookKeeper;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.util.*;

public class WorkingMPW {

    private Logger LOG = LogManager.getLogger(WorkingMPW.class);

    {
        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    Map<CLabel, WorkingSPW> workingPWMap;

    Map<CLabel, Map<Set<String>, List<Interval>>> labelIntervalMap;

    int remainingMax;

    PSAlgorithm algorithm;

    int[] windowScoreArr;
    int startWindow;
    int baseIdx;

    int partitionSize;
    int partitionNum;
    int w;
    int minScore;
    int k;
    int startFrame;

    MultiPartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
            BitsetPartitionPayload<PayloadWMaskDewey>> multiPartitionWindow;

    public WorkingMPW(MultiPartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
            BitsetPartitionPayload<PayloadWMaskDewey>> multiPartitionWindow,
                      int maxPartitionNum, int partitionSize, int partitionNum, int w, Map<CLabel, Integer> objNumMap, int k) {
        Map<CLabel, PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>>> pwMap = multiPartitionWindow.getPwMap();

        this.multiPartitionWindow = multiPartitionWindow;

        workingPWMap = new HashMap<>();
        
        PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>> pw = null;
        for (CLabel label: pwMap.keySet()) {
            WorkingSPW spw = new WorkingSPW(pwMap.get(label), maxPartitionNum, objNumMap.get(label));
            workingPWMap.put(label, spw);
            pw = spw.getPw();
            startFrame = spw.getPw().getStart();
        }
        // remaing max
        this.remainingMax = computeRemainingMax();
        this.algorithm = new PSAlgorithm();

        this.partitionNum = partitionNum;
        this.partitionSize = partitionSize;
        this.w = w;
        this.startWindow = WindowComputer.computeStartWindow(pw.getStart(),
                partitionSize, partitionNum, w);
        this.baseIdx = pw.getStart() + startWindow;
        windowScoreArr = IndexedUtils.initWindowScoreArr(pw, w, startWindow);
        this.minScore = 0;
        this.k = k;
        this.labelIntervalMap = new HashMap<>();
    }

    public void workUntil(int stopScore, TopkBookKeeperBreakTie topkBookKeeper) {
        // process by type.
        for (CLabel label : workingPWMap.keySet()) {
            Map<Set<String>, List<Interval>> singleLabelMap = new HashMap<>();
            while(workingPWMap.get(label).remainingMax >= stopScore) {
                LOG.debug("topk result: [{}]", topkBookKeeper.getMin());
                LOG.debug("[{}], remaining: [{}], stop score: [{}]", label, workingPWMap.get(label).remainingMax, stopScore);
                WorkingSPW currentSPW = workingPWMap.get(label);
                Map<Set<String>, List<Interval>> intervalMap = currentSPW.next();
                if (intervalMap == null) break;
                if (intervalMap.size() > 0) {
                    for (Map.Entry<Set<String>, List<Interval>> entry: intervalMap.entrySet()) {
                        singleLabelMap.putIfAbsent(entry.getKey(), entry.getValue());
                    }
                }
            }
            Set<CLabel> currentLabels = new HashSet<>(labelIntervalMap.keySet());
            currentLabels.add(label);
            if (currentLabels.size() == workingPWMap.size()) {
                LOG.debug("processing [{}] : [{}]", label, singleLabelMap.size());
                List<Map<Set<String>, List<Interval>>> list = new ArrayList<>();
                list.add(singleLabelMap);
                for (CLabel anotherLabel: labelIntervalMap.keySet()) {
                    if (anotherLabel != label) list.add(labelIntervalMap.get(anotherLabel));
                }
                // plane sweep.
                Map<Set<String>, List<Interval>> result =
                        algorithm.planeSweepOnePartition(list);
                // update intervals.

                IndexedUtils.updateWindowAccordingToIntervalList(result, minScore,
                        windowScoreArr, baseIdx, w);
                minScore = IndexedUtils.getKthFromArray(windowScoreArr, k);
            }
//            if (startFrame == 77400) {
//                System.out.println("we'll see");
//            }
            // FIXME: do we need to check duplication?
            labelIntervalMap.computeIfAbsent(label, x -> new HashMap<>());
            for (Map.Entry<Set<String>, List<Interval>> entry : singleLabelMap.entrySet()) {
//                List<Interval> existingOne = labelIntervalMap.get(label).get(entry.getKey());
//                if (existingOne == null || IntervalUtils.count(existingOne) < IntervalUtils.count(entry.getValue())) {
//                    labelIntervalMap.get(label).put(entry.getKey(), entry.getValue());
//                }
                labelIntervalMap.get(label).putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        this.remainingMax = computeRemainingMax();

        // update.
        IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);
    }

    public int getRemainingMax() {
        return remainingMax;
    }

    public int computeRemainingMax() {
        int max = 0;
        for (CLabel label: workingPWMap.keySet()) {
            max = Math.max(max, workingPWMap.get(label).getRemainingMax());
        }
        return max;
    }

}
