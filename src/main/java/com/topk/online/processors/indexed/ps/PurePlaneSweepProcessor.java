package com.topk.online.processors.indexed.ps;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.PWGenWGroup;
import com.topk.online.component.WindowComputer;
import com.topk.online.processors.indexed.utils.IndexedUtils;
import com.topk.online.ps.PSAlgorithm;
import com.topk.online.ps.PurePSAlgorithm;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class PurePlaneSweepProcessor {

    private static Logger LOG = LogManager.getLogger(PurePlaneSweepProcessor.class);

    List<BasePartition<Node<String, PayloadIntervals>,
            IndexedPartitionPayload>> partitions;
    int partitionSize;

    public PurePlaneSweepProcessor(List<BasePartition<Node<String,
            PayloadIntervals>, IndexedPartitionPayload>> partitions, int partitionSize) {
        this.partitions = partitions;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, int objNum) {
        //
        int partitionNum = (int) Math.ceil(w*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize) + 1;
        LOG.debug("max partition num :" + maxPartitionNum);

        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);

        List<PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload>> partitionWindows =
                PWGenWGroup.genPWs(partitions, objNum, partitionNum, partitionSize);
        // setup working partitions.

        for (PartitionWindow<String, PayloadIntervals, Node<String, PayloadIntervals>, String,
                IndexedPartitionPayload> pw: partitionWindows) {
            // plane sweep each partition.
            Map<Set<String>, List<Interval>> intervalMap = new HashMap<>();
            for (BasePartition<Node<String, PayloadIntervals>, IndexedPartitionPayload> p : pw.getPartitions()) {
                // run plane-sweep.
                PurePSAlgorithm psAlgorithm = new PurePSAlgorithm();
                Map<Set<String>, List<Interval>> rMap = psAlgorithm.planeSweep(
                        p.getPayload().getIntervalMap(), objNum);
                for (Map.Entry<Set<String>, List<Interval>> entry: rMap.entrySet()) {
                    intervalMap.computeIfAbsent(entry.getKey(), x -> new ArrayList<>()).addAll(entry.getValue());
                }
            }

            // merge intervals.
            for (Set<String> key: intervalMap.keySet()) {
                intervalMap.put(key, IntervalUtils.uniqueIntervals(intervalMap.get(key)));
            }
            // FIXME: not correct.

            // get result.
            // generate windows.
            int startWindow = WindowComputer.computeStartWindow(pw.getStart(), partitionSize, partitionNum, w);
            int baseIdx = pw.getStart() + startWindow;
            // upate
            int[] windowScoreArr = IndexedUtils.initWindowScoreArr(pw, w, startWindow);
            IndexedUtils.updateWindowAccordingToIntervalList(intervalMap, 0,
                    windowScoreArr, baseIdx, w);

            IndexedUtils.updateTopk(topkBookKeeper, windowScoreArr, baseIdx, w);

        }
        return topkBookKeeper.getTopkResults();
    }
}
