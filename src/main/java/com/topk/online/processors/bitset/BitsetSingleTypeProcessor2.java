package com.topk.online.processors.bitset;

import com.topk.bean.Interval;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.PWGenWGroup;
import com.topk.online.component.WindowComputer;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import com.topk.utils.BitSetUtils;
import com.topk.utils.IntervalUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

public class BitsetSingleTypeProcessor2 {

    private static Logger LOG = LogManager.getLogger(BitsetSingleTypeProcessor2.class);
    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>> partitions;
    int partitionSize;
    boolean optimize;


    public BitsetSingleTypeProcessor2(List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>> partitions,
                                      int partitionSize, boolean optimize) {
        this.partitions = partitions;
        this.partitionSize = partitionSize;
        this.optimize = optimize;
    }

    public Collection<WindowWithScore> topk(int k, int w, int objNum, Integer pw) {
        //
        int partitionNum = (int) Math.ceil(w*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize) + 1;

        List<PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>>> partitionWindows =
                PWGenWGroup.genPWs(partitions, objNum, partitionNum, partitionSize, pw);
//        for (PartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
//                BitsetPartitionPayload<PayloadWMaskDewey>> pw : partitionWindows) {
//            pw.estimateScore(objNum);
//        }
//        Collections.sort(partitionWindows, (x1, x2)->  - Integer.compare(x1.getScore(), x2.getScore()));

        List<WorkingPW> workingPWS = partitionWindows.stream().map(i -> new WorkingPW(i, maxPartitionNum, partitionSize,
                partitionNum, w, objNum, optimize)).collect(Collectors.toList());

        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);

        Collections.sort(workingPWS, (x1, x2) -> x2.remainingMax - x1.remainingMax);

        while(workingPWS.get(0).remainingMax > topkBookKeeper.getMin()
                && workingPWS.get(0).remainingMax > 0 &&
                topkBookKeeper.getMin() < w) {
            // work
//            int lastScore = workingPWS.get(workingPWS.size() -1).getRemainingMax();
            int lastScore = workingPWS.get(0).getRemainingMax() - 1;
            if (workingPWS.size() > 1) {
                lastScore = workingPWS.get(1).getRemainingMax();
            }
            workingPWS.get(0).workUntil(lastScore - 1, topkBookKeeper);

            if (workingPWS.size() > 1) {
                Collections.sort(workingPWS, (x1, x2) -> x2.remainingMax - x1.remainingMax);
            }
        }

        // stop point.
//        for (WorkingPW pw: workingPWS) {
//            System.out.println("pw: " +pw.getPw().getStart()+"; remaining:"+pw.getRemainingMax());
//            pw.reportRemaining();
//        }
        return topkBookKeeper.getTopkResults();
    }
}
