package com.topk.online.processors.indexed.composite;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.PartitionWindow;
import com.topk.online.component.PWGenWGroup;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexedCompositeProcessor {

    Logger LOG = LogManager.getLogger(IndexedCompositeProcessor.class);

    {
//        Configurator.setLevel(LOG.getName(), Level.DEBUG);
    }

    List<BasePartition<Node<String, PayloadClassIntervals>, IndexedPartitionPayload>> partitions;
    int partitionSize;

    public IndexedCompositeProcessor(List<BasePartition<Node<String, PayloadClassIntervals>,
            IndexedPartitionPayload>> partitions, int partitionSize) {
        this.partitions = partitions;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, Map<CLabel, Integer> conditionMap, int firstRetrieveLevels) {
        int partitionNum = (int) Math.ceil(w*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize) + 1;
        LOG.debug("max partition num :" + maxPartitionNum);

        int objNum = 0;
        for (Integer v : conditionMap.values()) {
            objNum += v;
        }

        List<PartitionWindow<String, PayloadClassIntervals, Node<String, PayloadClassIntervals>, String,
                IndexedPartitionPayload>> partitionWindows =
                PWGenWGroup.genPWs(partitions, objNum, partitionNum, partitionSize);

        List<CompositeWorkingPW> workingPWs = partitionWindows.stream().map(i ->
                new CompositeWorkingPW(i, maxPartitionNum, partitionSize,
                        partitionNum, w, conditionMap, k, firstRetrieveLevels)).collect(Collectors.toList());

        for (CompositeWorkingPW pw : workingPWs) {
            pw.estimateScore(objNum);
        }
        Collections.sort(partitionWindows, (x1, x2)->  - Integer.compare(x1.getScore(), x2.getScore()));
        if (LOG.isDebugEnabled()) {
            for (PartitionWindow p : partitionWindows) {
                LOG.debug("window: {}, score: {}", p.getStart(), p.getScore());
            }
        }


        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);
        while(workingPWs.get(0).getScore() > topkBookKeeper.getMin() && workingPWs.get(0).getScore() > 0) {
            CompositeWorkingPW pw = workingPWs.get(0);

            LOG.debug("processing partition window {}, score: {}",
                    pw.getPartitionWindow().getStart(), pw.getScore());
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            int stopScore = 0;
            if (workingPWs.size() > 1) {
                stopScore = workingPWs.get(workingPWs.size() -1).getScore()/2;
                if (stopScore < 0) stopScore = 0;
            }
            pw.workUtil(stopScore, topkBookKeeper);
            Collections.sort(workingPWs, (x1, x2)->  - Integer.compare(x1.getScore(), x2.getScore()));
//            if (stopScore == 0) {
//                System.out.println("ok");
//            }
        }
        return topkBookKeeper.getTopkResults();
    }
}
