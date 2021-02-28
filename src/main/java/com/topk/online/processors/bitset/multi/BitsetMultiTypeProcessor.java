package com.topk.online.processors.bitset.multi;

import com.topk.offline.bean.*;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.MultiPartitionWindow;
import com.topk.online.component.MPWGenWGroup;
import com.topk.online.component.MPWGenWGroupAndOnly;
import com.topk.online.processors.ConditionItem;
import com.topk.online.processors.bitset.WorkingPW;
import com.topk.online.result.TopkBookKeeper;
import com.topk.online.result.TopkBookKeeperBreakTie;
import com.topk.online.result.WindowWithScore;
import com.topk.online.retriever.EarlyStopperNever;
import com.topk.online.retriever.KeyMapperDummy;
import com.topk.online.retriever.NodeAssertionStr;
import com.topk.online.retriever.SimpleRootNodeExtractor;

import java.util.*;
import java.util.stream.Collectors;

public class BitsetMultiTypeProcessor {

    Map<CLabel, List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>>> partitionMap;

    int partitionSize;

    public BitsetMultiTypeProcessor(Map<CLabel, List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>>> partitionMap, int partitionSize) {
        this.partitionMap = partitionMap;
        this.partitionSize = partitionSize;
    }

    public Collection<WindowWithScore> topk(int k, int w, List<List<ConditionItem>> conditions) {

        int partitionNum = (int) Math.ceil((w)*1.0/partitionSize);
        int maxPartitionNum = (int) Math.ceil((w-1)*1.0/partitionSize) + 1;


        Map<CLabel, Integer> objNumMap = new HashMap<>();
        for (List<ConditionItem> conditionItems: conditions) {
            ConditionItem conditionItem = conditionItems.get(0);
            objNumMap.put(conditionItem.getType(), conditionItem.getObjNum());
        }


        List<MultiPartitionWindow<String, PayloadWMaskDewey, Node<String, PayloadWMaskDewey>, String,
                BitsetPartitionPayload<PayloadWMaskDewey>>> multiPartitionWindows =
                MPWGenWGroupAndOnly.genMPWs(partitionMap, conditions, partitionNum, partitionSize,
                        (selectedObjs, x)-> new NodeAssertionStr(selectedObjs), new SimpleRootNodeExtractor<>(),
                        (s, x) ->new KeyMapperDummy<>(), (s, x)-> new EarlyStopperNever<>());

        List<WorkingMPW> workingPWS = multiPartitionWindows.stream().map(i -> new WorkingMPW(i, maxPartitionNum, partitionSize,
                partitionNum, w, objNumMap, k)).collect(Collectors.toList());

        Collections.sort(workingPWS, (x1, x2) -> x2.remainingMax - x1.remainingMax);

        TopkBookKeeperBreakTie topkBookKeeper = new TopkBookKeeperBreakTie(k);

        while(workingPWS.get(0).remainingMax > topkBookKeeper.getMin()
                && workingPWS.get(0).remainingMax > 0
                && topkBookKeeper.getMin() < w) {
            // work
            int lastScore = workingPWS.get(0).getRemainingMax() - 1;
            if (workingPWS.size() > 1) {
                lastScore = workingPWS.get(1).getRemainingMax();
            }
            WorkingMPW workingMPW = workingPWS.get(0);
//            if (workingMPW.startFrame == 3000) {
//                System.out.println("gotya 1");
//            }
            workingMPW.workUntil(lastScore - 1, topkBookKeeper);
//            if (workingMPW.startFrame == 3000) {
//                System.out.println("gotya 2");
//            }

            Collections.sort(workingPWS, (x1, x2) -> x2.remainingMax - x1.remainingMax);

        }
//        for (WorkingMPW workingMPW: workingPWS) {
//            System.out.println("["+workingMPW.workingPWMap.get(CLabel.CAR).getPw().getStart() +
//                    "], remaining:" + workingMPW.getRemainingMax());
//        }

        return topkBookKeeper.getTopkResults();
    }
}
