package com.topk.online.processors.bitset;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.processors.indexed.utils.BasePartitionHolder;

import java.util.*;
import java.util.stream.Collectors;

public class WorkingPartition2 implements BasePartitionHolder<Node<String, PayloadWMaskDewey>,
        BitsetPartitionPayload<PayloadWMaskDewey>> {

    int currentPos = 0;

    List<Node<String, PayloadWMaskDewey>> allNodes;

    int remainingCount = 0;

    BitsetPartitionPayload<PayloadWMaskDewey> indexMap;

    BitSet commonMask;

    int[] mapping;

    int currentId = -1;

    BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>> basePartition;

    public WorkingPartition2(BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>> basePartition) {
        this.allNodes = basePartition.getRoots();
        this.currentPos = 0;
        if (allNodes == null || allNodes.size() == 0) {
            this.remainingCount = 0;
        } else {
            this.remainingCount = allNodes.get(0).getPayload().getCount();
        }
        this.indexMap = basePartition.getPayload();
        this.basePartition = basePartition;
    }

    public Node<String, PayloadWMaskDewey> nextNode(int objNum) {
        if (remainingCount == 0) return null;
//        int i = 0;
        do {
            moveToNext();
//            System.out.println("sss:"+currentPos+";"+allNodes.size());
//            i++;
        } while(currentPos < allNodes.size() &&
                allNodes.get(currentPos).getPayload().getMask().cardinality() < objNum);

        if (currentPos < allNodes.size()) {
            Node<String, PayloadWMaskDewey> node = allNodes.get(currentPos);
            return node;
        }
        return null;
    }

    public List<Node<String, PayloadWMaskDewey>> getNodesWithKey(String key) {
//        return indexMap.getMap().get(key);
        List<Node<String, PayloadWMaskDewey>> index = indexMap.getMap().get(key);
        if (index == null) return null;
        List<Node<String, PayloadWMaskDewey>> filtered = index.stream()
                .filter(i -> i.getId() >= currentId).collect(Collectors.toList());
        return filtered;
//        if (filtered.size() > 0)
//            return Collections.singletonList(filtered.get(0));
//        else return null;
    }

    public int getRemainingCount() {
        return remainingCount;
    }

    public void setRemainingCount(int remainingCount) {
        this.remainingCount = remainingCount;
    }

    public BasePartition<Node<String, PayloadWMaskDewey>, BitsetPartitionPayload<PayloadWMaskDewey>> getBasePartition() {
        return basePartition;
    }

    public BitSet getCommonMask() {
        return commonMask;
    }

    public void setCommonMask(BitSet commonMask) {
        this.commonMask = commonMask;
    }

    public int[] getMapping() {
        return mapping;
    }

    public void setMapping(int[] mapping) {
        this.mapping = mapping;
    }

    void moveToNext() {
        currentPos++;
        if (remainingCount == 0) return;
        if (currentPos +1 < allNodes.size()) {
            remainingCount = allNodes.get(currentPos + 1).getPayload().getCount();
            currentId = allNodes.get(currentPos+ 1).getId();
        } else {
            remainingCount = 0;
            currentId = Integer.MAX_VALUE;
        }
    }

}
