package com.topk.test.utils;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.builder.partition.BitsetPartitionPayload;

import java.util.List;

public class Utils {

    public static <T, F> int countNumberOfRoots(List<BasePartition<T, F>> partitions) {
        int count = 0;
        for (BasePartition<T, F> p : partitions) {
            count += p.getRoots().size();
        }
        return count;
    }

    public static int countNumberOfCandidates(
            List<BasePartition<Node<String, PayloadWMaskDewey>,
                    BitsetPartitionPayload<PayloadWMaskDewey>>> partitions, int threshold) {
        int count =0;
        for (BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>> p : partitions) {
            for (Node<String, PayloadWMaskDewey> node : p.getRoots()) {
                if (node.getPayload().getMask().cardinality() >= threshold) {
                    count ++;
                }
            }
        }
        return count;
    }
}
