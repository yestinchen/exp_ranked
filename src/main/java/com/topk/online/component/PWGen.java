package com.topk.online.component;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.PayloadCount;
import com.topk.online.PartitionWindow;

import java.util.ArrayList;
import java.util.List;

public class PWGen {

    public static <T, F extends PayloadCount, P, K, Q> List<PartitionWindow<T, F, P, K, Q>> genPWs(List<BasePartition<P, Q>> partitions,
                                                                           int objNum, int partitionNum, int partitionSize) {

        List<PartitionWindow<T, F, P, K, Q>> partitionWindows = new ArrayList<>();
        {
            int i = 0;
            // 1. first window.
            PartitionWindow<T, F, P, K, Q> pw1 = new PartitionWindow<>();
            pw1.setStart(0);
            for (; i < partitionNum && i < partitions.size(); i++) {
                BasePartition<P, Q> p = partitions.get(i);
                if (p.getTop1Map().containsKey(objNum)) {
                    pw1.getPartitions().add(p);
                }
                pw1.setEnd(p.getStartFrame() + p.getSize() - 1);
            }
            if (pw1.getPartitions().size() > 0) {
                partitionWindows.add(pw1);
            }
            // add the rest.
            PartitionWindow<T, F, P, K, Q> lastPw = pw1;
            for (; i < partitions.size(); i++) {
                PartitionWindow<T, F, P, K, Q> pwn = new PartitionWindow<>();
                // remove the first partition from the last pw.
                List<BasePartition<P, Q>> pwnList = new ArrayList<>(lastPw.getPartitions());
                if (pwnList.size() > 0 && lastPw.getStart() == pwnList.get(0).getStartFrame()) {
                    // remove first.
                    pwnList.remove(0);
                }
                BasePartition<P, Q> p = partitions.get(i);
                if (p.getTop1Map().containsKey(objNum)) {
                    pwnList.add(p);
//                    partitionWindows.add(pwn);
                }
                pwn.setPartitions(pwnList);
                pwn.setStart((i-partitionNum+1) * partitionSize);
                pwn.setEnd(p.getStartFrame() + p.getSize() - 1);
                if (pwnList.size() > 0) {
                    partitionWindows.add(pwn);
                }
                lastPw = pwn;
            }
        }
        return partitionWindows;
    }
}
