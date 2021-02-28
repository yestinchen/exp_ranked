package com.topk.online.component;

import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.PartitionWindow;

import java.util.ArrayList;
import java.util.List;

public class PWGenWGroup {
    public static <T, F extends PayloadIntervals, P, K, Q> List<PartitionWindow<T, F, P, K, Q>> genPWs(List<BasePartition<P, Q>> partitions,
                                                                                                       int objNum, int partitionNum, int partitionSize){
        return genPWs(partitions, objNum, partitionNum, partitionSize, null);
    }

    public static <T, F extends PayloadIntervals, P, K, Q> List<PartitionWindow<T, F, P, K, Q>> genPWs(List<BasePartition<P, Q>> partitions,
                                                                 int objNum, int partitionNum, int partitionSize, Integer pw) {

        // construct partition windows.
        List<PartitionWindow<T, F, P, K, Q>> partitionWindows = new ArrayList<>();
        int prunedPartitions = 0;

        if (pw == null) {
//            pw = partitionNum * 2;
            pw = partitionNum+1;
        }

        {
            int i =0;
            PartitionWindow<T, F, P, K, Q> pw1 = new PartitionWindow<>();
            pw1.setStart(0);
            for (; i < pw && i < partitions.size();i ++) {
                BasePartition<P, Q> p = partitions.get(i);
                if (p.getTop1Map().containsKey(objNum)) {
                    pw1.getPartitions().add(p);
                } else {
                    prunedPartitions ++;
                }
                pw1.setEnd(p.getStartFrame() + p.getSize() -1);
            }
            if (pw1.getPartitions().size() > 0) {
                partitionWindows.add(pw1);
            }

            PartitionWindow<T, F, P, K, Q> lastPW = pw1;
            PartitionWindow<T, F, P, K, Q> pwn = new PartitionWindow<>();
            pwn.setPartitions(new ArrayList<>(pw1.getPartitions()));

            int checkPoint = pw - partitionNum;

            pwn.setStart(checkPoint * partitionSize);

            for (; i < partitions.size(); i++) {
                // generate all the rest.
                BasePartition<P, Q> p = partitions.get(i);
                if (p.getTop1Map().containsKey(objNum)) {
                    pwn.getPartitions().add(p);
                } else {
                    prunedPartitions++;
                }
                pwn.setEnd(p.getStartFrame() + p.getSize() - 1);

                if ((i + 1) % checkPoint == 0) {
                    List<BasePartition<P, Q>> pwnList = pwn.getPartitions();
                    while(pwnList.size() > 0 && pwnList.get(0).getStartFrame() < pwn.getStart()) {
                        pwnList.remove(0);
                    }

                    if (pwnList.size() > 0) {
                        partitionWindows.add(pwn);
                    }

                    lastPW = pwn;
                    pwn = new PartitionWindow<>();
                    pwn.setStart((i+1-partitionNum) * partitionSize);
                    pwn.setEnd((i+1+2*partitionNum) * partitionSize - 1);
                    pwn.setPartitions(new ArrayList<>(lastPW.getPartitions()));
                }
            }
            // add missing one.
            if (i% checkPoint != 0) {
                List<BasePartition<P, Q>> pwnList = pwn.getPartitions();
                while(pwnList.size() > 0 && pwnList.get(0).getStartFrame() < pwn.getStart()) {
                    pwnList.remove(0);
                }

                if (pwnList.size() > 0) {
                    partitionWindows.add(pwn);
                }
            }
        }
        // last one.

//        System.out.println("pruned partitions:" + prunedPartitions);
        return partitionWindows;
    }
}
