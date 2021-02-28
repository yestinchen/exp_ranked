package com.topk;

import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.bitset.BitsetSingleIndexBuilder;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.processors.bitset.BitsetSingleTypeProcessor2;
import com.topk.online.processors.bitset.measure.Measurement;
import com.topk.online.result.WindowWithScore;
import com.topk.test.utils.Utils;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        // args: file, type, objNum, partitionSize, w, k
        String file = args[0];
        String type = args[1];
        int objNum = Integer.valueOf(args[2]);
        int partitionSize = Integer.valueOf(args[3]);
        int w = Integer.valueOf(args[4]);
        int k = Integer.valueOf(args[5]);
        Integer pw = null;
        if (args.length > 6) {
            pw = Integer.valueOf(args[6]);
        }

        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, Arrays.asList(type));

        System.out.println("frames size:" + frames.size());

        Ticker buildTicker = new Ticker();
        Ticker evalTicker = new Ticker();

        BitsetSingleIndexBuilder indexBuilder = new BitsetSingleIndexBuilder();

        buildTicker.start();
        List<BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>>> partitions =
                indexBuilder.build(frames, partitionSize);
        buildTicker.end();

        System.out.println("# of nodes: " + Utils.countNumberOfRoots(partitions));

        System.out.println("# of candidates: "+ Utils.countNumberOfCandidates(partitions, objNum));

        Measurement.reset();

//        BitsetSingleTypeProcessor processor = new BitsetSingleTypeProcessor(partitions, partitionSize);
        BitsetSingleTypeProcessor2 processor = new BitsetSingleTypeProcessor2(partitions, partitionSize,
//                false);
                true);
        evalTicker.start();
        Collection<WindowWithScore> collection = processor.topk(k, w, objNum, pw);
        evalTicker.end();

        System.out.println("# of processed:" + Measurement.getProcessedCount());
        System.out.println("# of and processed:" + Measurement.getAndCount());
        System.out.println("# of and processed 2:" + Measurement.getAndCount2());

        int count =0;
        Set<Integer> windowMap = new HashSet<>();
        for (WindowWithScore wws : collection) {
            count ++;
            if (windowMap.contains(wws.getWindow().getStart())) {
                System.out.println("duplicate window:" + wws);
            }
            windowMap.add(wws.getWindow().getStart());
            System.out.println("result: "+ wws);
        }

        System.out.println("sorted windows:");
        List<Integer> sortedWindows = new ArrayList<>(windowMap);
        Collections.sort(sortedWindows);
        for (int sw : sortedWindows) {
            System.out.println(sw);
        }
        System.out.println("total size: " + count);
        System.out.println("total k: " +  collection.size());
        System.out.println("build time:" + buildTicker.report());
        System.out.println("eval time: " + evalTicker.report());

    }
}
