package com.topk.test;

import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.builder.indexed.IntervalIndexedSingleBuilder;
import com.topk.offline.builder.partition.IndexedPartitionPayload;
import com.topk.online.processors.indexed.IndexedSingleTypeProcessor2;
import com.topk.online.processors.indexed.ps.PurePlaneSweepProcessor;
import com.topk.online.result.WindowWithScore;

import java.util.*;

public class TestPurePlaneSweep {

    public static void main(String[] args) {

//        String file = "data/new/MVI_40751.txt";
        String file = "data/new/MOT16-06.txt";
//        String file = "data/new/MOT16-13.txt";
//        String file = "data/new/MVI_40171.txt";
        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, Arrays.asList("person"));
//        List<Set<String>> frames = SimpleReader.read("data/topk-test/t3.txt");

        int objNum = 4;
        int partitionSize = 100;
//        int k = 1;
        int k = 1;
        int w = 300;
        int limit = 0;

        if (limit > 0) {
            frames = frames.subList(0, limit);
        }

        if (k > frames.size()) {
            k = frames.size();
        }

        System.out.println("frames size:" + frames.size());

        Ticker buildTicker = new Ticker();
        Ticker evalTicker = new Ticker();

        IntervalIndexedSingleBuilder indexBuilder = new IntervalIndexedSingleBuilder();

        buildTicker.start();
        List<BasePartition<Node<String, PayloadIntervals>, IndexedPartitionPayload>> partitions =
                indexBuilder.build(frames, partitionSize);
        // for each partition, add
        buildTicker.end();

        PurePlaneSweepProcessor processor = new PurePlaneSweepProcessor(partitions, partitionSize);
//        IndexedSingleTypeProcessor processor = new IndexedSingleTypeProcessor(partitions, partitionSize);
        evalTicker.start();
        Collection<WindowWithScore> collection = processor.topk(k, w, objNum);
        evalTicker.end();
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
