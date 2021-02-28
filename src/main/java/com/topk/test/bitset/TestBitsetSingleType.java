package com.topk.test.bitset;

import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.bitset.BitsetSingleIndexBuilder;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.online.processors.bitset.BitsetSingleTypeProcessor;
import com.topk.online.processors.bitset.BitsetSingleTypeProcessor2;
import com.topk.online.processors.bitset.measure.Measurement;
import com.topk.online.result.WindowWithScore;
import com.topk.test.utils.Utils;

import java.util.*;

public class TestBitsetSingleType {

    public static void main(String[] args) throws InterruptedException {
//        String file = "data/yolo/MVI_40751.txt";
//        String file = "data/yolo/MOT16-06.txt";
//        String file = "data/yolo/MOT16-13.txt";
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/new/MOT16-13.txt";
//        String file = "data/gen/test-uniform1.txt";
//        String file = "data/gen/test-gaussian1.txt";
//        String file = "data/gen/test-uniform1-100-0.2.txt";
//        String file = "data/gen/test-uniform-occ-100-0.2.txt";
//        String file = "data/new/MOT16-06.txt";
//        String file = "data/yolo/MVI_40171.txt";
//        String file="data/yolo/news2.txt";
        String file="data/yolo/news1.txt";

//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/traffic3.txt";
//        String file = "data/yolo/ff.txt";
//        String file="data/yolo/joker.txt";
//        String file = "data/gen/test-uniform-base-10k-500-300-400-0.2.txt";
        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, Arrays.asList("person"));
//        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, Arrays.asList("car"));
//        List<Set<String>> frames = SimpleReader.read("data/topk-test/t3.txt");

        int objNum = 6;
        int partitionSize = 600;
        int k = 2000;
        int w = 600;
        int limit = 0;
        if (k > frames.size()) {
            k = frames.size();
        }
//        Thread.sleep(10000);

        if (limit > 0) {
            frames = frames.subList(0, limit);
        }

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
        Collection<WindowWithScore> collection = processor.topk(k, w, objNum, null);
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

//        processor.reportTicker();
    }
}
