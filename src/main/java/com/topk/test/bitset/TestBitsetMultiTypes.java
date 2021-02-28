package com.topk.test.bitset;

import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.*;
import com.topk.offline.bitset.BitsetMultiIndexBuilder;
import com.topk.offline.composite.MultiIndexBuilder;
import com.topk.online.processors.ConditionItem;
import com.topk.online.processors.bitset.multi.BitsetMultiTypeProcessor;
import com.topk.online.result.WindowWithScore;

import java.util.*;

public class TestBitsetMultiTypes {

    public static void main(String[] args) throws InterruptedException {
//        String file = "data/yolo/visualroad1.txt";
//        String file = "data/yolo/visualroad2.txt";
//        String file = "data/yolo/visualroad3.txt";
//        String file = "data/yolo/visualroad4.txt";
//        String file = "data/yolo/MOT16-13.txt";
//        String file = "data/yolo/MOT16-06.txt";
//        String file = "data/yolo/MVI_40171.txt";
//        String file = "data/yolo/MVI_40751.txt";
//        String file = "data/yolo/traffic1.txt"; //12401
//        String file = "data/yolo/traffic2.txt"; //
//        String file = "data/yolo/traffic3.txt"; //600?
//        String file = "data/concat/d1.txt";
//        String file = "data/yolo/ff.txt";
//        String file = "data/yolo/inception.txt";
//        String file = "data/yolo/midway.txt";
        String file = "data/yolo/news2.txt";
        // 77577
//        List<String> types = Arrays.asList("truck", "car"); //341
        List<String> types = Arrays.asList("person", "car"); // 1072
//        List<String> types = Arrays.asList("person");
//        List<String> types = Arrays.asList("car");
        Map<String, List<Set<String>>> typeFrames = SimplifiedIOs.readIDGroupByType(file, types);

        // split each frame.

        int partitionSize = 600;
        int k = 1;
        int w = 600;

        List<List<ConditionItem>> conditions = Arrays.asList(
                Arrays.asList(
                        new ConditionItem(2, 1, CLabel.CAR)
                )
                ,
//                Arrays.asList(
//                        new ConditionItem(2, 1, CLabel.TRUCK)
//                )
                Arrays.asList(
                        new ConditionItem(2, 1, CLabel.PERSON)
                )
        );

//        Thread.sleep(10000);

        Ticker ticker = new Ticker();
        ticker.start();
        BitsetMultiIndexBuilder indexBuilder = new BitsetMultiIndexBuilder(partitionSize);
        for (String type: typeFrames.keySet()) {
            CLabel label = CLabel.labelFor(type);
            if (label == null) {
                System.out.println("untranslated label:" + type);
                System.exit(-1);
            }
            indexBuilder.build(label, typeFrames.get(type));
        }
        ticker.end();

        Ticker evalTicker = new Ticker();
        evalTicker.start();

        BitsetMultiTypeProcessor topkProcessorMultipleType = new BitsetMultiTypeProcessor(
                indexBuilder.getTypeMap() , partitionSize
        );

        Collection<WindowWithScore> collection =
                topkProcessorMultipleType.topk(k, w, conditions);

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
        System.out.println("build time:"+ ticker.report());
        System.out.println("eval time: " + evalTicker.report());


    }
}
