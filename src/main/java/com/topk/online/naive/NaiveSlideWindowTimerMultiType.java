package com.topk.online.naive;

import com.common.bean.Tuple2;
import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.CLabel;
import com.topk.online.processors.ConditionItem;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class NaiveSlideWindowTimerMultiType {
    public static void main(String[] args) {

//        String file = "data/new/visualroad1.txt";
//        String file = "data/new/visualroad2.txt";
//        String file = "data/new/visualroad3.txt";
//        String file = "data/new/visualroad4.txt";
//        String file = "data/new/MOT16-13.txt";
//        String file = "data/new/MOT16-06.txt";
//        String file = "data/new/MVI_40171.txt";
//        String file = "data/new/MVI_40751.txt";
//        List<String> types = Arrays.asList("person", "car");
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/traffic2.txt";
//        String file = "data/yolo/traffic3.txt";
//        String file = "data/yolo/ff.txt";
//        String file = "data/yolo/inception.txt";
//        String file = "data/yolo/midway.txt";
        String file = "data/yolo/news2.txt";
//        String file = "data/concat/d1.txt";
        List<String> types = Arrays.asList("person", "car");
//        List<String> types = Arrays.asList("truck", "car");
//        List<String> types = Arrays.asList("person");
        Tuple2<List<Set<String>>, Map<String, CLabel>> tuple = SimplifiedIOs.readIDsAndTypeMapping(file, types);

        // filter.
//        for (Set<String> s : tuple.get_1()) {
//            s.removeIf(i -> !Arrays.asList("car2", "person67", "person8", "car4").contains(i));
//        }
//
//        List<Integer> frameIds = new ArrayList<>();
//        for (int i=0; i < tuple.get_1().size(); i++) {
//            if (tuple.get_1().get(i).size() == 4) {
//                frameIds.add(i);
//            }
//        }
//        System.out.println(IntervalUtils.toInterval(frameIds));

        int w = 600;

        List<List<ConditionItem>> conditions = Arrays.asList(
                Arrays.asList(
                        new ConditionItem(2, 1, CLabel.CAR)
                ),
                Arrays.asList(
                        new ConditionItem(2, 1, CLabel.PERSON)
                )
//                Arrays.asList(
//                        new ConditionItem(2, 1, CLabel.TRUCK)
//                )
        );

        Ticker ticker = new Ticker();
        ticker.start();
        MultiTypeScoreComputer c = new MultiTypeScoreComputer();
        Map<Integer, Integer> wsMap = c.scoreFor(tuple.get_1(), tuple.get_2(), conditions, w);
        ticker.end();
        System.out.println("time:" + ticker.report());

        System.out.println("wsMap:" + wsMap);

        System.out.println("2919:" + wsMap.get(2919) );
        System.out.println("2835:" + wsMap.get(2835) );
        System.out.println("3386:" + wsMap.get(3386) );
        System.out.println("2737:" + wsMap.get(2737) );


        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(wsMap.entrySet());
        list.sort((x1, x2) -> -x1.getValue().compareTo(x2.getValue()));

        System.out.println("list:"+ list.get(0));
    }
}
