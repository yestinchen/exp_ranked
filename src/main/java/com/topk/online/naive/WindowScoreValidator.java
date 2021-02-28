package com.topk.online.naive;

import com.common.bean.VideoFrame;
import com.common.bean.VideoSequence;
import com.common.io.VideoSequenceIO;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class WindowScoreValidator {

    public static void main(String[] args) {
//        String file = "data/new/visualroad1.txt";
//        String file = "data/new/MOT16-06.txt";
        String file = "data/yolo/traffic1.txt";


        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, Arrays.asList("car"));
        List<Set<String>> frames = new ArrayList<>();
        Set<String> filterSet = new HashSet<>(
//                Arrays.asList("person732", "person711", "person551", "person774")
                Arrays.asList(
                        "car1637", "car1313", "car1742", "car1545", "car1655", "car1792"
                )
        );
        for (VideoFrame f : videoSequence.getFrames()) {
            Set<String> set = f.getIds();
            set.removeIf(i -> !filterSet.contains(i));
            frames.add(set);
        }

        // list frames.
        List<Integer> frameIds = new ArrayList<>();
        for (int i=0; i < frames.size(); i++) {
            if (frames.get(i).size() == filterSet.size()) {
                frameIds.add(i);
            }
        }
        System.out.println(IntervalUtils.toInterval(frameIds));

        int objNum = filterSet.size();
        int[] se = new int[]{3358, 4357};

        ScoreComputer sc = new ScoreComputer();
        int score = sc.scoreFor(frames.subList(se[0], se[1] + 1), objNum);
        System.out.println("score:" + score);
    }
}
