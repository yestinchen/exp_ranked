package com.topk.online.naive;

import com.common.bean.VideoFrame;
import com.common.bean.VideoSequence;
import com.common.io.VideoSequenceIO;
import com.interval.util.Ticker;
import com.topk.io.SimpleReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class NaiveSlideWindowTimer {
    public static void main(String[] args) throws FileNotFoundException {

//        String file = "data/yolo/visualroad4.txt";
//        String file = "data/yolo/MOT16-13.txt";
//        String file = "data/new/MOT16-13.txt";
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/MOT16-06.txt";
//        String file = "data/yolo/MVI_40171.txt";
//        String file = "data/yolo/MVI_40751.txt";
//        String file = "data/gen/test-uniform1.txt";
//        String file = "data/yolo/news2.txt";
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/ff.txt";
//        String file = "data/yolo/inception.txt";
//        String file = "data/yolo/joker.txt";
//        String file = "data/yolo/news1.txt";
//        String file = "data/yolo/news2.txt";
//        String file = "data/yolo/news3.txt";
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/traffic2.txt";
//        String file = "data/yolo/traffic3.txt";
        String file = "data/concat/d3.txt";
//        String file = "data/gen/test-uniform-base-10k-500-300-400-0.2.txt";

//        String file = "data/yolo/midway.txt";
//        String file = "data/yolo/midway.txt";

//        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, Arrays.asList("person"));
        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, Arrays.asList("car"));
        List<Set<String>> frames = new ArrayList<>();
        for (VideoFrame f : videoSequence.getFrames()) {
            frames.add(f.getIds());
        }
//
        int objNum = 6;
//        int w =  1000;
//        int w = 2000;
//        int w= 1000;
//        int w= 300;
//        int w= 3000;
//        int w = 300;
//        int w = 600;
//        int w = 900;
//        int w = 1200;
        int w = 1500;

//        String file = "data/topk-test/t3.txt";
//        List<Set<String>> frames = SimpleReader.read(file);

//        int objNum = 2;
//        int w = 6;

        Ticker ticker = new Ticker();
        ticker.start();
        SlidingWindowScoreComputer c = new SlidingWindowScoreComputer();
        Map<Integer, Integer> map = c.scoreFor(frames, objNum, w);
        ticker.end();

        List<Map.Entry<Integer, Integer>> list = new ArrayList<>(map.entrySet());
        list.sort((x1, x2) -> -x1.getValue().compareTo(x2.getValue()));

        System.out.println("map"+ map);
//        System.out.println("sss:" + map.get(3650));
        System.out.println("time:" + ticker.report());
        System.out.println("max:"+list.get(0));
//        System.out.println("get:"+list.get(999));
    }
}
