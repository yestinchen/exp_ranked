package com.topk.test.builder;

import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.CLabel;
import com.topk.offline.composite.MultiIndexBuilder;

import java.util.*;

public class MultiIndexBuilderTest {

    public static void main(String[] args) {
        String file = "data/new/visualroad1.txt";
        List<String> types = Arrays.asList("person", "car");

        Map<String, List<Set<String>>> typeFrames = SimplifiedIOs.readIDGroupByType(file, types);
        // split each frame.

        int partitionSize = 20;

        Ticker ticker = new Ticker();
        ticker.start();
        MultiIndexBuilder indexBuilder = new MultiIndexBuilder(partitionSize);
        for (String type: typeFrames.keySet()) {
            CLabel label = CLabel.labelFor(type);
            if (label == null) {
                System.out.println("untranslated label:" + type);
                System.exit(-1);
            }
            indexBuilder.build(label, typeFrames.get(type));
        }
        ticker.end();
        System.out.println("time:"+ticker.report());
    }
}
