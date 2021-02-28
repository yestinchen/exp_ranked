package com.topk.test.builder;

import com.common.bean.Tuple2;
import com.common.io.SimplifiedIOs;
import com.interval.util.Ticker;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.composite.CompositeIndexBuilder;

import java.util.*;

public class CompositeIndexBuilderTest {

    public static void main(String[] args) {
        String file = "data/new/visualroad1.txt";
        List<String> types = Arrays.asList("person", "car");

        Tuple2<List<Set<String>>, Map<String, CLabel>> tuple = SimplifiedIOs.readIDsAndTypeMapping(file, types);
        List<Set<String>> frames = tuple.get_1();
        Map<String, CLabel> typeMapping =tuple.get_2();
        // split each frame.

        int partitionSize = 20;

        Ticker ticker = new Ticker();
        ticker.start();
        CompositeIndexBuilder indexBuilder = new CompositeIndexBuilder();
        List<BasePartition<Node<String, PayloadClassIntervals>, Byte>> roots
                = indexBuilder.build(frames, typeMapping, partitionSize);
        ticker.end();
        System.out.println("time:"+ticker.report());
    }

}
