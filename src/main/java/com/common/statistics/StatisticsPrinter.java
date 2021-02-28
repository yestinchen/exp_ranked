package com.common.statistics;

import com.common.bean.Tuple2;
import com.common.io.SimplifiedIOs;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.offline.bitset.BitsetCompactSingleIndexBuilder;
import com.topk.offline.bitset.BitsetSingleIndexBuilder;
import com.topk.offline.builder.partition.BitsetPartitionPayload;
import com.topk.offline.builder.partition.CompactBitsetPartitionPayload;

import java.util.*;

public class StatisticsPrinter {
    public static void main(String[] args) {

//        String file = "data/new/MOT16-13.txt";
//        String file = "data/new/MOT16-06.txt";
//        String file = "data/gen/test-uniform1.txt";
//        String file = "data/gen/test-gaussian1.txt";
//        String file = "data/gen/test-uniform1-100-0.3.txt";
//        String file="data/yolo/ff.txt";
//        String file="data/yolo/joker.txt";
//        String file = "data/yolo/midway.txt";
//        String file = "data/yolo/inception.txt";
//        String file = "data/yolo/news1.txt";
//        String file = "data/yolo/news2.txt";
//        String file = "data/yolo/news3.txt";
//        String file = "data/yolo/traffic1.txt";
//        String file = "data/yolo/traffic2.txt";
//        String file = "data/yolo/traffic3.txt";

        String file = "data/concat/d3.txt";
//        int partitionSize = 100;
//        List<String> types = Arrays.asList("person");
        List<String> types = Arrays.asList("car");
        printTimeSpan(file, types);
//        printPartitionInfo(file,partitionSize, types);
//        System.out.println(">>>>>>>>>>>>>>> compact:");
//        printCompactPartitionInfo(file,partitionSize, types);
    }


    public static void printCompactPartitionInfo(String file, int partitionSize,
                                          List<String> types) {

        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, types);
        BitsetCompactSingleIndexBuilder indexBuilder = new BitsetCompactSingleIndexBuilder();

        List<BasePartition<Node<List<String>, PayloadWMaskDewey>,
                CompactBitsetPartitionPayload<PayloadWMaskDewey>>> partitions =
                indexBuilder.build(frames, partitionSize);
        // partitions.
        int i =0;
        for (BasePartition<Node<List<String>, PayloadWMaskDewey>,
                CompactBitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {

            int nodeNum =0;
            Map<Integer, Integer> nodeCountMap = new HashMap<>();
            for (Node<List<String>, PayloadWMaskDewey> node:  partition.getRoots()){
                int key = node.getPayload().getMask().cardinality();
                int v = nodeCountMap.getOrDefault(key, 0);
                v++;
                nodeCountMap.put(key, v);
                nodeNum ++;
            }

            System.out.println("partition :" + i + " ; NodeNum: " + nodeNum);

            i++;

            List<Map.Entry<Integer, Integer>> list = new ArrayList<>(nodeCountMap.entrySet());
            list.sort((x1, x2)-> x1.getKey().compareTo(x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: list) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }
        }

        // for each partition ,print obj vs node count.
        i = 0;

        System.out.println("==============");

        for (BasePartition<Node<List<String>, PayloadWMaskDewey>,
                CompactBitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {
            System.out.println("partition :" + i);

            Map<String, List<Node<List<String>, PayloadWMaskDewey>>> nodeCountMap = new HashMap<>();
            for (Node<List<String>, PayloadWMaskDewey> node:  partition.getRoots()){
                for (String key: node.getKey()) {
                    List<Node<List<String>, PayloadWMaskDewey>> v
                            = nodeCountMap.getOrDefault(key, new ArrayList<>());
                    v.add(node);
                    nodeCountMap.put(key, v);
                }
            }
            i++;

            // sort.
            for (List<Node<List<String>, PayloadWMaskDewey>> nodeList : nodeCountMap.values()) {
                nodeList.sort((x1, x2)-> Integer.compare(x2.getPayload().getCount(), x1.getPayload().getCount()));
                if (nodeList.size() > 300) {
                    System.out.println("what?");
                }
            }

            Map<Integer, Integer> countNumMap = new HashMap<>();
            for (String nodeKey: nodeCountMap.keySet()) {
                int key = nodeCountMap.get(nodeKey).size();
                int value = countNumMap.getOrDefault(key, 0);
                value ++;
                countNumMap.put(key, value);
            }

            List<Map.Entry<Integer, Integer>> list = new ArrayList<>(countNumMap.entrySet());
            list.sort((x1, x2)-> x1.getKey().compareTo(x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: list) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }

        }
        System.out.println("---------- frame count, node count");
        i = 0;
        for (BasePartition<Node<List<String>, PayloadWMaskDewey>,
                CompactBitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {
            System.out.println("partition :" + i);
            Map<Integer, List<Node<List<String>, PayloadWMaskDewey>>> frameCountNodeMap = new HashMap<>();
            for (Node<List<String>, PayloadWMaskDewey> node : partition.getRoots()) {
                int key = node.getPayload().getCount();
                List<Node<List<String>, PayloadWMaskDewey>> v = frameCountNodeMap.getOrDefault(key, new ArrayList<>());
                v.add(node);
                frameCountNodeMap.put(key, v);
                if (key == 1 && v.size() >300) {
                    System.out.println("what?>");
                }
            }

            Map<Integer, Integer> newMap = new HashMap<>();
            for (Integer key: frameCountNodeMap.keySet()) {
                newMap.put(key, frameCountNodeMap.get(key).size());
            }

            List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(newMap.entrySet());
            entries.sort((x1, x2)-> Integer.compare(x1.getKey(), x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: entries) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }
            i ++;
        }
    }


    public static void printPartitionInfo(String file, int partitionSize,
                                          List<String> types) {

        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, types);
        BitsetSingleIndexBuilder indexBuilder = new BitsetSingleIndexBuilder();

        List<BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>>> partitions =
                indexBuilder.build(frames, partitionSize);
        // partitions.
        int i =0;
        for (BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {

            int nodeNum =0;
            Map<Integer, Integer> nodeCountMap = new HashMap<>();
            for (Node<String, PayloadWMaskDewey> node:  partition.getRoots()){
                int key = node.getPayload().getMask().cardinality();
                int v = nodeCountMap.getOrDefault(key, 0);
                v++;
                nodeCountMap.put(key, v);
                nodeNum ++;
            }

            System.out.println("partition :" + i + " ; NodeNum: " + nodeNum);

            i++;

            List<Map.Entry<Integer, Integer>> list = new ArrayList<>(nodeCountMap.entrySet());
            list.sort((x1, x2)-> x1.getKey().compareTo(x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: list) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }
        }

        // for each partition ,print obj vs node count.
        i = 0;

        System.out.println("==============");

        for (BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {
            System.out.println("partition :" + i);

            Map<String, List<Node<String, PayloadWMaskDewey>>> nodeCountMap = new HashMap<>();
            for (Node<String, PayloadWMaskDewey> node:  partition.getRoots()){
                String key = node.getKey();
                List<Node<String, PayloadWMaskDewey>> v
                        = nodeCountMap.getOrDefault(key, new ArrayList<>());
                v.add(node);
                nodeCountMap.put(key, v);
            }
            i++;

            // sort.
            for (List<Node<String, PayloadWMaskDewey>> nodeList : nodeCountMap.values()) {
                nodeList.sort((x1, x2)-> Integer.compare(x2.getPayload().getCount(), x1.getPayload().getCount()));
                if (nodeList.size() > 300) {
                    System.out.println("what?");
                }
            }

            Map<Integer, Integer> countNumMap = new HashMap<>();
            for (String nodeKey: nodeCountMap.keySet()) {
                int key = nodeCountMap.get(nodeKey).size();
                int value = countNumMap.getOrDefault(key, 0);
                value ++;
                countNumMap.put(key, value);
            }

            List<Map.Entry<Integer, Integer>> list = new ArrayList<>(countNumMap.entrySet());
            list.sort((x1, x2)-> x1.getKey().compareTo(x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: list) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }
        }
        System.out.println("---------- frame count, node count");
        i = 0;
        for (BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>> partition: partitions) {
            System.out.println("partition :" + i);
            Map<Integer, Integer> frameCountNodeMap = new HashMap<>();
            for (Node<String, PayloadWMaskDewey> node : partition.getRoots()) {
                int key = node.getPayload().getCount();
                int v = frameCountNodeMap.getOrDefault(key, 0);
                v ++;
                frameCountNodeMap.put(key, v);
            }

            List<Map.Entry<Integer, Integer>> entries = new ArrayList<>(frameCountNodeMap.entrySet());
            entries.sort((x1, x2)-> Integer.compare(x1.getKey(), x2.getKey()));
            for (Map.Entry<Integer, Integer> entry: entries) {
                System.out.println(entry.getKey()+"\t"+entry.getValue());
            }
            i ++;
        }
    }

    public static void printTimeSpan(String file,
                             List<String> types) {
        List<Set<String>> frames = SimplifiedIOs.readIDOnly(file, types);
        Map<String, List<Integer>> objMap = new HashMap<>();

        Map<Integer, Integer> objNumPerFrame = new HashMap<>();
        for (int i=0;i < frames.size(); i++) {
            objNumPerFrame.put(i, frames.get(i).size());
            for (String obj: frames.get(i)) {
                objMap.computeIfAbsent(obj, x -> new ArrayList<>()).add(i);
            }
        }
        System.out.println("frames:" + frames.size());
        System.out.println("total objs:" + objMap.size());

        System.out.println("avg obj per frame:" + (1.0 *
                objNumPerFrame.values().stream().mapToInt(i->i).sum() / objNumPerFrame.size()));

        System.out.println("max obj per frame:" + (1.0 *
                objNumPerFrame.values().stream().mapToInt(i->i).max().getAsInt() ));

        Map<Integer, Integer> objNumFrameCountMap = new HashMap<>();
        for (Map.Entry<Integer, Integer> pair: objNumPerFrame.entrySet()) {
            int v = objNumFrameCountMap.getOrDefault(pair.getValue(), 0);
            v++;
            objNumFrameCountMap.put(pair.getValue(), v);
        }

        // output.
        List<Map.Entry<Integer, Integer>> valueMap = new ArrayList<>(objNumFrameCountMap.entrySet());
        valueMap.sort((x1, x2)-> Integer.compare(x1.getKey(), x2.getKey()));
        for (Map.Entry<Integer, Integer> entry: valueMap) {
            System.out.println(entry.getKey()+"\t"+entry.getValue());
        }

        List<Tuple2<String, Integer>> objSpanList = new ArrayList<>();
        Map<Integer, Integer> framesObjNumMap = new HashMap<>();
        for (String key: objMap.keySet()) {
            List<Integer> appearFrames = objMap.get(key);
            int start = appearFrames.get(0);
            int end = appearFrames.get(appearFrames.size() - 1);
            objSpanList.add(new Tuple2<>(key, end - start + 1));

            int v = framesObjNumMap.getOrDefault(appearFrames.size(), 0);
            v++;
            framesObjNumMap.put(appearFrames.size(), v);
        }
        Collections.sort(objSpanList, (x1, x2)-> Integer.compare(x1.get_2(), x2.get_2()));
        for (Tuple2<String, Integer> pair: objSpanList) {
            System.out.println(pair.get_2()+":"+pair.get_1());
        }

        Map<Integer, Integer> spanCountMap = new HashMap<>();
        for (Tuple2<String, Integer> pair: objSpanList) {
            Integer v =  spanCountMap.get(pair.get_2());
            if (v == null) v = 0;
            v++;
            spanCountMap.put(pair.get_2(), v);
        }

        List<Map.Entry<Integer, Integer>> countList = new ArrayList<>(spanCountMap.entrySet());
        Collections.sort(countList, (x1, x2)-> Integer.compare(x1.getKey(), x2.getKey()));

        System.out.println("======span count ========");
        for (Map.Entry<Integer, Integer> pair: countList) {
            System.out.println(pair.getKey()+"\t"+pair.getValue());
        }

        List<Map.Entry<Integer, Integer>> frameObjList = new ArrayList<>(framesObjNumMap.entrySet());
        frameObjList.sort((x1, x2)-> x1.getKey().compareTo(x2.getKey()));

        System.out.println("========= frame count =========");
        for (Map.Entry<Integer, Integer> pair: frameObjList) {
            System.out.println(pair.getKey()+"\t"+pair.getValue());
        }

    }
}
