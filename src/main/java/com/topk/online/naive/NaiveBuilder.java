package com.topk.online.naive;

import com.common.bean.VideoFrame;
import com.common.bean.VideoSequence;
import com.common.io.VideoSequenceIO;
import com.interval.util.SetUtils;
import com.interval.util.Ticker;
import com.topk.offline.bean.BasePartition;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.utils.BitSetUtils;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class NaiveBuilder {

    public BasePartition<Node<String, PayloadWMaskDewey>, PayloadWMaskDewey> buildPartition(List<Set<String>> frames,
                                                                   int start, int size) {
        Map<Set<String>, Set<Integer>> objMap = new HashMap<>();
        Set<String> objs = new HashSet<>();
        int i =start;
        for (Set<String> frame: frames) {
            objs.addAll(frame);
            for (Set<String> key: new HashSet<>(objMap.keySet())) {
                // compute intersection?
                Set<String> result = SetUtils.intersect(frame, key);
                if (result.size() > 0) {
                    Set<Integer> timeSet = objMap.computeIfAbsent(result, x -> new HashSet<>());
                    timeSet.addAll(objMap.getOrDefault(key, Collections.emptySet()));
                    timeSet.add(i);
                }
            }
            if (frame != null && frame.size() >0 && !objMap.containsKey(frame)) {
                objMap.put(frame, new HashSet<>(Arrays.asList(i)));
            }
            i ++;
        }

        List<String> objList = new ArrayList<>(objs);
        Collections.sort(objList);

        int j = 0;
        List<Node<String, PayloadWMaskDewey>>  nodeList = new ArrayList<>();
        for (Map.Entry<Set<String>, Set<Integer>> entry: objMap.entrySet()) {
            Node<String, PayloadWMaskDewey> node = new Node<>();
            node.setId(j);
            PayloadWMaskDewey payloadWMaskDewey = new PayloadWMaskDewey();

            BitSet bitSet = new BitSet();
            for (int k = 0; k < objList.size(); k++) {
                String obj = objList.get(k);
                if (entry.getKey().contains(obj)) {
                    bitSet.set(i, true);
                }
            }
            payloadWMaskDewey.setMask(bitSet);
            payloadWMaskDewey.setCount(entry.getValue().size());
            List<Integer> frameList = new ArrayList<>(entry.getValue());
            Collections.sort(frameList);
            payloadWMaskDewey.setIntervals(IntervalUtils.toInterval(frameList));
            node.setPayload(payloadWMaskDewey);
            nodeList.add(node);
            j++;
        }

        BasePartition<Node<String, PayloadWMaskDewey>, PayloadWMaskDewey> partition = new BasePartition<>();
        partition.setObjs(objList);
        partition.setSize(size);
        partition.setStartFrame(start);
        partition.setRoots(nodeList);
        return partition;
    }

    public List<BasePartition<Node<String, PayloadWMaskDewey>, PayloadWMaskDewey>> build(List<Set<String>> frames, int partitionSize) {
        List<BasePartition<Node<String, PayloadWMaskDewey>, PayloadWMaskDewey>> result = new ArrayList<>();
        for (int i =0; i < Math.ceil(frames.size() * 1.0 / partitionSize); i++) {
            int start = i * partitionSize;
            int end = Math.min((i+1) * partitionSize - 1, frames.size());
            result.add(buildPartition(frames.subList(start, end), start, end - start + 1));
        }
        return result;
    }

    public static void main(String[] args) {

        String file = "data/gen/test-uniform-occ-100-0.2.txt";
        VideoSequence videoSequence = VideoSequenceIO.readFromFile(file, Arrays.asList("person"));
        List<Set<String>> frames = new ArrayList<>();
        for (VideoFrame f : videoSequence.getFrames()) {
            frames.add(f.getIds());
        }
        Ticker ticker = new Ticker();
        NaiveBuilder builder = new NaiveBuilder();

        ticker.start();
        List<BasePartition<Node<String, PayloadWMaskDewey>, PayloadWMaskDewey>> partitions =
                builder.build(frames, 200);
        ticker.end();
        System.out.println("time:"+ticker.report());
        System.out.println("p:" + partitions.size());
    }
}
