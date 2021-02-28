package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.offline.bean.PayloadClassIntervalsWMask;
import com.topk.utils.IntervalUtils;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CTNodeWMaskCreator implements NodeCreator<String, PayloadClassIntervalsWMask> {

    List<String> orderedObjs;

    public CTNodeWMaskCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public Node<String, PayloadClassIntervalsWMask> createNewNode(String key, int count,
                                                              List<Integer> frames, Set<String> remainingObjs,
                                                                  List<Set<String>> frameList,
                                                                  int startFrameId,
                                                                  Node<String, PayloadClassIntervalsWMask> prevNode,
                                                                  int currentIndexInLevel,
                                                                  CLabel label) {
        Node<String, PayloadClassIntervalsWMask> node = new Node<>();
        node.setKey(key);
        PayloadClassIntervalsWMask payload = new PayloadClassIntervalsWMask();
        payload.setCount(count);
        payload.setLabel(label);
        payload.setIntervals(IntervalUtils.toInterval(frames));
        Set<String> allObjs = new HashSet<>();
        for (int i : frames) {
            allObjs.addAll(frameList.get(i-startFrameId));
        }
        BitSet bitset = new BitSet();
        for (String obj: remainingObjs) {
            if (allObjs.contains(obj)) {
                bitset.set(orderedObjs.indexOf(obj), true);
            }
        }
        payload.setMask(bitset);
        if (payload.getIntervals() == null) {
            System.out.println("null!");
        }
        node.setPayload(payload);
        return node;
    }
}
