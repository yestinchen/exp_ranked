package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.offline.bean.PayloadIntervalsWMask;
import com.topk.utils.IntervalUtils;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * With mask
 */
public class STNodeWMaskCreator implements NodeCreator<String, PayloadIntervalsWMask> {

    List<String> orderedObjs;

    public STNodeWMaskCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public Node<String, PayloadIntervalsWMask> createNewNode(String key, int count,
                                                             List<Integer> frames,
                                                             Set<String> remainingObjs,
                                                             List<Set<String>> frameList,
                                                             int startFrameId,
                                                             Node<String, PayloadIntervalsWMask> prevNode,
                                                             int index,
                                                             CLabel label) {
        Node<String, PayloadIntervalsWMask> node = new Node<>();
        node.setKey(key);
        PayloadIntervalsWMask payload = new PayloadIntervalsWMask();
        payload.setCount(count);
        payload.setIntervals(IntervalUtils.toInterval(frames));
        // remaining bitsets.
        // 1. compute coverted objects.
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
        node.setPayload(payload);
        return node;
    }
}
