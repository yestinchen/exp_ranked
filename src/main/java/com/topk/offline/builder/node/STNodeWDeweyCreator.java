package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervalsWMask;
import com.topk.offline.bean.PayloadWMaskDewey;
import com.topk.utils.IntervalUtils;

import java.util.*;

/**
 * With dewey number
 */
public class STNodeWDeweyCreator implements NodeCreator<String, PayloadWMaskDewey> {

    List<String> orderedObjs;

    public STNodeWDeweyCreator(List<String> orderedObjs) {
        this.orderedObjs = orderedObjs;
    }

    @Override
    public Node<String, PayloadWMaskDewey> createNewNode(
            String key, int count, List<Integer> frames,
            Set<String> remainingObjs, List<Set<String>> frameList,
            int startFrameId,
            Node<String, PayloadWMaskDewey> prevNode, int indexInCurrentLevel,
            CLabel label) {
        Node<String, PayloadWMaskDewey> node = new Node<>();
        node.setKey(key);
        PayloadWMaskDewey payload = new PayloadWMaskDewey();
        payload.setCount(count);
        payload.setIntervals(IntervalUtils.toInterval(frames));
        // compute bitset for the current node.
        BitSet bitSet = new BitSet();
        if (prevNode != null) {
            BitSet prevSet = prevNode.getPayload().getMask();
            bitSet.or(prevSet);
        }
        // add the current one.
        int pos = orderedObjs.indexOf(key);
        bitSet.set(pos, true);
        payload.setMask(bitSet);
        // dewey number.
        List<Integer> deweyNumber = new ArrayList<>();
        if (prevNode != null) {
            deweyNumber.addAll(prevNode.getPayload().getDeweyNumber());
        }
        deweyNumber.add(indexInCurrentLevel);
        payload.setDeweyNumber(deweyNumber);
        node.setPayload(payload);
//        System.out.println("gen node:" + node);
        return node;
    }
}
