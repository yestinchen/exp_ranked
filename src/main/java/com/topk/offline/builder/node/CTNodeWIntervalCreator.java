package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadClassIntervals;
import com.topk.utils.IntervalUtils;

import java.util.List;
import java.util.Set;

/**
 * For composite type index.
 */
public class CTNodeWIntervalCreator implements NodeCreator<String, PayloadClassIntervals> {
    @Override
    public Node<String, PayloadClassIntervals> createNewNode(String key, int count,
                                                             List<Integer> frames, Set<String> remainingObjs,
                                                             List<Set<String>> frameList,
                                                             int startFrameId,
                                                             Node<String, PayloadClassIntervals> prevNode, int indexInCurrentLevel,
                                                             CLabel label) {
        Node<String, PayloadClassIntervals> node = new Node<>();
        node.setKey(key);
        PayloadClassIntervals payload = new PayloadClassIntervals();
        payload.setCount(count);
        payload.setLabel(label);
        payload.setIntervals(IntervalUtils.toInterval(frames));
        node.setPayload(payload);
        return node;
    }
}
