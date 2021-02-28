package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.utils.IntervalUtils;

import java.util.List;
import java.util.Set;

public class STNodeWIntervalCreator implements NodeCreator<String, PayloadIntervals>{

    @Override
    public Node<String, PayloadIntervals> createNewNode(String key, int count, List<Integer> frames,
                                                        Set<String> remainingObjs,
                                                        List<Set<String>> frameList,
                                                        int startFrameId,
                                                        Node<String, PayloadIntervals> prevNode,
                                                        int index,
                                                        CLabel label) {
        Node<String, PayloadIntervals> node = new Node<>();
        node.setKey(key);
        PayloadIntervals payloadIntervals = new PayloadIntervals();
        payloadIntervals.setCount(count);
        payloadIntervals.setIntervals(IntervalUtils.toInterval(frames));
        node.setPayload(payloadIntervals);
        return node;
    }
}
