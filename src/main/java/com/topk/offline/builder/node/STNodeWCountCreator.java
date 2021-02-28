package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadCount;

import java.util.List;
import java.util.Set;

public class STNodeWCountCreator implements NodeCreator<String, PayloadCount> {
    @Override
    public Node<String, PayloadCount> createNewNode(String key, int count,
                                                    List<Integer> frames, Set<String> remainingObjs,
                                                    List<Set<String>> frameList,
                                                    int startFrameId,
                                                    Node<String, PayloadCount> prevNode, int index,
                                                    CLabel label) {
        Node<String, PayloadCount> node = new Node<>();
        node.setKey(key);
        PayloadCount payload = new PayloadCount();
        payload.setCount(count);
        node.setPayload(payload);
        return node;
    }
}
