package com.topk.offline.builder.node;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMaskMap;
import com.topk.utils.IntervalUtils;

import java.util.*;

public class CTNodeWMaskMapCreator implements NodeCreator<String, PayloadWMaskMap> {

    Map<CLabel, List<String>> orderedObjMap;

    public CTNodeWMaskMapCreator(Map<CLabel, List<String>> orderedObjMap) {
        this.orderedObjMap = orderedObjMap;
    }

    @Override
    public Node<String, PayloadWMaskMap> createNewNode(String key, int count, List<Integer> frames,
                                                       Set<String> remainingObjs, List<Set<String>> frameList,
                                                       int startFrameId, Node<String, PayloadWMaskMap> prevNode,
                                                       int indexInCurrentLevel, CLabel label) {
        Node<String, PayloadWMaskMap> node = new Node<>();
        node.setKey(key);
        PayloadWMaskMap payload = new PayloadWMaskMap();
        payload.setCount(count);
        payload.setIntervals(IntervalUtils.toInterval(frames));
        // deal with bitset.
        Map<CLabel, BitSet> newBitSetMap = new HashMap<>();
        BitSet bitSet = new BitSet();
        if (prevNode != null) {
            Map<CLabel, BitSet> prevSets = prevNode.getPayload().getMaskMap();
            newBitSetMap.putAll(prevSets);
            BitSet prevCurrentNode =  prevSets.get(label);
            bitSet.or(prevCurrentNode);
        }
        int pos = orderedObjMap.get(label).indexOf(key);
        bitSet.set(pos, true);
        // put to map.
        newBitSetMap.put(label, bitSet);
        payload.setMaskMap(newBitSetMap);
        payload.setLabel(label);

        node.setPayload(payload);
        return node;
    }
}
