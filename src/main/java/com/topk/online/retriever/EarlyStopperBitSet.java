package com.topk.online.retriever;

import com.topk.offline.bean.Node;
import com.topk.offline.bean.PayloadWMask;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class EarlyStopperBitSet<T, F extends PayloadWMask> implements EarlyStopper<T, F> {

    BitSet selected;

    public EarlyStopperBitSet(BitSet selectedBitSet) {
        this.selected = selectedBitSet;
    }

    public EarlyStopperBitSet(List<String> orderedObjs, Set<String> selectedObjs) {
        selected = new BitSet(orderedObjs.size());
        for (String s : selectedObjs) {
            int index = orderedObjs.indexOf(s);
            if (index > 0) {
                selected.set(index, true);
            }
        }
    }

    @Override
    public boolean shouldStop(Node<T, F> node) {
        BitSet mask = node.getPayload().getMask();
        BitSet bitSet = new BitSet();
        bitSet.or(selected);
        bitSet.and(mask);
        boolean stop =bitSet.nextSetBit(0) == -1;
//        if (stop && mask.nextSetBit(0) >0 ) {
//            System.out.println("good.");
//        }
        return stop;
    }
}
