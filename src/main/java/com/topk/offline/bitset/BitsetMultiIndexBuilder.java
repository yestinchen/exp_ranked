package com.topk.offline.bitset;

import com.topk.offline.bean.*;
import com.topk.offline.builder.partition.BitsetPartitionPayload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BitsetMultiIndexBuilder {

    Map<CLabel, List<BasePartition<Node<String, PayloadWMaskDewey>,
                BitsetPartitionPayload<PayloadWMaskDewey>>>> typeMap = new HashMap<>();

    BitsetSingleIndexBuilder singleBuilder = new BitsetSingleIndexBuilder();
    int partitionSize;

    public BitsetMultiIndexBuilder(int partitionSize) {
        this.partitionSize= partitionSize;
    }

    public void build(CLabel type, List<Set<String>> frames) {
        typeMap.put(type, singleBuilder.build(frames, partitionSize));
    }

    public Map<CLabel, List<BasePartition<Node<String, PayloadWMaskDewey>,
            BitsetPartitionPayload<PayloadWMaskDewey>>>> getTypeMap() {
        return typeMap;
    }
}
