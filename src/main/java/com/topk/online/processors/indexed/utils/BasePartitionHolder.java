package com.topk.online.processors.indexed.utils;

import com.topk.offline.bean.BasePartition;

public interface BasePartitionHolder<T, F> {
    BasePartition<T, F> getBasePartition();

    int getRemainingCount();
}
