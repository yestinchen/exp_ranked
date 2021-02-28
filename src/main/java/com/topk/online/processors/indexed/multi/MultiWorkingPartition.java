package com.topk.online.processors.indexed.multi;

import com.topk.offline.bean.CLabel;
import com.topk.offline.bean.PayloadIntervals;
import com.topk.online.processors.indexed.IndexedWorkingPartition2;

public class MultiWorkingPartition {

    IndexedWorkingPartition2<PayloadIntervals> wp;

    // what label?
    CLabel label;

    public MultiWorkingPartition(IndexedWorkingPartition2<PayloadIntervals> wp, CLabel label) {
        this.wp = wp;
        this.label = label;
    }

    public IndexedWorkingPartition2<PayloadIntervals> getWp() {
        return wp;
    }

    public void setWp(IndexedWorkingPartition2<PayloadIntervals> wp) {
        this.wp = wp;
    }

    public CLabel getLabel() {
        return label;
    }

    public void setLabel(CLabel label) {
        this.label = label;
    }
}
