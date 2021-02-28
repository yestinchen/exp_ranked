package com.topk.online.processors;

import com.topk.offline.bean.CLabel;

public class ConditionItem {
    int objNum;
    int lambda;
    CLabel type;

    public ConditionItem(int objNum, int lambda, CLabel type) {
        this.objNum = objNum;
        this.lambda = lambda;
        this.type = type;
    }

    public int getObjNum() {
        return objNum;
    }

    public void setObjNum(int objNum) {
        this.objNum = objNum;
    }

    public int getLambda() {
        return lambda;
    }

    public void setLambda(int lambda) {
        this.lambda = lambda;
    }

    public CLabel getType() {
        return type;
    }

    public void setType(CLabel type) {
        this.type = type;
    }
}