package com.topk.offline.bean;

public enum CLabel {
    CAR, PERSON, TRUCK;

    public static CLabel labelFor(String str) {
        switch (str) {
            case "car": return CAR;
            case "person": return PERSON;
            case "truck": return TRUCK;
        }
        return null;
    }
}