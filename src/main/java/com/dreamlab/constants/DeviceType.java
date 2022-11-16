package com.dreamlab.constants;

public enum DeviceType {
    EDGE,
    FOG,
    CLOUD;

    private static final DeviceType values[] = values();

    public static DeviceType get(int ordinal) {
        return values[ordinal];
    }
}
