package com.dreamlab.utils;

public abstract class Operation {
    abstract public void doIt();

    public void onFailure(Exception cause) {
        cause.printStackTrace();
        throw new RuntimeException(cause);
    }
}
