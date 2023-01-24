package com.dreamlab.utils;

public abstract class Operation {
    abstract public void doIt();

    public void onFailure(Exception cause) {
        throw new RuntimeException(cause);
    }
}
