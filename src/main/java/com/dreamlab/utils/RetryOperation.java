package com.dreamlab.utils;

public class RetryOperation {
    public static void doWithRetry(int maxAttempts, Operation operation) {
        assert maxAttempts > 0;
        for (int count = 0; count < maxAttempts; count++) {
            try {
                operation.doIt();
                count = maxAttempts; // don't retry
            } catch (Exception e) {
                if (count == maxAttempts - 1) {
                    operation.onFailure(e);
                }
            }
        }
    }
}
