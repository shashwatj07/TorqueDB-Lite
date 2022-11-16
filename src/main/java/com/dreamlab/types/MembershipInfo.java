package com.dreamlab.types;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class MembershipInfo implements Serializable {

    private static final long serialVersionUID = -4274519773685314392L;
    private final UUID parentFogId;
    private final Instant lastHeartbeat;
    private final int ttlSecs;

    public MembershipInfo(UUID parentFogId, Instant lastHeartbeat, int ttlSecs) {
        this.parentFogId = parentFogId;
        this.lastHeartbeat = lastHeartbeat;
        this.ttlSecs = ttlSecs;
    }

    public UUID getParentFogId() {
        return parentFogId;
    }

    public Instant getLastHeartbeat() {
        return lastHeartbeat;
    }

    public int getTtlSecs() {
        return ttlSecs;
    }
}
