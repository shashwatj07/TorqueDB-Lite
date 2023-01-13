package com.dreamlab.types;

import java.util.UUID;

public class ExecPlan {

    private UUID blockId;
    private UUID fogId;

    public ExecPlan(UUID blockId, UUID fogId) {
        this.blockId = blockId;
        this.fogId = fogId;
    }

    public UUID getBlockId() {
        return blockId;
    }

    public void setBlockId(UUID blockId) {
        this.blockId = blockId;
    }

    public UUID getFogId() {
        return fogId;
    }

    public void setFogId(UUID fogId) {
        this.fogId = fogId;
    }
//    public IQuery query;
//    public int cached = 0; //BY default, all blocks are considered to be not Cached
//    public String cachedFogIp;
}
