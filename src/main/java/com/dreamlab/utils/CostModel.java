package com.dreamlab.utils;

import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.types.ExecPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public final class CostModel {
    public static List<ExecPlan> QP1(HashSet<BlockIdReplicaMetadata> blockIdReplicaMetadataSet) {
        // TODO
        List<ExecPlan> execPlanList = new ArrayList<>();
        for (BlockIdReplicaMetadata blockIdReplicaMetadata : blockIdReplicaMetadataSet) {
            execPlanList.add(new ExecPlan(Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()),
                    Utils.getUuidFromMessage(blockIdReplicaMetadata.getReplicas(0).getDeviceId())));
        }
        return execPlanList;
    }
}
