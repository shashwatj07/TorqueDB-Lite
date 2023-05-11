package com.dreamlab.utils;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.edgefs.grpcServices.BlockReplica;
import com.dreamlab.types.ExecPlan;
import com.dreamlab.types.FogPartition;
import com.influxdb.client.domain.Run;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public final class CostModel {
    public static List<ExecPlan> QP1(HashSet<BlockIdReplicaMetadata> blockIdReplicaMetadataSet, Map<UUID, FogPartition> fogPartitions) {
        // random
        List<ExecPlan> execPlanList = new ArrayList<>();
        for (BlockIdReplicaMetadata blockIdReplicaMetadata : blockIdReplicaMetadataSet) {
            List<BlockReplica> activeReplicas = blockIdReplicaMetadata.getReplicasList().stream()
                    .filter(replica -> fogPartitions.get(Utils.getUuidFromMessage(replica.getDeviceId())).isActive())
                    .collect(Collectors.toList());
            if (activeReplicas.size() == 0) {
                throw new RuntimeException("No Available Replica for " + Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()));
            }
            BlockReplica replica = activeReplicas.get(Constants.RANDOM.nextInt(activeReplicas.size()));
            execPlanList.add(new ExecPlan(Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()),
                    Utils.getUuidFromMessage(replica.getDeviceId())));
        }
        return execPlanList;
    }
}
