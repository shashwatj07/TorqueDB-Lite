package com.dreamlab.utils;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.edgefs.grpcServices.BlockReplica;
import com.dreamlab.types.ExecPlan;
import com.dreamlab.types.FogPartition;
import com.influxdb.client.domain.Run;

import java.util.*;
import java.util.stream.Collectors;

public final class CostModel {
    public static List<ExecPlan> QP1(HashSet<BlockIdReplicaMetadata> blockIdReplicaMetadataSet, Map<UUID, FogPartition> fogPartitions, UUID fogId) {
        // random
        List<ExecPlan> execPlanList = new ArrayList<>();
        for (BlockIdReplicaMetadata blockIdReplicaMetadata : blockIdReplicaMetadataSet) {
            List<BlockReplica> activeReplicas = blockIdReplicaMetadata.getReplicasList().stream()
                    .filter(replica -> fogPartitions.get(Utils.getUuidFromMessage(replica.getDeviceId())).isActive())
                    .collect(Collectors.toList());
            if (activeReplicas.size() == 0) {
                throw new RuntimeException("No Available Replica for " + Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()));
            }
            if (activeReplicas.stream().anyMatch(blockReplica -> Utils.getUuidFromMessage(blockReplica.getDeviceId()).equals(fogId))) {
                execPlanList.add(new ExecPlan(Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()),
                        fogId));
            }
            else {
                BlockReplica replica = activeReplicas.get(Constants.RANDOM.nextInt(activeReplicas.size()));
                execPlanList.add(new ExecPlan(Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId()),
                        Utils.getUuidFromMessage(replica.getDeviceId())));
            }
        }
        return execPlanList;
    }

    static class FrequencyComparator implements Comparator<Map.Entry<UUID, Set<UUID>>> {
        @Override
        public int compare(Map.Entry<UUID, Set<UUID>> entry1,
                           Map.Entry<UUID, Set<UUID>> entry2) {
            return Integer.compare(entry2.getValue().size(), entry1.getValue().size());
        }
    }

    public static List<ExecPlan> QP2(HashSet<BlockIdReplicaMetadata> blockIdReplicaMetadataSet, Map<UUID, FogPartition> fogPartitions, UUID fogId) {
        // minimize the number of blocks per fog

        Map<UUID, UUID> mapping = new HashMap<>();
        Map<UUID, Set<UUID>> fogs = new HashMap<>();
        for (BlockIdReplicaMetadata blockIdReplicaMetadata : blockIdReplicaMetadataSet) {
            UUID blockId = Utils.getUuidFromMessage(blockIdReplicaMetadata.getBlockId());
            for (BlockReplica blockReplica : blockIdReplicaMetadata.getReplicasList()) {
                UUID replicaFogId = Utils.getUuidFromMessage(blockReplica.getDeviceId());
                if (!fogs.containsKey(replicaFogId)) {
                    fogs.put(replicaFogId, new HashSet<>());
                }
                fogs.get(replicaFogId).add(blockId);
            }
        }

        LinkedHashMap<UUID, Set<UUID>> sortedMap = fogs.entrySet().stream()
                .sorted(new FrequencyComparator())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        List<UUID> fogsList = new ArrayList<>(sortedMap.keySet());

        Map<UUID, Integer> assignedBlocksCount = new HashMap<>();

        for (UUID fog : fogsList) {
            assignedBlocksCount.put(fog, 0);
        }

        while(mapping.size() < blockIdReplicaMetadataSet.size()) {
            for (UUID fog : fogsList) {
                UUID selectedBlockId = null;
                for (UUID blockId : sortedMap.get(fog)) {
                    if (!mapping.containsKey(blockId)) {
                        selectedBlockId = blockId;
                        break;
                    }
                }
                if (selectedBlockId == null) {
                    continue;
                }

                int min = Integer.MAX_VALUE;
                for (UUID fogCandidate : fogsList) {
                    min = Math.min(assignedBlocksCount.get(fogCandidate), min);
                }

                Set<UUID> fogCandidates = new HashSet<>();
                for (UUID fogCandidate : fogsList) {
                    if (assignedBlocksCount.get(fogCandidate) == min) {
                        fogCandidates.add(fogCandidate);
                    }
                }

                UUID assignedFogId = null;
                if (fogCandidates.contains(fog)) {
                    assignedFogId = fog;
                }
                else {
                    for (UUID assignmentCandidate : fogsList) {
                        if (fogCandidates.contains(assignmentCandidate)) {
                            assignedFogId = assignmentCandidate;
                            break;
                        }
                    }
                }
                mapping.put(selectedBlockId, assignedFogId);
                assignedBlocksCount.put(assignedFogId, assignedBlocksCount.get(assignedFogId) + 1);

                for (UUID fogElement : fogsList) {
                    sortedMap.get(fogElement).remove(selectedBlockId);
                }
            }
        }

        List<ExecPlan> execPlanList = new ArrayList<>();

        for (UUID blockId : mapping.keySet()) {
            execPlanList.add(new ExecPlan(blockId, mapping.get(blockId)));
        }

        return execPlanList;
    }
}
