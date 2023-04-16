package com.dreamlab.types;

import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class BlockReplicaInfo implements Serializable {

    private static final long serialVersionUID = -8885818812042252438L;
    private final UUID blockID;
    private final List<DeviceInfo> replicaLocations;
    private final int blockIdHash;

    public BlockReplicaInfo(UUID blockID) {
        this.blockID = blockID;
        this.blockIdHash = blockID.hashCode();
        replicaLocations = new ArrayList<>();
    }

    public BlockIdReplicaMetadata toMessage() {
        return BlockIdReplicaMetadata.newBuilder()
                .setBlockId(Utils.getMessageFromUUID(this.blockID))
                .addAllReplicas(replicaLocations.stream().map(DeviceInfo::toMessage).collect(Collectors.toList()))
                .build();
    }

    public void addReplicaLocation(DeviceInfo replicaLocation) {
        replicaLocations.add(replicaLocation);
    }

    public UUID getBlockID() {
        return blockID;
    }

    public List<DeviceInfo> getReplicaLocations() {
        return Collections.unmodifiableList(replicaLocations);
    }

    @Override
    public String toString() {
        return "BlockReplicaInfo{" +
                "blockID=" + blockID +
                ", replicaLocations=" + replicaLocations +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlockReplicaInfo that = (BlockReplicaInfo) o;
        return blockID.equals(that.blockID);
    }

    @Override
    public int hashCode() {
        return blockIdHash;
    }
}
