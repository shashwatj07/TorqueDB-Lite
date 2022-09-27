import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class BlockReplicaInfo {
    private final UUID blockID;
    private final List<DeviceInfo> replicaLocations;

    public BlockReplicaInfo(UUID blockID) {
        this.blockID = blockID;
        replicaLocations = new ArrayList<>();
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

}
