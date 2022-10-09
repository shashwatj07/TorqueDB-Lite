import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Heartbeat implements Runnable {
    
    private final HeartbeatRequest heartbeatRequest;
    
    private ParentServerGrpc.ParentServerBlockingStub parentServerBlockingStub;

    private UUID parentFogId;

    private UUID edgeId;

    private final int ttlSecs;

    private final Map<UUID, FogInfo> fogDetails;

    private final Logger LOGGER;

    public Heartbeat(UUID edgeId, int ttlSecs, Map<UUID, FogInfo> fogDetails) {
        LOGGER = Logger.getLogger(String.format("[Edge: %s] ", edgeId.toString()));
        this.ttlSecs = ttlSecs;
        this.edgeId = edgeId;
        heartbeatRequest = HeartbeatRequest.newBuilder().setEdgeId(Utils.getMessageFromUUID(edgeId)).setTtlSecs(ttlSecs).build();
        this.fogDetails = fogDetails;
    }

    private void updateParentFog() {
        FogInfo parentFogInfo = Utils.getParentFog(fogDetails, 1.5, 1.5);
        parentFogId = parentFogInfo.getDeviceId();
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentFogInfo.getDeviceIP(), parentFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        parentServerBlockingStub = ParentServerGrpc.newBlockingStub(managedChannel);
    }

    @Override
    public void run() {
        while (true) {
            try {
                updateParentFog();
                parentServerBlockingStub.sendHeartbeat(heartbeatRequest);
                LOGGER.info(LOGGER.getName() + "Heartbeat Sent To: " + parentFogId);
                Thread.sleep(1000L * ttlSecs);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, LOGGER.getName() + e.getMessage(), e);
            }
        }
    }
}
