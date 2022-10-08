import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.UUID;

public class Heartbeat implements Runnable {
    
    private final HeartbeatRequest heartbeatRequest;
    
    private ParentServerGrpc.ParentServerBlockingStub parentServerBlockingStub;

//    private boolean exit = false;

    public Heartbeat(UUID edgeId, String parentFogIp, int parentFogPort, int ttlSecs) {
        heartbeatRequest = HeartbeatRequest.newBuilder().setEdgeId(Utils.getMessageFromUUID(edgeId)).setTtlSecs(ttlSecs).build();
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentFogIp, parentFogPort)
                .usePlaintext()
                .build();
        parentServerBlockingStub = ParentServerGrpc.newBlockingStub(managedChannel);
    }

    public void updateParentFog(String parentFogIp, int parentFogPort) {
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentFogIp, parentFogPort)
                .usePlaintext()
                .build();
        parentServerBlockingStub = ParentServerGrpc.newBlockingStub(managedChannel);
    }

    @Override
    public void run() {
        try {
            while (true) {
                parentServerBlockingStub.sendHeartbeat(heartbeatRequest);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public void stop()
//    {
//        exit = true;
//    }
}
