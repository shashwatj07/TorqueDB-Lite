package com.dreamlab.utils;

import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.service.EdgeService;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Heartbeat implements Runnable {

    private final EdgeService edgeService;

    private final Map<UUID, ParentServerGrpc.ParentServerBlockingStub> stubs;

    private final HeartbeatRequest heartbeatRequest;

    private UUID parentFogId;

    private UUID edgeId;

    private final int ttlSecs;

    private final Map<UUID, FogInfo> fogDetails;

    private final Logger LOGGER;

    public Heartbeat(EdgeService edgeService, UUID edgeId, int ttlSecs, Map<UUID, FogInfo> fogDetails) {
        LOGGER = Logger.getLogger(String.format("[Edge: %s] ", edgeId.toString()));
        this.edgeService = edgeService;
        this.ttlSecs = ttlSecs;
        this.edgeId = edgeId;
        stubs = new HashMap<>();
        heartbeatRequest = HeartbeatRequest.newBuilder().setEdgeId(Utils.getMessageFromUUID(edgeId)).setTtlSecs(ttlSecs).build();
        this.fogDetails = fogDetails;
    }

    private void updateParentFog() {
        FogInfo parentFogInfo = Utils.getParentFog(fogDetails, edgeService.getLatitude(), edgeService.getLongitude());
        parentFogId = parentFogInfo.getDeviceId();
        if(!stubs.containsKey(parentFogId)) {
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(parentFogInfo.getDeviceIP(), parentFogInfo.getDevicePort())
                    .usePlaintext()
                    .build();
            stubs.put(parentFogId, ParentServerGrpc.newBlockingStub(managedChannel));
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000L * ttlSecs);
                updateParentFog();
                stubs.get(parentFogId).sendHeartbeat(heartbeatRequest);
                LOGGER.info(String.format("%sCurrent Location (%f, %f)", LOGGER.getName(), edgeService.getLatitude(), edgeService.getLongitude()));
                LOGGER.info(LOGGER.getName() + "Heartbeat Sent To: " + parentFogId);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, LOGGER.getName() + e.getMessage(), e);
            }
        }
    }
}
