package com.dreamlab.utils;

import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.service.EdgeService;
import com.dreamlab.types.FogInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Heartbeat implements Runnable {

    private final EdgeService edgeService;

    private final Map<UUID, ParentServerGrpc.ParentServerFutureStub> stubs;

    private final HeartbeatRequest heartbeatRequest;

    private UUID parentFogId;

    private UUID edgeId;

    private final double ttlSecs;

    private final Map<UUID, FogInfo> fogDetails;

    private final Logger LOGGER;

    public Heartbeat(EdgeService edgeService, UUID edgeId, double ttlSecs, Map<UUID, FogInfo> fogDetails) {
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
                    .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                    .build();
            stubs.put(parentFogId, ParentServerGrpc.newFutureStub(managedChannel));
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                updateParentFog();
                final long start = System.currentTimeMillis();
                stubs.get(parentFogId).sendHeartbeat(heartbeatRequest);
//                final long end = System.currentTimeMillis();
//                LOGGER.info(String.format("%s[Outer] ParentServer.sendHeartbeat: %d", LOGGER.getName(), (end - start)));
                LOGGER.info(String.format("%sCurrent Location (%f, %f)", LOGGER.getName(), edgeService.getLatitude(), edgeService.getLongitude()));
                LOGGER.info(LOGGER.getName() + "Heartbeat Sent To: " + parentFogId);
                final long sleepTime = (long) (1000L * ttlSecs - (System.currentTimeMillis() - start));
                Thread.sleep(sleepTime > 0 ? sleepTime : 0);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, LOGGER.getName() + e.getMessage(), e);
            }
        }
    }
}
