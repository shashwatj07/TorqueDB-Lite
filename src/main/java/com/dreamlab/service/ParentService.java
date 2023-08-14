package com.dreamlab.service;

import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.MembershipServerGrpc;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.SetParentFogRequest;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ParentService extends ParentServerGrpc.ParentServerImplBase {

    private final Logger LOGGER;

    private final UUID fogId;

    private final List<UUID> fogIds;

    private final Map<UUID, FogInfo> fogDetails;

    private final Map<UUID, MembershipServerGrpc.MembershipServerBlockingStub> membershipStubs;

    public ParentService(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        LOGGER = Logger.getLogger(String.format("[Fog: %s] ", fogId.toString()));
        this.fogId = fogId;
        this.fogDetails = fogDetails;
        fogIds = new ArrayList<>(fogDetails.keySet());
        Collections.sort(fogIds);
        membershipStubs = new HashMap<>();
    }

    @Override
    public void sendHeartbeat(HeartbeatRequest request, StreamObserver<Response> responseObserver) {
        LOGGER.info(LOGGER.getName() + "Heartbeat Received From: " + Utils.getUuidFromMessage(request.getEdgeId()));
        final long start = System.currentTimeMillis();
        SetParentFogRequest.Builder builder = SetParentFogRequest.newBuilder();
        SetParentFogRequest setParentFogRequest = builder
                .setEdgeId(request.getEdgeId())
                .setFogId(Utils.getMessageFromUUID(fogId))
                .setHeartbeatTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
                .setTtlSecs(request.getTtlSecs())
                .build();
        List<Integer> membershipFogIndices = Utils.getMembershipFogIndices(Utils.getUuidFromMessage(request.getEdgeId()), fogIds.size());
        membershipFogIndices.forEach(index -> sendParentInfoToMembershipFog(fogIds.get(index), setParentFogRequest));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner] ParentServer.sendHeartbeat: %d", LOGGER.getName(), (end - start)));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void sendParentInfoToMembershipFog(UUID membershipFogId, SetParentFogRequest setParentFogRequest) {
        synchronized (membershipStubs) {
            if (!membershipStubs.containsKey(membershipFogId)) {
                FogInfo membershipFogInfo = fogDetails.get(membershipFogId);
                ManagedChannel managedChannel = ManagedChannelBuilder
                        .forAddress(String.valueOf(membershipFogInfo.getDeviceIP()), membershipFogInfo.getDevicePort())
                        .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                        .build();
                MembershipServerGrpc.MembershipServerBlockingStub membershipServerBlockingStub = MembershipServerGrpc.newBlockingStub(managedChannel);
                membershipStubs.put(membershipFogId, membershipServerBlockingStub);
            }
        }
        final long start = System.currentTimeMillis();
        Response response = membershipStubs.get(membershipFogId).setParentFog(setParentFogRequest);
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] MembershipServer.setParentFog: %d", LOGGER.getName(), (end - start)));
    }
}
