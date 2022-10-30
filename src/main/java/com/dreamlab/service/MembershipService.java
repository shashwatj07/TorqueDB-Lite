package com.dreamlab.service;

import com.dreamlab.types.FogInfo;
import com.dreamlab.types.MembershipInfo;
import com.dreamlab.utils.Utils;
import com.dreamlab.edgefs.grpcServices.GetParentFogRequest;
import com.dreamlab.edgefs.grpcServices.GetParentFogResponse;
import com.dreamlab.edgefs.grpcServices.MembershipServerGrpc;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.SetParentFogRequest;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MembershipService extends MembershipServerGrpc.MembershipServerImplBase {

    private final Map<UUID, MembershipInfo> membershipMap;

    private final Map<UUID, FogInfo> fogDetails;

    public MembershipService(Map<UUID, FogInfo> fogDetails) {
        membershipMap = new HashMap<>();
        this.fogDetails = fogDetails;
    }

    @Override
    public void setParentFog(SetParentFogRequest request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        UUID edgeId = Utils.getUuidFromMessage(request.getEdgeId());
        UUID parentFogId = Utils.getUuidFromMessage(request.getFogId());
        if (!membershipMap.containsKey(edgeId)
                || membershipMap.get(edgeId).getLastHeartbeat().isAfter(Utils.getInstantFromTimestampMessage(request.getHeartbeatTimestamp()))) {
            membershipMap.put(Utils.getUuidFromMessage(request.getEdgeId()),
                    new MembershipInfo(parentFogId,
                            Utils.getInstantFromTimestampMessage(request.getHeartbeatTimestamp()),
                            request.getTtlSecs()));
        }
        responseBuilder.setIsSuccess(true);
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getParentFog(GetParentFogRequest request, StreamObserver<GetParentFogResponse> responseObserver) {
        GetParentFogResponse.Builder responseBuilder = GetParentFogResponse.newBuilder();
        try {
            MembershipInfo membershipInfo = membershipMap.get(Utils.getUuidFromMessage(request.getEdgeId()));
            UUID parentFogId = membershipInfo.getParentFogId();
            FogInfo parentFogInfo = fogDetails.get(parentFogId);
            responseBuilder
                   .setIsSuccess(true)
                   .setParentFogId(Utils.getMessageFromUUID(parentFogId))
                   .setTtlSecs(membershipInfo.getTtlSecs())
                   .setHeartbeatTimestamp(Utils.getTimestampMessageFromInstant(membershipInfo.getLastHeartbeat()))
                   .setIp(parentFogInfo.getDeviceIP())
                   .setPort(parentFogInfo.getDevicePort());
        } catch (Exception ex) {
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
