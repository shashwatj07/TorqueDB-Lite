package com.dreamlab.service;

import com.dreamlab.utils.Heartbeat;
import com.dreamlab.utils.LocationHandler;
import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class EdgeService extends EdgeServerGrpc.EdgeServerImplBase {

    private volatile double latitude;

    private volatile double longitude;

    private final String ip;

    private final UUID id;

    private final int port;

    private final int heartbeatTtlSecs;

    private final Thread heartbeatThread;

    private final Map<UUID, FogInfo> fogDetails;

    private final Map<UUID, CoordinatorServerGrpc.CoordinatorServerBlockingStub> coordinatorStubs;

    public EdgeService(UUID id, String ip, int port, int heartbeatTtlSecs, String fogsConfigFilePath, String trajectoryFilePath) throws IOException {
        this.id  = id;
        this.ip = ip;
        this.port = port;
        this.heartbeatTtlSecs = heartbeatTtlSecs;
        fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        Runnable locationHandler = new LocationHandler(this, id, heartbeatTtlSecs, trajectoryFilePath);
        Thread locationThread = new Thread(locationHandler);
        locationThread.start();
        Runnable heartbeat = new Heartbeat(this, id, heartbeatTtlSecs, fogDetails);
        heartbeatThread = new Thread(heartbeat);
        heartbeatThread.start();
        coordinatorStubs = new HashMap<>();
    }

    @Override
    public void putBlockAndMetadata(PutBlockAndMetadataRequest request, StreamObserver<BlockIdResponse> responseObserver) {
        UUIDMessage blockId = Utils.getMessageFromUUID(UUID.randomUUID());
        PutBlockRequest.Builder putBlockRequestBuilder = PutBlockRequest.newBuilder();
        putBlockRequestBuilder.setBlockId(blockId);
        putBlockRequestBuilder.setBlockContent(request.getBlockContent());
        putBlockRequestBuilder.setMetadataContent(request.getMetadataContent());

        PutMetadataRequest.Builder putMetadataRequestBuilder = PutMetadataRequest.newBuilder();
        putMetadataRequestBuilder.setBlockId(blockId);
        putMetadataRequestBuilder.setMetadataContent(request.getMetadataContent());

        final FogInfo parentFogInfo = Utils.getParentFog(fogDetails, getLatitude(), getLongitude());

        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = getCoordinatorStub(parentFogInfo.getDeviceId());
        Response putBlockResponse = coordinatorServerBlockingStub.putBlockByMetadata(putBlockRequestBuilder.build());
        Response putMetadataResponse = coordinatorServerBlockingStub.putMetadata(putMetadataRequestBuilder.build());
        BlockIdResponse.Builder blockIdResponseBuilder = BlockIdResponse.newBuilder();
        blockIdResponseBuilder.setBlockId(blockId);
        blockIdResponseBuilder.setIsSuccess(putBlockResponse.getIsSuccess() && putMetadataResponse.getIsSuccess());

        responseObserver.onNext(blockIdResponseBuilder.build());
        responseObserver.onCompleted();
    }

    private CoordinatorServerGrpc.CoordinatorServerBlockingStub getCoordinatorStub(UUID fogId) {
        synchronized (coordinatorStubs) {
            if (!coordinatorStubs.containsKey(fogId)) {
                FogInfo fogInfo = fogDetails.get(fogId);
                ManagedChannel managedChannel = ManagedChannelBuilder
                        .forAddress(String.valueOf(fogInfo.getDeviceIP()), fogInfo.getDevicePort())
                        .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                        .build();
                CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
                coordinatorStubs.put(fogId, coordinatorServerBlockingStub);
            }
        }
        return coordinatorStubs.get(fogId);
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
