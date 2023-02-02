package com.dreamlab.service;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Heartbeat;
import com.dreamlab.utils.LocationHandler;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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

    private final Logger LOGGER;

    public EdgeService(UUID id, String ip, int port, int heartbeatTtlSecs, String fogsConfigFilePath, String trajectoryFilePath) {
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
        LOGGER = Logger.getLogger(String.format("[Edge: %s] ", id));
    }

    @Override
    public void putBlockAndMetadata(PutBlockAndMetadataRequest request, StreamObserver<BlockIdResponse> responseObserver) {
        final long startInner = System.currentTimeMillis();
        UUIDMessage blockId = request.getBlockId();
        UUID blockUuid = Utils.getUuidFromMessage(blockId);
        PutBlockRequest.Builder putBlockRequestBuilder = PutBlockRequest.newBuilder();
        putBlockRequestBuilder.setBlockId(blockId);
        putBlockRequestBuilder.setBlockContent(request.getBlockContent());
        putBlockRequestBuilder.setMetadataContent(request.getMetadataContent());

        PutMetadataRequest.Builder putMetadataRequestBuilder = PutMetadataRequest.newBuilder();
        putMetadataRequestBuilder.setBlockId(blockId);
        putMetadataRequestBuilder.setMetadataContent(request.getMetadataContent());

        final FogInfo parentFogInfo = Utils.getParentFog(fogDetails, getLatitude(), getLongitude());

        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = getCoordinatorStub(parentFogInfo.getDeviceId());
        final long t1 = System.currentTimeMillis();
        Response putBlockResponse = coordinatorServerBlockingStub
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .putBlockByMetadata(putBlockRequestBuilder.build());
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.putBlockByMetadata: %d", LOGGER.getName(), blockUuid, (t2 - t1)));
        final long t3 = System.currentTimeMillis();
        Response putMetadataResponse = coordinatorServerBlockingStub
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .putMetadata(putMetadataRequestBuilder.build());
        final long t4 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.putMetadata: %d", LOGGER.getName(), blockUuid, (t4 - t3)));
        BlockIdResponse.Builder blockIdResponseBuilder = BlockIdResponse.newBuilder();
        blockIdResponseBuilder.setBlockId(blockId);
        blockIdResponseBuilder.setIsSuccess(putBlockResponse.getIsSuccess() && putMetadataResponse.getIsSuccess());

        final long endInner = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] EdgeServer.putBlockAndMetadata: %d", LOGGER.getName(), blockUuid, (endInner - startInner)));
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
