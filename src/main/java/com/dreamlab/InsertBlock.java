package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class InsertBlock {
    private InsertBlock() {
    }

    public static void main(String... args) throws IOException {
        final String blockFilePath = args[0];
        final String metadataFilePath = args[1];
        final String fogId = args[2];
        final String fogConfigPath = args[3];
        String blockId = new File(blockFilePath).getName().substring(5, 41);
        System.out.println(blockId);

        PutBlockAndMetadataRequest putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                .newBuilder()
                .setBlockContent(Utils.getBytes(blockFilePath))
                .setMetadataContent(Utils.getBytes(metadataFilePath))
                .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
                .build();
        BlockIdResponse blockIdResponse = putBlockAndMetadata(putBlockAndMetadataRequest, fogId, fogConfigPath);
        System.out.println("Success: " + Utils.getUuidFromMessage(blockIdResponse.getBlockId()));
    }

    static BlockIdResponse putBlockAndMetadata(PutBlockAndMetadataRequest request, String fogId, String path) {
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

        final Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(path);
        final FogInfo parentFogInfo = fogDetails.get(UUID.fromString(fogId));
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentFogInfo.getDeviceIP(), parentFogInfo.getDevicePort())
                .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                .build();
        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);

        final long t1 = System.currentTimeMillis();
        Response putBlockResponse = coordinatorServerBlockingStub
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .putBlockByMetadata(putBlockRequestBuilder.build());
        final long t2 = System.currentTimeMillis();
        System.out.println(String.format("%s[Outer %s] CoordinatorServer.putBlockByMetadata: %d", "Client ", blockUuid, (t2 - t1)));
        final long t3 = System.currentTimeMillis();
        Response putMetadataResponse = coordinatorServerBlockingStub
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .putMetadata(putMetadataRequestBuilder.build());
        final long t4 = System.currentTimeMillis();
        System.out.println(String.format("%s[Outer %s] CoordinatorServer.putMetadata: %d", "Client ", blockUuid, (t4 - t3)));
        BlockIdResponse.Builder blockIdResponseBuilder = BlockIdResponse.newBuilder();
        blockIdResponseBuilder.setBlockId(blockId);
        blockIdResponseBuilder.setIsSuccess(putBlockResponse.getIsSuccess() && putMetadataResponse.getIsSuccess());

        final long endInner = System.currentTimeMillis();
        System.out.println(String.format("%s[Inner %s] EdgeServer.putBlockAndMetadata: %d", "Client ", blockUuid, (endInner - startInner)));
        return blockIdResponseBuilder.build();
    }
}
