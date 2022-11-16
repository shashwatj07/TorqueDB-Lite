package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlocksRequest;
import com.dreamlab.edgefs.grpcServices.FindBlocksResponse;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.UUID;

public final class FindBlocksClient {
    private FindBlocksClient() {
    }

    public static void main(String... args) {
        final String fogIp = args[0];
        final int fogPort = Integer.parseInt(args[1]);
        final String blockId = args[2];
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(fogIp, fogPort)
                .usePlaintext()
                .build();
        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
        FindBlocksRequest findBlocksRequest = FindBlocksRequest
                .newBuilder()
                .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
                .setIsAndQuery(false)
                .build();
        FindBlocksResponse findBlocksResponse = coordinatorServerBlockingStub
                .findBlocks(findBlocksRequest);
        managedChannel.shutdown();
        System.out.println("Success: " + findBlocksResponse.toString());
    }
}
