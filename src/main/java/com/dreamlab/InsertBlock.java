package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public final class InsertBlock {
    private InsertBlock() {
    }

    public static void main(String... args) throws IOException {
        final String edgeIp = args[0];
        final int edgePort = Integer.parseInt(args[1]);
        final String blockFilePath = args[2];
        final String metadataFilePath = args[3];
        String blockId = new File(blockFilePath).getName().substring(5, 41);
        System.out.println(blockId);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(edgeIp, edgePort)
                .usePlaintext()
                .build();
        EdgeServerGrpc.EdgeServerBlockingStub edgeServerBlockingStub = EdgeServerGrpc.newBlockingStub(managedChannel);
        PutBlockAndMetadataRequest putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                .newBuilder()
                .setBlockContent(Utils.getBytes(blockFilePath))
                .setMetadataContent(Utils.getBytes(metadataFilePath))
                .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
                .build();
        BlockIdResponse blockIdResponse = edgeServerBlockingStub
                .putBlockAndMetadata(putBlockAndMetadataRequest);
        managedChannel.shutdown();
        System.out.println("Success: " + Utils.getUuidFromMessage(blockIdResponse.getBlockId()));
    }
}
