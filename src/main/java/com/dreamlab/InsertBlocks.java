package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class InsertBlocks {
    private InsertBlocks() {
    }

    public static void main(String... args) throws IOException, InterruptedException {
        final String edgeIp = Inet4Address.getLocalHost().getHostAddress();
        final int edgePort = Integer.parseInt(args[0]);
        final String blocksDirectory = args[1];
        final String metadataDirectory = args[2];
        final File blocksDir = new File(blocksDirectory);
        final File metadataDir = new File(metadataDirectory);
        final int interval = Integer.parseInt(args[3]);
        final List<String> blocks = Arrays.stream(blocksDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        final List<String> metadata = Arrays.stream(metadataDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        Collections.sort(blocks);
        Collections.sort(metadata);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(edgeIp, edgePort)
                .usePlaintext()
                .build();
        EdgeServerGrpc.EdgeServerBlockingStub edgeServerBlockingStub = EdgeServerGrpc.newBlockingStub(managedChannel);
        for (int i = 0; i < blocks.size(); i++) {
            String blockFilePath = blocks.get(i);
            String blockId = new File(blockFilePath).getName().substring(5, 41);
            PutBlockAndMetadataRequest putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                    .newBuilder()
                    .setBlockContent(Utils.getBytes(blockFilePath))
                    .setMetadataContent(Utils.getBytes(metadata.get(i)))
                    .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
                    .build();
            BlockIdResponse blockIdResponse = edgeServerBlockingStub
                    .putBlockAndMetadata(putBlockAndMetadataRequest);

            System.out.println("Success: " + Utils.getUuidFromMessage(blockIdResponse.getBlockId()));
            Thread.sleep(interval * 1000);
        }
        managedChannel.shutdown();
    }
}
