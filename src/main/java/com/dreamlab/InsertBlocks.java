package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public final class InsertBlocks {

    static final Logger LOGGER = Logger.getLogger("[Client] ");
    
    private InsertBlocks() {
    }

    public static void main(String... args) {
        final String edgeIp = System.getenv("device_ip");
        LOGGER.info("Inserting blocks on " + edgeIp);
        final int edgePort = Integer.parseInt(args[0]);
        final String blocksDirectory = args[1];
        final String metadataDirectory = args[2];
        final File blocksDir = new File(blocksDirectory);
        final File metadataDir = new File(metadataDirectory);
        final double interval = Double.parseDouble(args[3]);
        final List<String> blocks = Arrays.stream(blocksDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        final List<String> metadata = Arrays.stream(metadataDir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
        Collections.sort(blocks);
        Collections.sort(metadata);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(edgeIp, edgePort)
                .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                .build();
        EdgeServerGrpc.EdgeServerBlockingStub edgeServerBlockingStub = EdgeServerGrpc.newBlockingStub(managedChannel);
        for (int i = 0; i < blocks.size(); i++) {
            String blockFilePath = blocks.get(i);
            String blockId = new File(blockFilePath).getName().substring(5, 41);
            try {
                LOGGER.info("Inserting: " + blockId);
                PutBlockAndMetadataRequest putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                        .newBuilder()
                        .setBlockContent(Utils.getBytes(blockFilePath))
                        .setMetadataContent(Utils.getBytes(metadata.get(i)))
                        .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
                        .build();
                final long start = System.currentTimeMillis();
                BlockIdResponse blockIdResponse = edgeServerBlockingStub
                        .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                        .putBlockAndMetadata(putBlockAndMetadataRequest);
                final long end = System.currentTimeMillis();
                LOGGER.info(String.format("[Client] [Outer %s] EdgeServer.putBlockAndMetadata: %d", blockId, (end - start)));
                LOGGER.info("Success: " + Utils.getUuidFromMessage(blockIdResponse.getBlockId()));
                final long sleepTime = (long) (interval * 1000L - (end - start));
                Thread.sleep(sleepTime >= 0? sleepTime : 0);
            }
            catch (Exception ex) {
                LOGGER.info("Failed to insert block");
                ex.printStackTrace();
            }
        }
        managedChannel.shutdown();
    }
}
