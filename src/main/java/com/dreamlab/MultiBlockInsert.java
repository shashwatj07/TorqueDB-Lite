package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MultiBlockInsert {

    private MultiBlockInsert() {
    }

    public static void main(String[] args) {
        int numEdges = Integer.parseInt(args[0]);
        int numBlocksPerEdge = Integer.parseInt(args[1]);
        String edgeIp = args[2];
        int edgePort = Integer.parseInt(args[3]);
        int delay = Integer.parseInt(args[4]);
        String blockPath = args[5];
        String metadataDirPath = args[6];

        for (int edgeNum = 1; edgeNum <= numEdges; edgeNum++, edgePort++) {
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(edgeIp, edgePort)
                    .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                    .build();
            EdgeServerGrpc.EdgeServerBlockingStub edgeServerBlockingStub = EdgeServerGrpc.newBlockingStub(managedChannel);
            final int finalEdgeNum = edgeNum;
            Runnable batchInsert = () ->
            {
                for (int blockNum = 1; blockNum <= numBlocksPerEdge; blockNum++) {
                    PutBlockAndMetadataRequest putBlockAndMetadataRequest;
                    try {
                        putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                                .newBuilder()
                                .setBlockContent(Utils.getBytes(blockPath))
                                .setMetadataContent(Utils.getBytes(String.format("%s/edge_%d_block_%d.json", metadataDirPath, finalEdgeNum, blockNum)))
                                .build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    BlockIdResponse blockIdResponse = edgeServerBlockingStub
                            .putBlockAndMetadata(putBlockAndMetadataRequest);
                    System.out.println(String.format("[Edge %d Block %d] Success: %s", finalEdgeNum, blockNum, Utils.getUuidFromMessage(blockIdResponse.getBlockId())));
                    try {
                        Thread.sleep(1000 * delay);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                managedChannel.shutdown();
            };
            Thread thread = new Thread(batchInsert);
            thread.start();
        }
    }
}
