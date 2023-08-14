package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlocksRequest;
import com.dreamlab.edgefs.grpcServices.FindBlocksResponse;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public final class FindBlocksClient {
    private FindBlocksClient() {
    }

    public static void main(String... args) {
        final String fogIp = args[0];
        final int fogPort = Integer.parseInt(args[1]);
        final String blockId = args[2];
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(fogIp, fogPort)
                .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                .build();
        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
        FindBlocksRequest findBlocksRequest = FindBlocksRequest
                .newBuilder()
//                .setBlockId(Utils.getMessageFromUUID(UUID.fromString(blockId)))
//                .setBoundingBox(BoundingBox.newBuilder()
//                        .setTopLeftLatLon(Point.newBuilder().setLatitude(0.00006).setLongitude(-0.00005).build())
//                        .setBottomRightLatLon(Point.newBuilder().setLatitude(0.00001).setLongitude(0).build())
//                        .build())
                .setTimeRange(TimeRange.newBuilder()
                        .setStartTimestamp(Utils.getTimestampMessageFromInstant(Utils.getInstantFromString("2022-10-28 23-33-35")))
                        .setEndTimestamp(Utils.getTimestampMessageFromInstant(Utils.getInstantFromString("2022-10-29 22-03-35")))
                        .build())
                .putMetadataMap("p1", "1")
                .putMetadataMap("p2", "1")
                .putMetadataMap("p3", "2")
                .setIsAndQuery(true)
                .build();
        FindBlocksResponse findBlocksResponse = coordinatorServerBlockingStub
                .findBlocks(findBlocksRequest);
        managedChannel.shutdown();
        System.out.println("Success: " + findBlocksResponse.getBlockIdReplicasMetadataList().size());
    }
}
