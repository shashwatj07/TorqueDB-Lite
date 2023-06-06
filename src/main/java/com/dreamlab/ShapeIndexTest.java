package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.DataServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlocksResponse;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ShapeIndexTest {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    private ShapeIndexTest() {
    }

    public static void main(String... args) throws IOException {
        String ip = args[0];
        int port = Integer.parseInt(args[1]);
        double minLat = Double.parseDouble(args[2]);
        double maxLat = Double.parseDouble(args[3]);
        double minLon = Double.parseDouble(args[4]);
        double maxLon = Double.parseDouble(args[5]);

        BoundingBox boundingBox = BoundingBox.newBuilder()
                .setTopLeftLatLon(Point.newBuilder()
                    .setLatitude(maxLat)
                    .setLongitude(minLon)
                    .build())
                .setBottomRightLatLon(Point.newBuilder()
                    .setLatitude(minLat)
                    .setLongitude(maxLon)
                    .build())
                .build();

        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(ip, port)
                .usePlaintext()
                .build();
        DataServerGrpc.DataServerBlockingStub dataServerBlockingStub = DataServerGrpc.newBlockingStub(managedChannel);

        FindBlocksResponse findBlocksResponse = dataServerBlockingStub.testSpatialIndex(boundingBox);

        System.out.println("Response: " + findBlocksResponse.getBlockIdReplicasMetadataList().stream().map(i -> Utils.getUuidFromMessage(i.getBlockId())).collect(Collectors.toList()));
    }
}