package com.dreamlab.server;

import com.dreamlab.service.EdgeService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.UUID;

public class MultiEdgeServer {

    private MultiEdgeServer() {
    }

    public static void main(String... args) throws IOException {
        int numEdges = Integer.parseInt(args[0]);
        final String edgeIp = args[1];
        int edgePort = Integer.parseInt(args[2]);
        final int heartbeatTtlSecs = Integer.parseInt(args[3]);
        final String fogsConfigFilePath = args[4];
        final String trajectoryDirPath = args[5];
        for (int edgeCount = 1; edgeCount <= numEdges; edgeCount++) {
            final UUID edgeId = UUID.randomUUID();
            final String trajectoryFilePath = String.format("%s/edge_%d.csv", trajectoryDirPath, edgeCount);
            Server server = ServerBuilder
                    .forPort(edgePort)
                    .addService(new EdgeService(edgeId, edgeIp, edgePort++, heartbeatTtlSecs, fogsConfigFilePath, trajectoryFilePath))
                    .build();
            server.start();
            System.out.printf("Edge Server [%s] started at port %d\n", edgeId, server.getPort());
        }
    }
}
