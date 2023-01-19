package com.dreamlab.server;

import com.dreamlab.service.EdgeService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.util.UUID;

public class EdgeServer {

    private EdgeServer() {
    }

    public static void main(String... args) throws IOException, InterruptedException {
        final UUID edgeId = UUID.randomUUID();
        final String edgeIp = System.getenv("device_ip");
        System.out.println("Edge Server Starting on " + edgeIp);
        final int edgePort = Integer.parseInt(args[0]);
        final int heartbeatTtlSecs = Integer.parseInt(args[1]);
        final String fogsConfigFilePath = args[2];
        final String trajectoryFilePath = args[3];
        Server server = ServerBuilder
                .forPort(edgePort)
                .addService(new EdgeService(edgeId, edgeIp, edgePort, heartbeatTtlSecs, fogsConfigFilePath, trajectoryFilePath))
                .build();
        server.start();
        System.out.println("Edge Server started at " + server.getPort());
        System.out.println("Edge Server Id is " + edgeId);
        server.awaitTermination();
    }
}
