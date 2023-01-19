package com.dreamlab.server;

import com.dreamlab.service.EdgeService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.UUID;

public class EdgeServer {

    private EdgeServer() {
    }

    public static void main(String... args) {
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
        try {
            server.start();
        }
        catch (IOException ex) {
            System.out.println("Server could not be started");
            ex.printStackTrace();
        }
        System.out.println("Edge Server started at " + server.getPort());
        System.out.println("Edge Server Id is " + edgeId);
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
