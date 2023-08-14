package com.dreamlab.server;

import com.dreamlab.service.CoordinatorService;
import com.dreamlab.service.DataService;
import com.dreamlab.service.MembershipService;
import com.dreamlab.service.ParentService;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class FogServer {
    public static void main(String[] args) {
        final String fogsConfigFilePath = args[0];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        final String ip = System.getenv("device_ip");
        System.out.println("Fog Server Starting on " + ip);
        UUID fogId = null;
        for (Map.Entry<UUID, FogInfo> entry : fogDetails.entrySet()) {
            if (entry.getValue().getDeviceIP().equals(ip)) {
                fogId = entry.getValue().getDeviceId();
                System.out.println(ip + " " + entry.getValue().getDeviceIP() + " " + fogId);
                break;
            }
        }

        startFogServer(fogId, fogDetails);
    }
    private static void startFogServer(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        FogInfo fogInfo = fogDetails.get(fogId);
        Server server = null;
        try {
            server = ServerBuilder
                    .forPort(fogInfo.getDevicePort())
                    .addService(new ParentService(fogInfo.getDeviceId(), fogDetails))
                    .addService(new MembershipService(fogInfo.getDeviceId(), fogDetails))
                    .addService(new DataService(fogInfo.getDeviceIP(), fogInfo.getDevicePort(), fogInfo.getDeviceId(), fogInfo.getToken()))
                    .addService(new CoordinatorService(fogInfo.getDeviceId(), fogDetails))
                    .build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Fog Server started at " + server.getPort());
        System.out.println("Fog Server Id is " + fogId);
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
