package com.dreamlab;

import com.dreamlab.edgefs.grpcServices.DataServerGrpc;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class BackupFogs {
    public static void main(String[] args) throws IOException {
        final String fogsConfigFilePath = args[0];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        for (FogInfo fogInfo : fogDetails.values()) {
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(fogInfo.getDeviceIP(), fogInfo.getDevicePort())
                    .usePlaintext()
                    .build();
            DataServerGrpc.DataServerBlockingStub dataServerBlockingStub = DataServerGrpc.newBlockingStub(managedChannel);
            Empty empty = Empty.newBuilder().build();
            Response response = dataServerBlockingStub
                    .backupIndexLocal(empty);
            System.out.println(String.format("Fog %s: %s", fogInfo.getDeviceId(), response.getIsSuccess()));
            managedChannel.shutdown();
        }
    }
}
