import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class FogServer {

    private FogServer() {
    }

    public static void main(String... args) throws IOException {
        final String fogsConfigFilePath = args[0];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        for (FogInfo fogInfo : fogDetails.values()) {
            startFogServer(fogInfo.getDeviceId(), fogDetails);
        }
    }

    private static void startFogServer(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        new Thread(() -> {
            FogInfo fogInfo = fogDetails.get(fogId);
            Server server = ServerBuilder
                    .forPort(fogInfo.getDevicePort())
                    .addService(new ParentService(fogInfo.getDeviceId(), fogDetails))
                    .addService(new MembershipService(fogDetails))
                    .addService(new DataQueryService())
                    .addService(new DataStoreService(fogInfo.getDeviceIP(), fogInfo.getDevicePort(), fogInfo.getDeviceId()))
                    .build();
            try {
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
        }).start();
    }
}
