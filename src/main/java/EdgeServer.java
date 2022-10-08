import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.UUID;

public class EdgeServer {

    private EdgeServer() {
    }

    public static void main(String... args) throws IOException, InterruptedException {
        final UUID edgeId = UUID.randomUUID();
        final String edgeIp = args[0];
        final int edgePort = Integer.parseInt(args[1]);
        final int heartbeatTtlSecs = Integer.parseInt(args[2]);
        final String fogsConfigFilePath = args[3];
        Server server = ServerBuilder
                .forPort(edgePort)
                .addService(new EdgeService(edgeId, edgeIp, edgePort, heartbeatTtlSecs, fogsConfigFilePath))
                .build();
        server.start();
        System.out.println("Edge Server started at " + server.getPort());
        System.out.println("Edge Server Id is " + edgeId);
        server.awaitTermination();
    }
}
