import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class InsertBlock {
    private InsertBlock() {
    }

    public static void main(String... args) throws IOException {
        final String edgeIp = args[0];
        final int edgePort = Integer.parseInt(args[1]);
        final String blockFilePath = args[2];
        final String metadataFilePath = args[3];

        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(edgeIp, edgePort)
                .usePlaintext()
                .build();
        EdgeServerGrpc.EdgeServerBlockingStub edgeServerBlockingStub = EdgeServerGrpc.newBlockingStub(managedChannel);
        PutBlockAndMetadataRequest putBlockAndMetadataRequest = PutBlockAndMetadataRequest
                .newBuilder()
                .setBlockContent(getBytes(blockFilePath))
                .setMetadataContent(getBytes(metadataFilePath))
                .build();
        BlockIdResponse blockIdResponse = edgeServerBlockingStub
                .putBlockAndMetadata(putBlockAndMetadataRequest);
        managedChannel.shutdown();
        System.out.println("Success: " + Utils.getUuidFromMessage(blockIdResponse.getBlockId()));
    }

    private static ByteString getBytes(String first) throws IOException {
        byte[] bytes = Files.readAllBytes(Path.of(first));
        return ByteString.copyFrom(bytes);
    }
}
