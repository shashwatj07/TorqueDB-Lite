import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class InsertBlock {
    public static void main(String... args) throws IOException {
        final String parentIP = args[0];
        final int parentPort = Integer.parseInt(args[1]);
        final String blockIdString = args[2];
        final String blockContentsFilePath = args[3];
        final String blockMetadataFilePath = args[4];
        final UUID uuidFromBlockId = UUID.fromString(blockIdString);
        UUIDMessage blockId = UUIDMessage
                .newBuilder()
                .setLsb(uuidFromBlockId.getLeastSignificantBits())
                .setMsb(uuidFromBlockId.getMostSignificantBits())
                .build();
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentIP, parentPort)
                .usePlaintext()
                .build();
        ParentServerGrpc.ParentServerBlockingStub parentServerBlockingStub = ParentServerGrpc.newBlockingStub(managedChannel);
        PutMetadataRequest putMetadataRequest = PutMetadataRequest
            .newBuilder()
            .setBlockId(blockId)
            .setMetadataContent(getBytes(blockMetadataFilePath))
            .build();
        Response putMetadataResponse = parentServerBlockingStub
                .putMetadata(putMetadataRequest);
        PutBlockRequest putBlockRequest = PutBlockRequest
            .newBuilder()
            .setBlockId(blockId)
            .setBlockContent(getBytes(blockContentsFilePath))
            .build();
        Response putBlockResponse = parentServerBlockingStub
                .putBlock(putBlockRequest);
        managedChannel.shutdown();
        System.out.println("Success: " + blockId);
    }

    private static ByteString getBytes(String first) throws IOException {
        byte[] bytes = Files.readAllBytes(Path.of(first));
        return ByteString.copyFrom(bytes);
    }
}
