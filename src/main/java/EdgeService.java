import com.dreamlab.edgefs.grpcServices.BlockIdResponse;
import com.dreamlab.edgefs.grpcServices.EdgeServerGrpc;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.edgefs.grpcServices.PutBlockAndMetadataRequest;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class EdgeService extends EdgeServerGrpc.EdgeServerImplBase {

    private final ByteBuffer ip;

    private final UUID id;

    private final int port;

    private final int heartbeatTtlSecs;

    private Runnable heartbeatThread;

    private final Map<UUID, FogInfo> fogDetails;

    public EdgeService(UUID id, String ip, int port, int heartbeatTtlSecs, String fogsConfigFilePath) throws IOException {
        this.id  = id;
        this.ip = ByteBuffer.wrap(ip.getBytes(StandardCharsets.UTF_8));
        this.port = port;
        this.heartbeatTtlSecs = heartbeatTtlSecs;
        fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        heartbeatThread = new Heartbeat(id, ip, port, heartbeatTtlSecs);
        heartbeatThread.run();
    }

    @Override
    public void putBlockAndMetadata(PutBlockAndMetadataRequest request, StreamObserver<BlockIdResponse> responseObserver) {
        UUIDMessage blockId = Utils.getMessageFromUUID(UUID.randomUUID());
        PutBlockRequest.Builder putBlockRequestBuilder = PutBlockRequest.newBuilder();
        putBlockRequestBuilder.setBlockId(blockId);
        putBlockRequestBuilder.setBlockContent(request.getBlockContent());
        putBlockRequestBuilder.setMetadataContent(request.getMetadataContent());
        PutMetadataRequest.Builder putMetadataRequestBuilder = PutMetadataRequest.newBuilder();
        putMetadataRequestBuilder.setBlockId(blockId);
        putMetadataRequestBuilder.setMetadataContent(request.getMetadataContent());
        final FogInfo parentFogInfo = Utils.getParentFog(fogDetails, 0, 0);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(String.valueOf(parentFogInfo.getDeviceIP()), parentFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        ParentServerGrpc.ParentServerBlockingStub parentServerBlockingStub = ParentServerGrpc.newBlockingStub(managedChannel);
        Response putBlockResponse = parentServerBlockingStub.putBlock(putBlockRequestBuilder.build());
        Response putMetadataResponse = parentServerBlockingStub.putMetadata(putMetadataRequestBuilder.build());
        BlockIdResponse.Builder blockIdResponseBuilder = BlockIdResponse.newBuilder();
        blockIdResponseBuilder.setBlockId(blockId);
        blockIdResponseBuilder.setIsSuccess(putBlockResponse.getIsSuccess() && putMetadataResponse.getIsSuccess());

        responseObserver.onNext(blockIdResponseBuilder.build());
        responseObserver.onCompleted();
    }
}
