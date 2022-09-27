import com.dreamlab.edgefs.grpcServices.BlockContentResponse;
import com.dreamlab.edgefs.grpcServices.DataQueryServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlockRequest;
import com.dreamlab.edgefs.grpcServices.FindBlockResponse;
import com.dreamlab.edgefs.grpcServices.TSDBQueryRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryResponse;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.UUID;

public class DataQueryService extends DataQueryServerGrpc.DataQueryServerImplBase {
    @Override
    public void findBlocks(FindBlockRequest request, StreamObserver<FindBlockResponse> responseObserver) {
        super.findBlocks(request, responseObserver);
    }

    @Override
    public void execTSDBQuery(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        super.execTSDBQuery(request, responseObserver);
    }

    @Override
    public void getBlockContent(UUIDMessage request, StreamObserver<BlockContentResponse> responseObserver) {
        UUID blockId = Utils.getUuidFromMessage(request);
        BlockContentResponse.Builder responseBuilder = BlockContentResponse.newBuilder();
        responseBuilder.setBlockId(request);
        File file = new File(String.format("store/%s.bin", blockId));
        try (InputStream in = new FileInputStream(file))
        {
            responseBuilder.setBlockContent(ByteString.copyFrom(in.readAllBytes()));
        } catch (Exception e) {
            System.out.println("Unable to read " + file.getAbsolutePath());
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
