import com.dreamlab.edgefs.grpcServices.GetParentFogRequest;
import com.dreamlab.edgefs.grpcServices.GetParentFogResponse;
import com.dreamlab.edgefs.grpcServices.MembershipServerGrpc;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.SetParentFogRequest;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;

public class MembershipService extends MembershipServerGrpc.MembershipServerImplBase {

    private static final String FOGS_JSON = "src/main/resources/fogs.json";

    private final Map<UUID, FogInfo> fogDetails;

    private final Map<UUID, MembershipInfo> membershipMap;

    public MembershipService() throws IOException {
        membershipMap = new HashMap<>();
        fogDetails = new HashMap<>();

    }

    @Override
    public void setParentFog(SetParentFogRequest request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        if (membershipMap.containsKey(Utils.getUuidFromMessage(request.getEdgeId()))
                && membershipMap.get(Utils.getUuidFromMessage(request.getEdgeId())).getLastHeartbeat().isAfter(Utils.getInstantFromTimestampMessage(request.getHeartbeatTimestamp()))) {
            membershipMap.put(Utils.getUuidFromMessage(request.getEdgeId()),
                    new MembershipInfo(Utils.getUuidFromMessage(request.getFogId()),
                            Utils.getInstantFromTimestampMessage(request.getHeartbeatTimestamp()),
                            request.getTtlSecs()));
        }
        responseBuilder.setIsSuccess(true);
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getParentFog(GetParentFogRequest request, StreamObserver<GetParentFogResponse> responseObserver) {
        GetParentFogResponse.Builder responseBuilder = GetParentFogResponse.newBuilder();
        try {
           MembershipInfo membershipInfo = membershipMap.get(Utils.getUuidFromMessage(request.getEdgeId()));
           responseBuilder.setIsSuccess(true)
                   .setParentFogId(Utils.getMessageFromUUID(membershipInfo.getParentFogId()))
                   .setTtlSecs(membershipInfo.getTtlSecs());
        } catch (Exception ex) {
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
