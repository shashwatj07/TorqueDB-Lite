import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.DataStoreServerGrpc;
import com.dreamlab.edgefs.grpcServices.IndexMetadataRequest;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.google.common.geometry.S2CellId;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DataStoreService extends DataStoreServerGrpc.DataStoreServerImplBase {
    private static final int TIME_CHUNK = 60 * 24;
    private final Map<String, Map<String, ArrayList<BlockReplicaInfo>>> metaMap;
    private final Map<Instant, ArrayList<BlockReplicaInfo>> timeMap;
    private final Map<S2CellId, ArrayList<BlockReplicaInfo>> geoMap;

        public DataStoreService(String serverIP, int serverPort) {
        metaMap = new HashMap<>();
        metaMap.put("measurement", new HashMap<>());
        metaMap.put("city", new HashMap<>());
        metaMap.put("startTS", new HashMap<>());
        metaMap.put("endTS", new HashMap<>());
        metaMap.put("min_temperature", new HashMap<>());
        metaMap.put("max_temperature", new HashMap<>());
        metaMap.put("min_dust", new HashMap<>());
        metaMap.put("max_dust", new HashMap<>());
        metaMap.put("min_sound", new HashMap<>());
        metaMap.put("max_sound", new HashMap<>());
        metaMap.put("min_light", new HashMap<>());
        metaMap.put("max_light", new HashMap<>());
        metaMap.put("min_UV", new HashMap<>());
        metaMap.put("max_UV", new HashMap<>());
        metaMap.put("min_S2CellId", new HashMap<>());
        metaMap.put("max_S2CellId", new HashMap<>());
        metaMap.put("mbId", new HashMap<>());
        metaMap.put("bucket", new HashMap<>());
        timeMap = new HashMap<>();
        geoMap = new HashMap<>();
    }

    @Override
    public void indexMetadata(IndexMetadataRequest request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        BlockReplicaInfo blockReplicaInfo = new BlockReplicaInfo(Utils.getUuidFromMessage(request.getBlockId()));
        request.getReplicasList()
                .stream()
                .map(Utils::getReplicaFromMessage)
                .forEach(blockReplicaInfo::addReplicaLocation);
        try {
            Map<String, String> metadataMap = request.getMetadataMapMap();
            UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
            for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                metaMap.get(key).getOrDefault(value,
                        new ArrayList<>()).add(blockReplicaInfo);
            }
            indexTimestamp(request.getTimeRange(), blockReplicaInfo);
            indexS2CellIds(request.getBoundingBox(), blockReplicaInfo);
            responseBuilder.setIsSuccess(true);
        }
        catch (Exception e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void storeBlock(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        File contentsFile = new File(String.format("store/%s.bin", request.getBlockId()));
        try (PrintWriter contentsFileWriter = new PrintWriter(contentsFile, StandardCharsets.UTF_8)) {
            contentsFileWriter.println(request.getBlockContent().toStringUtf8());
            responseBuilder.setIsSuccess(true);
        } catch (IOException e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    private void indexTimestamp(TimeRange timeRange, BlockReplicaInfo blockReplicaInfo) {
        Utils.getTimeChunks(timeRange, TIME_CHUNK)
                .forEach(instant -> timeMap.getOrDefault(instant, new ArrayList<>()).add(blockReplicaInfo));
    }

    private void indexS2CellIds(BoundingBox boundingBox, BlockReplicaInfo blockReplicaInfo) {
        final double minLat = boundingBox.getBottomRightLatLon().getLatitude();
        final double minLon = boundingBox.getTopLeftLatLon().getLongitude();
        final double maxLat = boundingBox.getTopLeftLatLon().getLatitude();
        final double maxLon = boundingBox.getBottomRightLatLon().getLongitude();

        Iterable<S2CellId> cellIds = Utils.getCellIds(minLat, minLon, maxLat, maxLon);

        for (S2CellId s2CellId : cellIds) {
            geoMap.getOrDefault(s2CellId, new ArrayList<>()).add(blockReplicaInfo);
        }
    }
}
