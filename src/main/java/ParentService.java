import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.DataStoreServerGrpc;
import com.dreamlab.edgefs.grpcServices.HeartbeatRequest;
import com.dreamlab.edgefs.grpcServices.IndexMetadataRequest;
import com.dreamlab.edgefs.grpcServices.MembershipServerGrpc;
import com.dreamlab.edgefs.grpcServices.ParentServerGrpc;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.SetParentFogRequest;
import com.dreamlab.edgefs.grpcServices.StoreBlockRequest;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

public class ParentService extends ParentServerGrpc.ParentServerImplBase {

    private static final Logger LOGGER = Logger.getLogger(ParentService.class.getName());

    private static final int TIME_CHUNK = 60;

    private static final String KEY_START_TIMESTAMP = "startTS";
    private static final String KEY_END_TIMESTAMP = "endTS";
    private static final String KEY_MIN_LATITUDE = "min_lat";
    private static final String KEY_MIN_LONGITUDE = "min_lon";
    private static final String KEY_MAX_LATITUDE = "max_lat";
    private static final String KEY_MAX_LONGITUDE = "max_lon";

    private final UUID fogId;
    private final int numFogs;
    private final Map<UUID, FogPartition> fogPartitions;
    private final List<UUID> fogIds;

    public ParentService(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        this.fogId = fogId;
        fogPartitions = generateFogPartitions(new ArrayList<>(fogDetails.values()));
        numFogs = fogDetails.size();
        fogIds = new ArrayList<>(fogDetails.keySet());
        Collections.sort(fogIds);
    }

    private Map<UUID, FogPartition> generateFogPartitions(List<FogInfo> fogDevices) {
        List<Polygon> polygons = generateVoronoiPolygons(fogDevices);
        return generateFogPolygonMap(fogDevices, polygons);
    }

    private Map<UUID, FogPartition> generateFogPolygonMap(List<FogInfo> fogDevices, List<Polygon> polygons) {
        Map<UUID, FogPartition> fogPartitionMap = new HashMap<>();
        for (FogInfo fog : fogDevices) {
            for (Polygon polygon : polygons) {
                Coordinate coordinate = new Coordinate(fog.getLongitude(), fog.getLatitude());
                if (polygon.contains(GeometryFactory.createPointFromInternalCoord(coordinate, polygon))) {
                    fogPartitionMap.put(fog.getDeviceId(), Utils.getFogPartition(fog, polygon));
                }
            }
        }
        return fogPartitionMap;
    }

    private List<Polygon> generateVoronoiPolygons(List<FogInfo> fogDevices) {
        final Polygon region = Utils.createPolygon(-90, 90, -180, 180);
        List<Coordinate> coordinates = fogDevices.stream().map(Utils::getCoordinateFromFogInfo).collect(Collectors.toList());
        VoronoiDiagramBuilder diagramBuilder = new VoronoiDiagramBuilder();
        diagramBuilder.setSites(coordinates);
        diagramBuilder.setClipEnvelope(region.getEnvelopeInternal());
        Geometry polygonCollection = diagramBuilder.getDiagram(region.getFactory());

        List<Polygon> voronoiPolygons = new ArrayList<>();

        if (polygonCollection instanceof GeometryCollection) {
            GeometryCollection geometryCollection = (GeometryCollection) polygonCollection;
            for (int polygonIndex = 0; polygonIndex < geometryCollection.getNumGeometries(); polygonIndex++) {
                Polygon polygon = (Polygon) geometryCollection.getGeometryN(polygonIndex);
                voronoiPolygons.add(polygon);
            }
        }
        return voronoiPolygons;
    }

    private List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        return fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
    }

    private List<UUID> getTemporalShortlist(TimeRange timeRange) {
        List<Instant> timeChunks = Utils.getTimeChunks(timeRange, TIME_CHUNK);
        return timeChunks.stream().map(chunk -> fogIds.get(Math.abs(chunk.hashCode() % numFogs))).collect(Collectors.toList());
    }

    private List<UUID> getFogsToReplicate(List<UUID> spatialShortlist, List<UUID> temporalShortlist, UUID randomReplica) {
        // TODO: Hanlde collisions, Change Hash Function
        UUID spacialReplica = Utils.getRandomElement(spatialShortlist);
        UUID temporalReplica = Utils.getRandomElement(temporalShortlist);
        return List.of(spacialReplica, temporalReplica, randomReplica);
    }

    private UUID getRandomFogToReplicate(UUID blockId) {
        return fogIds.get(Math.abs(blockId.hashCode() % numFogs));
    }

    @Override
    public void putBlock(PutBlockRequest request, StreamObserver<Response> responseObserver) {

        final StoreBlockRequest.Builder storeBlockRequestBuilder = StoreBlockRequest.newBuilder();
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Polygon boundingBoxPolygon = null;
        try {
            String jsonFile = request.getMetadataContent().toStringUtf8();
            JSONObject jsonObject =  new JSONObject(jsonFile);
            Instant startInstant = Utils.getInstantFromString(jsonObject.getString(KEY_START_TIMESTAMP));
            Instant endInstant = Utils.getInstantFromString(jsonObject.getString(KEY_END_TIMESTAMP));
            double minLatitude = Double.parseDouble(jsonObject.getString(KEY_MIN_LATITUDE));
            double minLongitude = Double.parseDouble(jsonObject.getString(KEY_MIN_LONGITUDE));
            double maxLatitude = Double.parseDouble(jsonObject.getString(KEY_MAX_LATITUDE));
            double maxLongitude = Double.parseDouble(jsonObject.getString(KEY_MAX_LONGITUDE));
            boundingBoxPolygon = Utils.createPolygon(minLatitude, maxLatitude, minLongitude, maxLongitude);
            timeRangeBuilder
                    .setStartTimestamp(Utils.getTimestampMessageFromInstant(startInstant))
                    .setEndTimestamp(Utils.getTimestampMessageFromInstant(endInstant));
            boundingBoxBuilder
                    .setTopLeftLatLon(Point.newBuilder()
                            .setLatitude(maxLatitude)
                            .setLongitude(minLongitude)
                            .build())
                    .setBottomRightLatLon(Point.newBuilder()
                            .setLatitude(minLatitude)
                            .setLongitude(maxLongitude)
                            .build());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        storeBlockRequestBuilder.setBlockId(request.getBlockId());
        storeBlockRequestBuilder.setBlockContent(request.getBlockContent());
        TimeRange timeRange = timeRangeBuilder.build();
        BoundingBox boundingBox = boundingBoxBuilder.build();
        List<UUID> spatialShortlist = getSpatialShortlist(boundingBoxPolygon);
        List<UUID> temporalShortlist = getTemporalShortlist(timeRange);
        UUID randomReplica = getRandomFogToReplicate(Utils.getUuidFromMessage(request.getBlockId()));
        List<UUID> blockReplicaFogIds = getFogsToReplicate(spatialShortlist, temporalShortlist, randomReplica);
        StoreBlockRequest storeBlockRequest = storeBlockRequestBuilder.build();
        blockReplicaFogIds.forEach(replicaFogId -> sendBlockToDataStoreFog(replicaFogId, storeBlockRequest));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putMetadata(PutMetadataRequest request, StreamObserver<Response> responseObserver) {
        final IndexMetadataRequest.Builder indexMetadataRequestBuilder = IndexMetadataRequest.newBuilder();
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Polygon boundingBoxPolygon = null;
        try {
            String jsonFile = request.getMetadataContent().toStringUtf8();
            JSONObject jsonObject =  new JSONObject(jsonFile);
            Instant startInstant = Utils.getInstantFromString(jsonObject.getString(KEY_START_TIMESTAMP));
            Instant endInstant = Utils.getInstantFromString(jsonObject.getString(KEY_END_TIMESTAMP));
            double minLatitude = Double.parseDouble(jsonObject.getString(KEY_MIN_LATITUDE));
            double minLongitude = Double.parseDouble(jsonObject.getString(KEY_MIN_LONGITUDE));
            double maxLatitude = Double.parseDouble(jsonObject.getString(KEY_MAX_LATITUDE));
            double maxLongitude = Double.parseDouble(jsonObject.getString(KEY_MAX_LONGITUDE));
            boundingBoxPolygon = Utils.createPolygon(minLatitude, maxLatitude, minLongitude, maxLongitude);
            jsonObject.remove(KEY_START_TIMESTAMP);
            jsonObject.remove(KEY_END_TIMESTAMP);
            jsonObject.remove(KEY_MAX_LONGITUDE);
            jsonObject.remove(KEY_MIN_LATITUDE);
            jsonObject.remove(KEY_MIN_LONGITUDE);
            jsonObject.remove(KEY_MAX_LATITUDE);
            Iterator<String> keys = jsonObject.keys();
            for (String key = keys.next(); keys.hasNext(); key = keys.next()) {
                String value = (String) jsonObject.get(key);
                indexMetadataRequestBuilder.putMetadataMap(key, value);
            }
            timeRangeBuilder
                    .setStartTimestamp(Utils.getTimestampMessageFromInstant(startInstant))
                    .setEndTimestamp(Utils.getTimestampMessageFromInstant(endInstant));
            boundingBoxBuilder
                    .setTopLeftLatLon(Point.newBuilder()
                            .setLatitude(maxLatitude)
                            .setLongitude(minLongitude)
                            .build())
                    .setBottomRightLatLon(Point.newBuilder()
                            .setLatitude(minLatitude)
                            .setLongitude(maxLongitude)
                            .build());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        indexMetadataRequestBuilder.setBlockId(request.getBlockId());
        TimeRange timeRange = timeRangeBuilder.build();
        indexMetadataRequestBuilder.setTimeRange(timeRange);
        BoundingBox boundingBox = boundingBoxBuilder.build();
        indexMetadataRequestBuilder.setBoundingBox(boundingBox);
        List<UUID> spatialShortlist = getSpatialShortlist(boundingBoxPolygon);
        List<UUID> temporalShortlist = getTemporalShortlist(timeRange);
        UUID randomReplica = getRandomFogToReplicate(Utils.getUuidFromMessage(request.getBlockId()));
        List<UUID> blockReplicaFogIds = getFogsToReplicate(spatialShortlist, temporalShortlist, randomReplica);
        indexMetadataRequestBuilder.putMetadataMap("replica_fogs", blockReplicaFogIds.toString());
        Collection<UUID> metadataReplicaFogIds = new HashSet<>();
        metadataReplicaFogIds.addAll(spatialShortlist);
        metadataReplicaFogIds.addAll(temporalShortlist);
        metadataReplicaFogIds.add(randomReplica);
        IndexMetadataRequest indexMetadataRequest = indexMetadataRequestBuilder.build();
        metadataReplicaFogIds.forEach(replicaFogId -> sendMetadataToDataStoreFog(replicaFogId, indexMetadataRequest));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void sendHeartbeat(HeartbeatRequest request, StreamObserver<Response> responseObserver) {
        LOGGER.info("Heartbeat Received From: " + Utils.getUuidFromMessage(request.getEdgeId()));
        SetParentFogRequest.Builder builder = SetParentFogRequest.newBuilder();
        SetParentFogRequest setParentFogRequest = builder
                .setEdgeId(request.getEdgeId())
                .setFogId(Utils.getMessageFromUUID(fogId))
                .setHeartbeatTimestamp(Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000).build())
                .setTtlSecs(request.getTtlSecs())
                .build();
        List<Integer> membershipFogIndices = Utils.getMembershipFogIndices(Utils.getUuidFromMessage(request.getEdgeId()));
        membershipFogIndices.forEach(index -> sendParentInfoToMembershipFog(fogIds.get(index), setParentFogRequest));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void sendParentInfoToMembershipFog(UUID membershipFogId, SetParentFogRequest setParentFogRequest) {
        FogPartition membershipFogInfo = fogPartitions.get(membershipFogId);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(String.valueOf(membershipFogInfo.getDeviceIP()), membershipFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        MembershipServerGrpc.MembershipServerBlockingStub membershipServerBlockingStub = MembershipServerGrpc.newBlockingStub(managedChannel);
        Response response = membershipServerBlockingStub.setParentFog(setParentFogRequest);
        managedChannel.shutdown();
    }

    private void sendMetadataToDataStoreFog(UUID dataStoreFogId, IndexMetadataRequest indexMetadataRequest) {
        FogPartition membershipFogInfo = fogPartitions.get(dataStoreFogId);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(String.valueOf(membershipFogInfo.getDeviceIP()), membershipFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        DataStoreServerGrpc.DataStoreServerBlockingStub dataStoreServerBlockingStub = DataStoreServerGrpc.newBlockingStub(managedChannel);
        Response response = dataStoreServerBlockingStub.indexMetadata(indexMetadataRequest);
        managedChannel.shutdown();
    }

    private void sendBlockToDataStoreFog(UUID dataStoreFogId, StoreBlockRequest storeBlockRequest) {
        FogPartition membershipFogInfo = fogPartitions.get(dataStoreFogId);
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(membershipFogInfo.getDeviceIP(), membershipFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        DataStoreServerGrpc.DataStoreServerBlockingStub dataStoreServerBlockingStub = DataStoreServerGrpc.newBlockingStub(managedChannel);
        Response response = dataStoreServerBlockingStub.storeBlock(storeBlockRequest);
        managedChannel.shutdown();
    }

}
