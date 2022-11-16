package com.dreamlab.service;

import com.dreamlab.constants.Constants;
import com.dreamlab.constants.Keys;
import com.dreamlab.edgefs.grpcServices.BlockContentResponse;
import com.dreamlab.edgefs.grpcServices.BlockIdReplicaMetadata;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.DataServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlocksRequest;
import com.dreamlab.edgefs.grpcServices.FindBlocksResponse;
import com.dreamlab.edgefs.grpcServices.IndexMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.edgefs.grpcServices.PutBlockRequest;
import com.dreamlab.edgefs.grpcServices.PutMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.StoreBlockRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryResponse;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CoordinatorService extends CoordinatorServerGrpc.CoordinatorServerImplBase {

    private final Logger LOGGER;
    private final UUID fogId;
    private final int numFogs;
    private final Map<UUID, FogPartition> fogPartitions;
    private final List<UUID> fogIds;
    private final Map<UUID, DataServerGrpc.DataServerBlockingStub> dataStubs;

    public CoordinatorService(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        LOGGER = Logger.getLogger(String.format("[Fog: %s] ", fogId.toString()));
        this.fogId = fogId;
        fogPartitions = generateFogPartitions(new ArrayList<>(fogDetails.values()));
        numFogs = fogDetails.size();
        fogIds = new ArrayList<>(fogDetails.keySet());
        Collections.sort(fogIds);
        dataStubs = new HashMap<>();
    }

    @Override
    public void findBlocks(FindBlocksRequest request, StreamObserver<FindBlocksResponse> responseObserver) {
        Set<UUID> fogIds = new HashSet<>();
        if (request.hasBlockId()) {
            fogIds.add(getRandomFogToReplicate(Utils.getUuidFromMessage(request.getBlockId())));
        }
        if (request.hasTimeRange()) {
            fogIds.addAll(getTemporalShortlist(request.getTimeRange()));
        }
        if (request.hasTimeRange()) {
            fogIds.addAll(getSpatialShortlist(request.getBoundingBox()));
        }
        HashSet<BlockIdReplicaMetadata> responseSet = new HashSet<>();
        FindBlocksResponse.Builder builder = FindBlocksResponse.newBuilder();
        for (UUID fogId : fogIds) {
            responseSet.addAll(getDataStub(fogId).findBlocksLocal(request).getBlockIdReplicasMetadataList());
        }
        builder.addAllBlockIdReplicasMetadata(responseSet);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void execTSDBQuery(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        //TODO
    }

    @Override
    public void getBlockContent(UUIDMessage request, StreamObserver<BlockContentResponse> responseObserver) {
        UUID randomReplica = getRandomFogToReplicate(Utils.getUuidFromMessage(request));
        BlockContentResponse blockContentResponse = getDataStub(randomReplica).getBlockContentLocal(request);
        responseObserver.onNext(blockContentResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void putBlockAndMetadata(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        //TODO
    }

    @Override
    public void putBlockByMetadata(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        final StoreBlockRequest.Builder storeBlockRequestBuilder = StoreBlockRequest.newBuilder();
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Polygon boundingBoxPolygon = null;
        try {
            String jsonFile = request.getMetadataContent().toStringUtf8();
            JSONObject jsonObject =  new JSONObject(jsonFile);
            Instant startInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_START_TIMESTAMP));
            Instant endInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_END_TIMESTAMP));
            double minLatitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MIN_LATITUDE));
            double minLongitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MIN_LONGITUDE));
            double maxLatitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MAX_LATITUDE));
            double maxLongitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MAX_LONGITUDE));
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
        Set<UUID> blockReplicaFogIds = getFogsToReplicate(spatialShortlist, temporalShortlist, randomReplica);
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
            Instant startInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_START_TIMESTAMP));
            Instant endInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_END_TIMESTAMP));
            double minLatitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MIN_LATITUDE));
            double minLongitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MIN_LONGITUDE));
            double maxLatitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MAX_LATITUDE));
            double maxLongitude = Double.parseDouble(jsonObject.getString(Keys.KEY_MAX_LONGITUDE));
            boundingBoxPolygon = Utils.createPolygon(minLatitude, maxLatitude, minLongitude, maxLongitude);
            jsonObject.remove(Keys.KEY_START_TIMESTAMP);
            jsonObject.remove(Keys.KEY_END_TIMESTAMP);
            jsonObject.remove(Keys.KEY_MAX_LONGITUDE);
            jsonObject.remove(Keys.KEY_MIN_LATITUDE);
            jsonObject.remove(Keys.KEY_MIN_LONGITUDE);
            jsonObject.remove(Keys.KEY_MAX_LATITUDE);
            Iterator<String> keys = jsonObject.keys();
            while (keys.hasNext()) {
                String key = keys.next();
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
        Set<UUID> blockReplicaFogIds = getFogsToReplicate(spatialShortlist, temporalShortlist, randomReplica);
//        indexMetadataRequestBuilder.putMetadataMap(Keys.KEY_REPLICA_FOGS, blockReplicaFogIds.toString());
        indexMetadataRequestBuilder.addAllReplicas(blockReplicaFogIds.stream()
                .map(blockReplicaFogId -> Utils.getMessageFromReplica(fogPartitions.get(blockReplicaFogId)))
                .collect(Collectors.toList()));
        Collection<UUID> metadataReplicaFogIds = new HashSet<>();
        metadataReplicaFogIds.addAll(spatialShortlist);
        metadataReplicaFogIds.addAll(temporalShortlist);
        metadataReplicaFogIds.addAll(blockReplicaFogIds);
        IndexMetadataRequest indexMetadataRequest = indexMetadataRequestBuilder.build();
        metadataReplicaFogIds.forEach(replicaFogId -> sendMetadataToDataStoreFog(replicaFogId, indexMetadataRequest));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void sendMetadataToDataStoreFog(UUID dataStoreFogId, IndexMetadataRequest indexMetadataRequest) {
        Response response = getDataStub(dataStoreFogId).indexMetadataLocal(indexMetadataRequest);
    }

    private void sendBlockToDataStoreFog(UUID dataStoreFogId, StoreBlockRequest storeBlockRequest) {
        Response response = getDataStub(dataStoreFogId).storeBlockLocal(storeBlockRequest);
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

    private List<UUID> getSpatialShortlist(BoundingBox boundingBox) {
        return getSpatialShortlist(Utils.createPolygon(boundingBox));
    }

    private List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        return fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
    }

    private List<UUID> getTemporalShortlist(TimeRange timeRange) {
        List<Instant> timeChunks = Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK_SECONDS);
        return timeChunks.stream().map(chunk -> fogIds.get(Math.abs(chunk.hashCode() % numFogs))).collect(Collectors.toList());
    }

    private Set<UUID> getFogsToReplicate(List<UUID> spatialShortlist, List<UUID> temporalShortlist, UUID randomReplica) {
        Set<UUID> replicas = new HashSet<>(Set.of(randomReplica));
        o: for (UUID spacialReplica: spatialShortlist) {
            for (UUID temporalReplica : temporalShortlist) {
                if (!spacialReplica.equals(temporalReplica) && !replicas.contains(spacialReplica) && !replicas.add(temporalReplica)) {
                    replicas.add(spacialReplica);
                    replicas.add(temporalReplica);
                    break o;
                }
            }
        }
        if (replicas.size() < 3) {
            for (UUID anyReplica : fogIds) {
                replicas.add(anyReplica);
                if (replicas.size() >= 3) {
                    break;
                }
            }
        }
        return replicas;
    }

    private UUID getRandomFogToReplicate(UUID blockId) {
        return fogIds.get(Math.abs(blockId.hashCode() % numFogs));
    }

    private DataServerGrpc.DataServerBlockingStub getDataStub(UUID fogId) {
        synchronized (dataStubs) {
            if (!dataStubs.containsKey(fogId)) {
                FogPartition membershipFogInfo = fogPartitions.get(fogId);
                ManagedChannel managedChannel = ManagedChannelBuilder
                        .forAddress(String.valueOf(membershipFogInfo.getDeviceIP()), membershipFogInfo.getDevicePort())
                        .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
                        .build();
                DataServerGrpc.DataServerBlockingStub dataServerBlockingStub = DataServerGrpc.newBlockingStub(managedChannel);
                dataStubs.put(fogId, dataServerBlockingStub);
            }
        }
        return dataStubs.get(fogId);
    }

}
