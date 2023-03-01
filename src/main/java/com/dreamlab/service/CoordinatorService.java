package com.dreamlab.service;

import com.dreamlab.api.TSDBQuery;
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
import com.dreamlab.query.InfluxDBQuery;
import com.dreamlab.types.CostModelOutput;
import com.dreamlab.types.ExecPlan;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.CostModel;
import com.dreamlab.utils.QueryDecomposition;
import com.dreamlab.utils.Utils;
import com.google.protobuf.ByteString;
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

import java.io.ObjectInputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CoordinatorService extends CoordinatorServerGrpc.CoordinatorServerImplBase {

    private final Logger LOGGER;
    private final UUID fogId;
    private final int numFogs;
    private final Map<UUID, FogPartition> fogPartitions;
    private final List<UUID> fogIds;
    private final ConcurrentMap<UUID, DataServerGrpc.DataServerBlockingStub> dataStubs;

    public CoordinatorService(UUID fogId, Map<UUID, FogInfo> fogDetails) {
        LOGGER = Logger.getLogger(String.format("[Fog: %s] ", fogId.toString()));
        this.fogId = fogId;
        fogPartitions = generateFogPartitions(new ArrayList<>(fogDetails.values()));
        numFogs = fogDetails.size();
        fogIds = new ArrayList<>(fogDetails.keySet());
        Collections.sort(fogIds);
        dataStubs = new ConcurrentHashMap<>();
    }

    @Override
    public void findBlocks(FindBlocksRequest request, StreamObserver<FindBlocksResponse> responseObserver) {
        final long start = System.currentTimeMillis();
        Set<UUID> fogIds = new HashSet<>();
        if (request.hasBlockId()) {
            fogIds.add(getFogHashByBlockId(Utils.getUuidFromMessage(request.getBlockId())));
        }
        if (request.hasTimeRange()) {
            fogIds.addAll(getTemporalShortlist(request.getTimeRange()));
        }
        if (request.hasBoundingBox()) {
            fogIds.addAll(getSpatialShortlist(request.getBoundingBox()));
        }

        List<Future<FindBlocksResponse>> futures = new ArrayList<>();
        final long t1 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Constants.N_THREADS);
            fogIds.forEach(fogId -> futures.add(executorService.submit(() -> getDataStub(fogId).findBlocksLocal(request))));
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] CoordinatorServer.forEach.findBlocksLocal: %d", LOGGER.getName(), (t2 - t1)));

        HashSet<BlockIdReplicaMetadata> responseSet = new HashSet<>();
        for (Future<FindBlocksResponse> future : futures) {
            try {
                FindBlocksResponse findBlocksResponse = future.get();
                responseSet.addAll(findBlocksResponse.getBlockIdReplicasMetadataList());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }

//        Set<BlockIdReplicaMetadata> responseSet = new HashSet<>();
        FindBlocksResponse.Builder builder = FindBlocksResponse.newBuilder();
//        for (UUID fogId : fogIds) {
//            final long t1 = System.currentTimeMillis();
//            FindBlocksResponse findBlocksResponseLocal = getDataStub(fogId).findBlocksLocal(request);
//
//            final long t2 = System.currentTimeMillis();
//            LOGGER.info(String.format("%s[Outer] CoordinatorServer.findBlocksLocal: %d", LOGGER.getName(), (t2 - t1)));
//            LOGGER.info("findBlocksResponseLocal " + findBlocksResponseLocal);
//            responseSet.addAll(findBlocksResponseLocal.getBlockIdReplicasMetadataList());
//            LOGGER.info(responseSet.toString());
//        }
        builder.addAllBlockIdReplicasMetadata(responseSet);
        FindBlocksResponse findBlocksResponse = builder.build();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner] CoordinatorServer.findBlocks: %d", LOGGER.getName(), (end - start)));
        LOGGER.info(LOGGER.getName() + "\n" + findBlocksResponse);
        responseObserver.onNext(findBlocksResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void execTSDBQuery(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        final long start = System.currentTimeMillis();
        TSDBQueryResponse.Builder tsdbQueryResponseBuilder = TSDBQueryResponse.newBuilder();
        //TODO
        /*
        1. Extract Predicates - time range, spatial bounding box, blockId
        2. Find out which fogs these predicates map to
        3. Query those fogs: findBlocksLocal
        4. Union of results
        5. Query those fogs: execTSDBQueryLocal
        6. Aggregate Results
        7. Return
         */
        ObjectInputStream objectInputStream = null;
        InfluxDBQuery influxDBQuery = null;
        try {
            objectInputStream = new ObjectInputStream(request.getFluxQuery(0).newInput());
            influxDBQuery = (InfluxDBQuery) objectInputStream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        LOGGER.info(influxDBQuery.toString());
        FindBlocksRequest.Builder findBlocksRequestBuilder = FindBlocksRequest.newBuilder();
        findBlocksRequestBuilder.setQueryId(request.getQueryId());
        findBlocksRequestBuilder.setIsAndQuery(true);
        Set<UUID> fogIds = new HashSet<>();
        Set<UUID> temporalShortlist = new HashSet<>();
        Set<UUID> spatialShortlist = new HashSet<>();
        if (influxDBQuery.getOperations().containsKey("range")) {
            HashMap<String, String> range = influxDBQuery.getOperations().get("range");
            LOGGER.info(range.toString());
            findBlocksRequestBuilder
                    .setTimeRange(
                            TimeRange.newBuilder()
                                    .setStartTimestamp(Utils.getTimestampMessageFromInstant(Utils.getInstantFromString(range.get("start"))))
                                    .setEndTimestamp(Utils.getTimestampMessageFromInstant(Utils.getInstantFromString(range.get("stop"))))
                                    .build()
                    );
            temporalShortlist.addAll(getTemporalShortlist(range.get("start"), range.get("stop")));
            LOGGER.info(String.format("%s[Query %s] CoordinatorServer.temporalShortlist: %s", LOGGER.getName(), influxDBQuery.getQueryId(), temporalShortlist));
        }
        if (influxDBQuery.getOperations().containsKey("region")) {
            HashMap<String, String> region = influxDBQuery.getOperations().get("region");
            findBlocksRequestBuilder
                    .setBoundingBox(
                    BoundingBox
                            .newBuilder()
                            .setBottomRightLatLon(
                                    Point.newBuilder()
                                            .setLatitude(Double.parseDouble(region.get("minLat")))
                                            .setLongitude(Double.parseDouble(region.get("maxLon")))
                                            .build())
                            .setTopLeftLatLon(
                                    Point.newBuilder()
                                            .setLatitude(Double.parseDouble(region.get("maxLat")))
                                            .setLongitude(Double.parseDouble(region.get("minLon")))
                                            .build())
                            .build());
            LOGGER.info(String.format("%s[Query %s] CoordinatorServer.spatialShortlist: %s", LOGGER.getName(), influxDBQuery.getQueryId(), spatialShortlist));
            spatialShortlist.addAll(getSpatialShortlist(findBlocksRequestBuilder.getBoundingBox()));
        }
        FindBlocksRequest findBlocksRequest = findBlocksRequestBuilder.build();

        if (influxDBQuery.getOperations().containsKey("region") && spatialShortlist.size() < temporalShortlist.size()) {
            fogIds.addAll(spatialShortlist);
        }
        else {
            fogIds.addAll(temporalShortlist);
        }

        LOGGER.info(String.format("%s[Query %s] CoordinatorServer.finalShortlist: %s", LOGGER.getName(), influxDBQuery.getQueryId(), fogIds));

        List<Future<FindBlocksResponse>> futures = new ArrayList<>();
        final long t1 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Constants.N_THREADS);
            fogIds.forEach(fogId -> futures.add(executorService.submit(() -> getDataStub(fogId).findBlocksLocal(findBlocksRequest))));
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.forEach.findBlocksLocal: %d", LOGGER.getName(), influxDBQuery.getQueryId(), (t2 - t1)));

        HashSet<BlockIdReplicaMetadata> responseSet = new HashSet<>();
        for (Future<FindBlocksResponse> future : futures) {
            try {
                FindBlocksResponse findBlocksResponse = future.get();
                responseSet.addAll(findBlocksResponse.getBlockIdReplicasMetadataList());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        LOGGER.info(String.format("%s[Count %s] CoordinatorServer.finalBlocks: %d", LOGGER.getName(), influxDBQuery.getQueryId(), responseSet.size()));
//        LOGGER.info(String.format("%s[Count] CoordinatorServer.finalBlocks(%s): %s", LOGGER.getName(), influxDBQuery.getQueryId(), responseSet));

        /*
        1. Execute Cost model to assign blocks to fogs
        2. Generate flux query on the coordinator
        3. Execute sub queries on specific fogs
        4.
         */

        List<ExecPlan> plan = CostModel.QP1(responseSet);
        List<UUID> fogsToQuery = plan.stream().map(ExecPlan::getFogId).distinct().collect(Collectors.toList());
        Map<UUID, TSDBQuery> fogQueries = new HashMap<>();
        for (UUID fogId : fogsToQuery) {
            fogQueries.putIfAbsent(fogId, influxDBQuery);
        }
        QueryDecomposition queryDecomposition = new QueryDecomposition();
        CostModelOutput costModelOutput = queryDecomposition.l21decompose(fogQueries, plan);
        System.out.println("L2: " + costModelOutput.perFogLevel2Query);
        LOGGER.info(String.format("%s[Count %s] CoordinatorServer.execTSDBQueryLocal: %d", LOGGER.getName(), influxDBQuery.getQueryId(), costModelOutput.perFogLevel2Query.size()));

        List<Future<TSDBQueryResponse>> futureList = new ArrayList<>();
        final long t3 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Constants.N_THREADS);
            for (Map.Entry<UUID, List<String>> entry : costModelOutput.perFogLevel2Query.entrySet()) {
                for (String query : entry.getValue()) {
                    futureList.add(executorService.submit(() -> execTSDBQueryOnDataStoreFog(entry.getKey(),
                            TSDBQueryRequest.newBuilder().addFluxQuery(ByteString.copyFromUtf8(query)).setQueryId(request.getQueryId()).build())));
                }
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t4 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.forEach.execTSDBQueryLocal: %d", LOGGER.getName(), influxDBQuery.getQueryId(), (t4 - t3)));

        StringBuilder responseBuffer = new StringBuilder();
        for (Future<TSDBQueryResponse> future : futureList) {
            try {
                responseBuffer.append(future.get().getFluxQueryResponse().toStringUtf8());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
//                throw new RuntimeException(e);
            }
        }

        tsdbQueryResponseBuilder.setFluxQueryResponse(ByteString.copyFromUtf8(responseBuffer.toString()));
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] CoordinatorServer.execTSDBQuery: %d", LOGGER.getName(), influxDBQuery.getQueryId(), (end - start)));
        responseObserver.onNext(tsdbQueryResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBlockContent(UUIDMessage request, StreamObserver<BlockContentResponse> responseObserver) {
        final long start = System.currentTimeMillis();
        UUID randomReplica = getFogHashByBlockId(Utils.getUuidFromMessage(request));
        final long t1 = System.currentTimeMillis();
        BlockContentResponse blockContentResponse = getDataStub(randomReplica).getBlockContentLocal(request);
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] CoordinatorServer.getBlockContentLocal: %d", LOGGER.getName(), (end - t1)));
        LOGGER.info(String.format("%s[Inner] CoordinatorServer.getBlockContent: %d", LOGGER.getName(), (end - start)));
        responseObserver.onNext(blockContentResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void putBlockAndMetadata(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        //TODO
    }

    @Override
    public void putBlockByMetadata(PutBlockRequest request, StreamObserver<Response> responseObserver) {
        final UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
        final long start = System.currentTimeMillis();
        final StoreBlockRequest.Builder storeBlockRequestBuilder = StoreBlockRequest.newBuilder();
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Polygon boundingBoxPolygon = null;
        Instant startInstant = null, endInstant = null;
        try {
            String jsonFile = request.getMetadataContent().toStringUtf8();
            JSONObject jsonObject =  new JSONObject(jsonFile);
            startInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_START_TIMESTAMP));
            endInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_END_TIMESTAMP));
            double minLatitude = jsonObject.getDouble(Keys.KEY_MIN_LATITUDE);
            double minLongitude = jsonObject.getDouble(Keys.KEY_MIN_LONGITUDE);
            double maxLatitude = jsonObject.getDouble(Keys.KEY_MAX_LATITUDE);
            double maxLongitude = jsonObject.getDouble(Keys.KEY_MAX_LONGITUDE);
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
//        BoundingBox boundingBox = boundingBoxBuilder.build();
        List<UUID> spatialShortlist = getSpatialShortlist(boundingBoxPolygon);
        List<UUID> temporalShortlist = getTemporalShortlist(timeRange);
        UUID randomReplica = getFogHashByBlockId(blockId);
        UUID temporalReplica = getFogHashByTimeRange(startInstant, endInstant);
        UUID spatialReplica = getFogHashByBoundingBox(boundingBoxPolygon);
        Set<UUID> blockReplicaFogIds = getFogsToReplicate(blockId, spatialShortlist, temporalShortlist, randomReplica, temporalReplica, spatialReplica);
        LOGGER.info(String.format("%s[Insert %s] Replica Fogs %s", LOGGER.getName(), blockId, blockReplicaFogIds));
        StoreBlockRequest storeBlockRequest = storeBlockRequestBuilder.build();
        final long t1 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(3);
            blockReplicaFogIds.forEach(replicaFogId -> executorService.submit(() -> sendBlockToDataStoreFog(replicaFogId, storeBlockRequest)));
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.forEach.sendBlockToDataStoreFog: %d", LOGGER.getName(), blockId, (t2 - t1)));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] CoordinatorServer.putBlockByMetadata: %d", LOGGER.getName(), blockId, (end - start)));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void putMetadata(PutMetadataRequest request, StreamObserver<Response> responseObserver) {
        final UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
        final long start = System.currentTimeMillis();
        final IndexMetadataRequest.Builder indexMetadataRequestBuilder = IndexMetadataRequest.newBuilder();
        final TimeRange.Builder timeRangeBuilder = TimeRange.newBuilder();
        final BoundingBox.Builder boundingBoxBuilder = BoundingBox.newBuilder();
        Polygon boundingBoxPolygon = null;
        Instant startInstant = null, endInstant = null;
        try {
            String jsonFile = request.getMetadataContent().toStringUtf8();
            JSONObject jsonObject =  new JSONObject(jsonFile);
            startInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_START_TIMESTAMP));
            endInstant = Utils.getInstantFromString(jsonObject.getString(Keys.KEY_END_TIMESTAMP));
            double minLatitude = jsonObject.getDouble(Keys.KEY_MIN_LATITUDE);
            double minLongitude = jsonObject.getDouble(Keys.KEY_MIN_LONGITUDE);
            double maxLatitude = jsonObject.getDouble(Keys.KEY_MAX_LATITUDE);
            double maxLongitude = jsonObject.getDouble(Keys.KEY_MAX_LONGITUDE);
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
            throw new RuntimeException(e);
        }
        indexMetadataRequestBuilder.setBlockId(request.getBlockId());
        TimeRange timeRange = timeRangeBuilder.build();
        indexMetadataRequestBuilder.setTimeRange(timeRange);
        BoundingBox boundingBox = boundingBoxBuilder.build();
        indexMetadataRequestBuilder.setBoundingBox(boundingBox);
        final List<UUID> spatialShortlist = getSpatialShortlist(boundingBoxPolygon);
        final List<UUID> temporalShortlist = getTemporalShortlist(timeRange);
        UUID randomReplica = getFogHashByBlockId(blockId);
        UUID temporalReplica = getFogHashByTimeRange(startInstant, endInstant);
        UUID spatialReplica = getFogHashByBoundingBox(boundingBoxPolygon);
        Set<UUID> blockReplicaFogIds = getFogsToReplicate(blockId, spatialShortlist, temporalShortlist, randomReplica, temporalReplica, spatialReplica);
//        indexMetadataRequestBuilder.putMetadataMap(Keys.KEY_REPLICA_FOGS, blockReplicaFogIds.toString());
        indexMetadataRequestBuilder.addAllReplicas(blockReplicaFogIds.stream()
                .map(blockReplicaFogId -> Utils.getMessageFromReplica(fogPartitions.get(blockReplicaFogId)))
                .collect(Collectors.toList()));
        Collection<UUID> metadataReplicaFogIds = new HashSet<>();
        metadataReplicaFogIds.addAll(spatialShortlist);
        metadataReplicaFogIds.addAll(temporalShortlist);
        metadataReplicaFogIds.addAll(blockReplicaFogIds);
        IndexMetadataRequest indexMetadataRequest = indexMetadataRequestBuilder.build();
        final long t1 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Constants.N_THREADS);
            metadataReplicaFogIds.forEach(replicaFogId -> {
                executorService.submit(() -> sendMetadataToDataStoreFog(replicaFogId, indexMetadataRequest));
            });
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] CoordinatorServer.forEach.sendMetadataToDataStoreFog: %d", LOGGER.getName(), blockId, (t2 - t1)));
        Response response = Response.newBuilder().setIsSuccess(true).build();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Insert %s] CoordinatorServer.randomReplica: %s", LOGGER.getName(), blockId, randomReplica));
        LOGGER.info(String.format("%s[Insert %s] CoordinatorServer.spatialShortlist: %s", LOGGER.getName(), blockId, spatialShortlist));
        LOGGER.info(String.format("%s[Insert %s] CoordinatorServer.temporalShortlist: %s", LOGGER.getName(), blockId, temporalShortlist));
        LOGGER.info(String.format("%s[Insert %s] CoordinatorServer.randomShortlist: [%s]", LOGGER.getName(), blockId, randomReplica));
        LOGGER.info(String.format("%s[Inner %s] CoordinatorServer.putMetadata: %d", LOGGER.getName(), blockId, (end - start)));
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private TSDBQueryResponse execTSDBQueryOnDataStoreFog(UUID dataStoreFogId, TSDBQueryRequest tsdbQueryRequest) {
        final long start = System.currentTimeMillis();
        TSDBQueryResponse response = getDataStub(dataStoreFogId).execTSDBQueryLocal(tsdbQueryRequest);
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] CoordinatorServer.execTSDBQueryLocal: %d", LOGGER.getName(), (end - start)));
        return response;
    }

    private void sendMetadataToDataStoreFog(UUID dataStoreFogId, IndexMetadataRequest indexMetadataRequest) {
        final long start = System.currentTimeMillis();
        Response response = getDataStub(dataStoreFogId)
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .indexMetadataLocal(indexMetadataRequest);
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] DataServer.indexMetadataLocal: %d", LOGGER.getName(), (end - start)));
    }

    private void sendBlockToDataStoreFog(UUID dataStoreFogId, StoreBlockRequest storeBlockRequest) {
        final long start = System.currentTimeMillis();
        Response response = getDataStub(dataStoreFogId)
                .withDeadlineAfter(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
                .storeBlockLocal(storeBlockRequest);
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer] DataServer.storeBlockLocal: %d", LOGGER.getName(), (end - start)));
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
        final Polygon region = Utils.createPolygon(12.834, 13.1437, 77.4601, 77.784);
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

    private List<UUID> getSpatialShortlist(double minLat, double maxLat, double minLon, double maxLon) {
        return getSpatialShortlist(Utils.createPolygon(minLat, maxLat, minLon, maxLon));
    }

    private List<UUID> getSpatialShortlist(String minLat, String maxLat, String minLon, String maxLon) {
        return getSpatialShortlist(Double.parseDouble(minLat),
                Double.parseDouble(maxLat), Double.parseDouble(minLon), Double.parseDouble(maxLon));
    }

    private List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        final long start = System.currentTimeMillis();
        List<UUID> spatialShortlist =  fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local] CoordinatorServer.getSpatialShortlist: %d", LOGGER.getName(), (end - start)));
        LOGGER.info(String.format("%s[Count] CoordinatorServer.spatialShortlist: %d", LOGGER.getName(), spatialShortlist.size()));
        return spatialShortlist;
    }

    private List<UUID> getTemporalShortlist(TimeRange timeRange) {
        final long start = System.currentTimeMillis();
        List<Instant> timeChunks = Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK_SECONDS);
        List<UUID> temporalShortlist = timeChunks.stream().map(chunk -> fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(chunk), Constants.SEED_HASH) % numFogs))).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local] CoordinatorServer.getTemporalShortlist: %d", LOGGER.getName(), (end - start)));
        LOGGER.info(String.format("%s[Count] CoordinatorServer.temporalShortlist: %d", LOGGER.getName(), temporalShortlist.size()));
        return temporalShortlist;
    }

    private List<UUID> getTemporalShortlist(String start, String end) {
        final long startTime = System.currentTimeMillis();
        List<Instant> timeChunks = Utils.getTimeChunks(start, end, Constants.TIME_CHUNK_SECONDS);
        List<UUID> temporalShortlist = timeChunks.stream().map(chunk -> fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(chunk), Constants.SEED_HASH) % numFogs))).collect(Collectors.toList());
        final long endTime = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local] CoordinatorServer.getTemporalShortlist: %d", LOGGER.getName(), (endTime - startTime)));
        return temporalShortlist;
    }

    private Set<UUID> getFogsToReplicate(UUID blockId, List<UUID> spatialShortlist, List<UUID> temporalShortlist,
                                         UUID randomReplica, UUID temporalReplica, UUID spatialReplica) {
        LOGGER.info(String.format("%s[Insert] CoordinatorServer.randomReplica(%s): %s", LOGGER.getName(), blockId, randomReplica));
        Set<UUID> replicas = new HashSet<>(Set.of(randomReplica));
        if (replicas.contains(spatialReplica)) {
            boolean added = false;
            for (UUID spatialReplicaCandidate : spatialShortlist) {
                if (!replicas.contains(spatialReplicaCandidate)) {
                    replicas.add(spatialReplicaCandidate);
                    spatialReplica = spatialReplicaCandidate;
                    added = true;
                    break;
                }
            }
            if (!added) {
                for (UUID replicaCandidate : fogIds) {
                    replicas.add(replicaCandidate);
                    spatialReplica = replicaCandidate;
                    break;
                }
            }
        }
        else {
            replicas.add(spatialReplica);
        }
        if (replicas.contains(temporalReplica)) {
            boolean added = false;
            for (UUID temporalReplicaCandidate : temporalShortlist) {
                if (!replicas.contains(temporalReplicaCandidate)) {
                    replicas.add(temporalReplicaCandidate);
                    temporalReplica = temporalReplicaCandidate;
                    added = true;
                    break;
                }
            }
            if (!added) {
                for (UUID replicaCandidate : fogIds) {
                    replicas.add(replicaCandidate);
                    temporalReplica = replicaCandidate;
                    break;
                }
            }
        }
        else {
            replicas.add(temporalReplica);
        }
//        o: for (UUID spatialReplicaCandidate : spatialShortlist) {
//            for (UUID temporalReplicaCandidate : temporalShortlist) {
//                if (!spatialReplicaCandidate.equals(temporalReplicaCandidate)
//                        && !replicas.contains(spatialReplicaCandidate)
//                        && !replicas.contains(temporalReplicaCandidate)) {
//                    replicas.add(spatialReplicaCandidate);
//                    replicas.add(temporalReplicaCandidate);
//                    break o;
//                }
//            }
//        }
//        if (replicas.size() < 3) {
//            for (UUID replicaCandidate : fogIds) {
//                replicas.add(replicaCandidate);
//                if (replicas.size() >= 3) {
//                    break;
//                }
//            }
//        }
        LOGGER.info(String.format("%s[Insert] CoordinatorServer.spatialReplica(%s): %s", LOGGER.getName(), blockId, spatialReplica));
        LOGGER.info(String.format("%s[Insert] CoordinatorServer.temporalReplica(%s): %s", LOGGER.getName(), blockId, temporalReplica));
        return replicas;
    }

    private UUID getFogHashByBlockId(UUID blockId) {
        return fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(blockId), Constants.SEED_HASH) % numFogs));
    }

    private UUID getFogHashByBoundingBox(Polygon boundingBox) {
        org.locationtech.jts.geom.Point centroid = boundingBox.getCentroid();
        for (Map.Entry<UUID, FogPartition> entry : fogPartitions.entrySet()) {
            if (entry.getValue().getPolygon().intersects(centroid)) {
                return entry.getKey();
            }
        }
        return fogIds.get(0);
    }

    private UUID getFogHashByTimeRange(Instant startInstant, Instant endInstant) {
        Duration duration = Duration.between(startInstant, endInstant);
        Instant midInstant = startInstant.plus(duration.toMillis() / 2, ChronoUnit.MILLIS);
        Instant midChunk = Instant.ofEpochSecond(midInstant.getEpochSecond() - ((midInstant.getEpochSecond() - Instant.MIN.getEpochSecond()) % Constants.TIME_CHUNK_SECONDS));
        return fogIds.get((int) Math.abs(Constants.XXHASH64.hash(Utils.serializeObject(midChunk), Constants.SEED_HASH) % numFogs));
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
