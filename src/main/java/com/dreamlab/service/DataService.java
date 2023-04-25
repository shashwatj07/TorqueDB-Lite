package com.dreamlab.service;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BlockContentResponse;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.DataServerGrpc;
import com.dreamlab.edgefs.grpcServices.FindBlocksRequest;
import com.dreamlab.edgefs.grpcServices.FindBlocksResponse;
import com.dreamlab.edgefs.grpcServices.IndexMetadataRequest;
import com.dreamlab.edgefs.grpcServices.Response;
import com.dreamlab.edgefs.grpcServices.StoreBlockRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryResponse;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.BlockReplicaInfo;
import com.dreamlab.utils.Operation;
import com.dreamlab.utils.RetryOperation;
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2Shape;
import com.google.common.geometry.S2ShapeIndex;
import com.google.common.geometry.S2ShapeIndexRegion;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import io.grpc.stub.StreamObserver;
import okhttp3.OkHttpClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataService extends DataServerGrpc.DataServerImplBase {

    private static String BACKUP_DIR_PATH = "/home/ultraviolet/experiments/backup";
    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> metaMap;
    private final ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> timeMap;
//    private final ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> geoMap;
    private S2ShapeIndex geoIndex;
    private final ConcurrentMap<UUID, BlockReplicaInfo> blockIdMap;
    private final InfluxDBClient influxDBClient;
    private final WriteApiBlocking writeApi;
    private final QueryApi queryApi;
    private final UUID fogId;
    private final String serverIp;
    private final int serverPort;
    private final char[] token;
    private final Logger LOGGER;

    private long blocksStoredCount;

    public DataService(String serverIP, int serverPort, UUID fogId, String token) {
        LOGGER = Logger.getLogger(String.format("[Fog: %s] ", fogId.toString()));
        this.fogId = fogId;
        this.serverIp = serverIP;
        this.serverPort = serverPort;
        this.token = token.toCharArray();

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                .retryOnConnectionFailure(true);
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .authenticateToken(this.token)
                .org("org")
                .connectionString("http://localhost:8086?readTimeout=60m&connectTimeout=60m&writeTimeout=60m") // ?readTimeout=1m&connectTimeout=1m&writeTimeout=1m
                .okHttpClient(okHttpClient)
                .logLevel(LogLevel.BASIC)
                .bucket("bucket")
                .build();
        influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);
        writeApi = influxDBClient.getWriteApiBlocking();
        queryApi = influxDBClient.getQueryApi();
        ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> metaMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/metaMap", BACKUP_DIR_PATH, fogId));
            metaMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>>) map);
        } catch (Exception ex) {
            metaMapLocal = new ConcurrentHashMap<>();
        }
        metaMap = metaMapLocal;

        ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> timeMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/timeMap", BACKUP_DIR_PATH, fogId));
            timeMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>) map);
        } catch (Exception ex) {
            timeMapLocal = new ConcurrentHashMap<>();
        }
        timeMap = timeMapLocal;

        S2ShapeIndex geoIndexLocal;
        try {
            Object index = Utils.readObjectFromFile(String.format("%s/%s/geoMap", BACKUP_DIR_PATH, fogId));
            geoIndexLocal = (S2ShapeIndex) index;
        } catch (Exception ex) {
            geoIndexLocal = new S2ShapeIndex();
        }
        geoIndex = geoIndexLocal;

        ConcurrentMap<UUID, BlockReplicaInfo> blockIdMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/blockIdMap", BACKUP_DIR_PATH, fogId));
            blockIdMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<UUID, BlockReplicaInfo>) map);
        } catch (Exception ex) {
            blockIdMapLocal = new ConcurrentHashMap<>();
        }
        blockIdMap = blockIdMapLocal;
        blocksStoredCount = 0;
    }

    @Override
    public void indexMetadataLocal(IndexMetadataRequest request, StreamObserver<Response> responseObserver) {
        final UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
        LOGGER.info(String.format("%s[Insert %s] Indexing Metadata on %s:%d", LOGGER.getName(), blockId, serverIp, serverPort));
        final long start = System.currentTimeMillis();
        Response.Builder responseBuilder = Response.newBuilder();
        BlockReplicaInfo blockReplicaInfo =
                new BlockReplicaInfo(Utils.getUuidFromMessage(request.getBlockId()),
                        request.getBoundingBox().getBottomRightLatLon().getLatitude(),
                        request.getBoundingBox().getTopLeftLatLon().getLatitude(),
                        request.getBoundingBox().getTopLeftLatLon().getLongitude(),
                        request.getBoundingBox().getBottomRightLatLon().getLongitude(),
                        Utils.getInstantFromTimestampMessage(request.getTimeRange().getStartTimestamp()),
                        Utils.getInstantFromTimestampMessage(request.getTimeRange().getEndTimestamp()));
        request.getReplicasList()
                .stream()
                .map(Utils::getReplicaFromMessage)
                .forEach(blockReplicaInfo::addReplicaLocation);
        try {
            Map<String, String> metadataMap = request.getMetadataMapMap();
            blockIdMap.put(blockId, blockReplicaInfo);
            for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                metaMap.putIfAbsent(key, new ConcurrentHashMap<>());
                metaMap.get(key).putIfAbsent(value, new ConcurrentLinkedQueue<>());
                metaMap.get(key).get(value).add(blockReplicaInfo);
            }
            indexTemporal(blockId, request.getTimeRange(), blockReplicaInfo);
            indexSpatial(blockId, request.getBoundingBox(), blockReplicaInfo);
            responseBuilder.setIsSuccess(true);
        }
        catch (Exception e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] DataServer.indexMetadataLocal: %d", LOGGER.getName(), blockId, (end - start)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void storeBlockLocal(StoreBlockRequest request, StreamObserver<Response> responseObserver) {
        final UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
        LOGGER.info(String.format("%s[Insert %s] Storing Block on %s:%d", LOGGER.getName(), blockId, serverIp, serverPort));
        blocksStoredCount++;
        final long start = System.currentTimeMillis();
        Response.Builder responseBuilder = Response.newBuilder();
        String bucket = "bucket";
        String org = "org";
        final long t1 = System.currentTimeMillis();
        RetryOperation.doWithRetry(1, new Operation() {
            @Override
            public void doIt() {
                writeApi.writeRecord(WritePrecision.MS, request.getBlockContent().toStringUtf8());
            }
        });
        final long t2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] InfluxDB.writeRecord: %d", LOGGER.getName(), blockId, (t2 - t1)));
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] DataServer.storeBlockLocal: %d", LOGGER.getName(), blockId, (end - start)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void findBlocksLocal(FindBlocksRequest request, StreamObserver<FindBlocksResponse> responseObserver) {
        final UUID queryId = Utils.getUuidFromMessage(request.getQueryId());
        LOGGER.info(String.format("%s[Query %s] Finding Block on %s:%d", LOGGER.getName(), queryId, serverIp, serverPort));
        final long start = System.currentTimeMillis();
        FindBlocksResponse.Builder findBlockResponseBuilder = FindBlocksResponse.newBuilder();
        // initial capacity based on 2*avg
        Set<BlockReplicaInfo> relevantBlocks = new HashSet<>();
        long n1 = System.currentTimeMillis();
        List<Instant> timeChunks = Utils.getTimeChunks(request.getTimeRange(), Constants.TIME_CHUNK_SECONDS);
        long n2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local %s] DataServer.getTimeChunks: %d", LOGGER.getName(), queryId, (n2 - n1)));
        LOGGER.info(String.format("%s[Count %s] DataServer.timeChunks: %d", LOGGER.getName(), timeChunks.size(), (n2 - n1)));
        n1 = System.currentTimeMillis();
        List<S2CellId> s2CellIds = Utils.getCellIds(request.getBoundingBox(), Constants.MAX_S2_CELL_LEVEL);
        n2 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local %s] DataServer.getCellIds: %d", LOGGER.getName(), queryId, (n2 - n1)));
        LOGGER.info(String.format("%s[Count %s] DataServer.s2CellIds: %d", LOGGER.getName(), s2CellIds.size(), (n2 - n1)));
        if (!request.getIsAndQuery()) {
            LOGGER.info(String.format("%s[Query %s] OR Query", LOGGER.getName(), queryId));
            if (request.hasBlockId()) {
                UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
                if (blockIdMap.containsKey(blockId)) {
                    relevantBlocks.add(blockIdMap.get(blockId));
                }
            }
            if (request.hasTimeRange()) {
                for (Instant timeChunk : timeChunks) {
                    relevantBlocks.addAll(timeMap.getOrDefault(timeChunk.toString(), Constants.EMPTY_LIST_REPLICA));
                }
            }
            if (request.hasBoundingBox()) {
                S2ShapeIndexRegion s2ShapeIndexRegion = new S2ShapeIndexRegion(geoIndex);
                s2CellIds.forEach((s2CellId) -> s2ShapeIndexRegion.visitIntersectingShapes(new S2Cell(s2CellId), (s2Shape, b) -> {
                    relevantBlocks.add((BlockReplicaInfo) s2Shape);
                    return false;
                }));
//                for (S2CellId s2CellId : s2CellIds) {
//                    relevantBlocks.addAll(geoMap.getOrDefault(s2CellId.toToken(), Constants.EMPTY_LIST_REPLICA));
//                }
            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) {
                relevantBlocks.addAll(metaMap.getOrDefault(predicate.getKey(), Constants.EMPTY_MAP_STRING_LIST_REPLICA).getOrDefault(predicate.getValue(), Constants.EMPTY_LIST_REPLICA));
            }
            LOGGER.info(String.valueOf(relevantBlocks.size()));
        }
        else {
            LOGGER.info(String.format("%s[Query %s] AND Query", LOGGER.getName(), queryId));
            boolean flag = false;
            if (request.hasBlockId()) {
                LOGGER.info(LOGGER.getName() + "hasBlockId: true");
                flag = true;
                UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
                if (blockIdMap.containsKey(blockId)) { // timer
                    relevantBlocks.add(blockIdMap.get(blockId));
                }
            }
            if (request.hasTimeRange()) {
                LOGGER.info(LOGGER.getName() + "hasTimeRange: true");
                Set<BlockReplicaInfo> timeBlocks = new HashSet<>();
                n1 = System.currentTimeMillis();
                for (Instant timeChunk : timeChunks) {
                    timeBlocks.addAll(timeMap.getOrDefault(timeChunk.toString(), Constants.EMPTY_LIST_REPLICA));
                }
                n2 = System.currentTimeMillis();
                LOGGER.info(String.format("%s[Local %s] DataServer.addAllTimeBlocks: %d", LOGGER.getName(), queryId, (n2 - n1)));
                LOGGER.info(String.format("%s[Count %s] DataServer.timeBlocks: %d", LOGGER.getName(), queryId, timeBlocks.size()));
//                LOGGER.info(LOGGER.getName() + "timeBlocks " + timeBlocks);
                n1 = System.currentTimeMillis();
                if (flag) {
                    relevantBlocks.retainAll(timeBlocks); // timer
                }
                else {
                    flag = true;
                    relevantBlocks.addAll(timeBlocks); // timer
                }
                n2 = System.currentTimeMillis();
                LOGGER.info(String.format("%s[Local %s] DataServer.retainAllTimeBlocks: %d", LOGGER.getName(), queryId, (n2 - n1)));
                LOGGER.info(LOGGER.getName() + " Relevant Blocks so far: " + relevantBlocks.size());
            }
            if (request.hasBoundingBox()) {
                LOGGER.info(LOGGER.getName() + "hasBoundingBox: true");
                Set<BlockReplicaInfo> geoBlocks = new HashSet<>();
                n1 = System.currentTimeMillis();
                S2ShapeIndexRegion s2ShapeIndexRegion = new S2ShapeIndexRegion(geoIndex);
                s2CellIds.forEach((s2CellId) -> s2ShapeIndexRegion.visitIntersectingShapes(new S2Cell(s2CellId), (s2Shape, b) -> {
                    geoBlocks.add((BlockReplicaInfo) s2Shape);
                    return false;
                }));
//                for (S2CellId s2CellId : s2CellIds) { // timer outside
//                    ConcurrentLinkedQueue<BlockReplicaInfo> result = geoMap.get(s2CellId.toToken());
//                    if (result != null) {
//                        geoBlocks.addAll(result);
//                    }
//                }
                n2 = System.currentTimeMillis();
                LOGGER.info(String.format("%s[Local %s] DataServer.addAllGeoBlocks: %d", LOGGER.getName(), queryId, (n2 - n1)));
                LOGGER.info(String.format("%s[Count %s] DataServer.geoBlocks: %d", LOGGER.getName(), queryId, geoBlocks.size()));
//                LOGGER.info(LOGGER.getName() + "geoBlocks " + geoBlocks);
                n1 = System.currentTimeMillis();
                if (flag) {
                    relevantBlocks.retainAll(geoBlocks); // timer
                }
                else {
                    relevantBlocks.addAll(geoBlocks); // timer
                }
                n2 = System.currentTimeMillis();
                LOGGER.info(String.format("%s[Local %s] DataServer.retainAllGeoBlocks: %d", LOGGER.getName(), queryId, (n2 - n1)));

            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) { // timer outside
                relevantBlocks.retainAll(metaMap.getOrDefault(predicate.getKey(), Constants.EMPTY_MAP_STRING_LIST_REPLICA).getOrDefault(predicate.getValue(), Constants.EMPTY_LIST_REPLICA));
            }
        }
        findBlockResponseBuilder.addAllBlockIdReplicasMetadata(
                relevantBlocks.stream().map(BlockReplicaInfo::toMessage).collect(Collectors.toSet()));
        FindBlocksResponse findBlocksResponse = findBlockResponseBuilder.build();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] DataServer.findBlocksLocal: %d", LOGGER.getName(), queryId, (end - start)));
        LOGGER.info(String.format("%s[Query %s] relevantBlocks: %s", LOGGER.getName(), queryId, relevantBlocks.size()));
//        LOGGER.info(LOGGER.getName() + "findBlocksLocal Response: " + findBlocksResponse);
        responseObserver.onNext(findBlocksResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void execTSDBQueryLocal(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        final UUID queryId = Utils.getUuidFromMessage(request.getQueryId());
        LOGGER.info(String.format("%s[Query %s] Flux Queries ", LOGGER.getName(), queryId, request.getFluxQueryList()));
        final long start = System.currentTimeMillis();

        TSDBQueryResponse.Builder responseBuilder = TSDBQueryResponse.newBuilder();
        List<Future<String>> futureList = new ArrayList<>();
        final long t3 = System.currentTimeMillis();
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(Constants.N_THREADS);
            for (ByteString query : request.getFluxQueryList()) {
                String fluxQuery = query.toStringUtf8();
                final String[] response = {null};
                futureList.add(executorService.submit(() -> queryApi.queryRaw(fluxQuery)));
            }
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        final long t4 = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Outer %s] InfluxDB.forEach.queryRaw: %d", LOGGER.getName(), queryId, (t4 - t3)));

        StringBuilder responseBuffer = new StringBuilder();
        for (Future<String> future : futureList) {
            try {
                responseBuffer.append(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        responseBuilder.setFluxQueryResponse(ByteString.copyFromUtf8(responseBuffer.toString()));
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner %s] DataServer.execTSDBQueryLocal: %d", LOGGER.getName(), queryId, (end - start)));

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBlockContentLocal(UUIDMessage request, StreamObserver<BlockContentResponse> responseObserver) {
        final long start = System.currentTimeMillis();
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
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Inner] DataServer.getBlockContentLocal: %d", LOGGER.getName(), (end - start)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void backupIndexLocal(Empty request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        LOGGER.log(Level.INFO, LOGGER.getName() + "Backing Up Metadata");
        try {
            Utils.writeObjectToFile(getMetaMap(), String.format("%s/%s/metaMap", BACKUP_DIR_PATH, fogId));
            Utils.writeObjectToFile(getTimeMap(), String.format("%s/%s/timeMap", BACKUP_DIR_PATH, fogId));
            Utils.writeObjectToFile(getGeoIndex(), String.format("%s/%s/geoMap", BACKUP_DIR_PATH, fogId));
            Utils.writeObjectToFile(getBlockIdMap(), String.format("%s/%s/blockIdMap", BACKUP_DIR_PATH, fogId));
            responseBuilder.setIsSuccess(true);
        } catch (IOException e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void logIndexLocal(Empty request, StreamObserver<Response> responseObserver) {
//        LOGGER.info(String.format("%sMetaMap: %s\n", LOGGER.getName(), metaMap.toString()));
//        LOGGER.info(String.format("%sTimeMap: %s\n", LOGGER.getName(), timeMap.toString()));
//        LOGGER.info(String.format("%sGeoMap: %s\n", LOGGER.getName(), geoMap.toString()));
//        LOGGER.info(String.format("%sBlockIdMap: %s\n", LOGGER.getName(), blockIdMap.toString()));
        LOGGER.info(String.format("%s[Count] DataServer.blockIdsIndexed: %d", LOGGER.getName(), blockIdMap.size()));
        LOGGER.info(String.format("%s[Count] DataServer.timeChunksIndexed: %d", LOGGER.getName(), timeMap.size()));
        LOGGER.info(String.format("%s[Count] DataServer.geoRegionsIndexed: %d", LOGGER.getName(), geoIndex.getShapes().size()));
        LOGGER.info(String.format("%s[Count] DataServer.blocksStored: %d", LOGGER.getName(), blocksStoredCount));
        responseObserver.onNext(Response.newBuilder().setIsSuccess(true).build());
        responseObserver.onCompleted();
    }

    private void indexTemporal(UUID blockId, TimeRange timeRange, BlockReplicaInfo blockReplicaInfo) {
        final long start = System.currentTimeMillis();
        Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK_SECONDS)
                .forEach(instant -> {
                    timeMap.putIfAbsent(instant.toString(), new ConcurrentLinkedQueue<>());
                    timeMap.get(instant.toString()).add(blockReplicaInfo);
                });
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local %s] DataServer.indexTimestamp: %d", LOGGER.getName(), blockId, (end - start)));
    }

    private void indexSpatial(UUID blockId, BoundingBox boundingBox, BlockReplicaInfo blockReplicaInfo) {
        final long start = System.currentTimeMillis();
//        final double minLat = boundingBox.getBottomRightLatLon().getLatitude();
//        final double minLon = boundingBox.getTopLeftLatLon().getLongitude();
//        final double maxLat = boundingBox.getTopLeftLatLon().getLatitude();
//        final double maxLon = boundingBox.getBottomRightLatLon().getLongitude();

        geoIndex.add(blockReplicaInfo);

//        Iterable<S2CellId> cellIds = Utils.getCellIds(minLat, minLon, maxLat, maxLon, Constants.S2_CELL_LEVEL);

//        for (S2CellId s2CellId : cellIds) {
//            geoMap.putIfAbsent(s2CellId.toToken(), new ConcurrentLinkedQueue<>());
//            geoMap.get(s2CellId.toToken()).add(blockReplicaInfo);
//        }

        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s[Local %s] DataServer.indexS2CellIds: %d", LOGGER.getName(), blockId, (end - start)));
    }

    public ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> getMetaMap() {
        return metaMap;
    }

    public ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>> getTimeMap() {
        return timeMap;
    }

    public S2ShapeIndex getGeoIndex() {
        return geoIndex;
    }

    public ConcurrentMap<UUID, BlockReplicaInfo> getBlockIdMap() {
        return blockIdMap;
    }

}
