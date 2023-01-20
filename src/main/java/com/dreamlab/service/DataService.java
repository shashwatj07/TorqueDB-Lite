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
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2CellId;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class DataService extends DataServerGrpc.DataServerImplBase {

    private static String BACKUP_DIR_PATH = "backup";
    private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> metaMap;
    private final ConcurrentMap<Instant, ConcurrentLinkedQueue<BlockReplicaInfo>> timeMap;
    private final ConcurrentMap<S2CellId, ConcurrentLinkedQueue<BlockReplicaInfo>> geoMap;
    private final ConcurrentMap<UUID, BlockReplicaInfo> blockIdMap;
    private final InfluxDBClient influxDBClient;
    private final UUID fogId;
    private final String serverIp;
    private final int serverPort;
    private final char[] token;
    private final Logger LOGGER;

    public DataService(String serverIP, int serverPort, UUID fogId, String token) {
        LOGGER = Logger.getLogger(String.format("[Fog: %s] ", fogId.toString()));
        this.fogId = fogId;
        this.serverIp = serverIP;
        this.serverPort = serverPort;
        this.token = token.toCharArray();
        influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", this.token);
        ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> metaMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/metaMap", BACKUP_DIR_PATH, fogId));
            metaMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>>) map);
        } catch (Exception ex) {
            metaMapLocal = new ConcurrentHashMap<>();
        }
        metaMap = metaMapLocal;

        ConcurrentMap<Instant, ConcurrentLinkedQueue<BlockReplicaInfo>> timeMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/timeMap", BACKUP_DIR_PATH, fogId));
            timeMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<Instant, ConcurrentLinkedQueue<BlockReplicaInfo>>) map);
        } catch (Exception ex) {
            timeMapLocal = new ConcurrentHashMap<>();
        }
        timeMap = timeMapLocal;

        ConcurrentMap<S2CellId, ConcurrentLinkedQueue<BlockReplicaInfo>> geoMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/geoMap", BACKUP_DIR_PATH, fogId));
            geoMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<S2CellId, ConcurrentLinkedQueue<BlockReplicaInfo>>) map);
        } catch (Exception ex) {
            geoMapLocal = new ConcurrentHashMap<>();
        }
        geoMap = geoMapLocal;

        ConcurrentMap<UUID, BlockReplicaInfo> blockIdMapLocal;
        try {
            Object map = Utils.readObjectFromFile(String.format("%s/%s/blockIdMap", BACKUP_DIR_PATH, fogId));
            blockIdMapLocal = new ConcurrentHashMap<>((ConcurrentHashMap<UUID, BlockReplicaInfo>) map);
        } catch (Exception ex) {
            blockIdMapLocal = new ConcurrentHashMap<>();
        }
        blockIdMap = blockIdMapLocal;

    }

    @Override
    public void indexMetadataLocal(IndexMetadataRequest request, StreamObserver<Response> responseObserver) {
        LOGGER.info(LOGGER.getName() + String.format("Indexing Metadata on %s:%d", serverIp, serverPort));
        final long start = System.currentTimeMillis();
        Response.Builder responseBuilder = Response.newBuilder();
        BlockReplicaInfo blockReplicaInfo = new BlockReplicaInfo(Utils.getUuidFromMessage(request.getBlockId()));
        request.getReplicasList()
                .stream()
                .map(Utils::getReplicaFromMessage)
                .forEach(blockReplicaInfo::addReplicaLocation);
        try {
            Map<String, String> metadataMap = request.getMetadataMapMap();
            UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
            blockIdMap.put(blockId, blockReplicaInfo);
            for (Map.Entry<String, String> entry : metadataMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                metaMap.putIfAbsent(key, new ConcurrentHashMap<>());
                metaMap.get(key).putIfAbsent(value, new ConcurrentLinkedQueue<>());
                metaMap.get(key).get(value).add(blockReplicaInfo);
            }
            indexTimestamp(request.getTimeRange(), blockReplicaInfo);
            indexS2CellIds(request.getBoundingBox(), blockReplicaInfo);
            responseBuilder.setIsSuccess(true);
        }
        catch (Exception e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s [Inner] DataServer.indexMetadataLocal: %d", LOGGER.getName(), (end - start)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void storeBlockLocal(StoreBlockRequest request, StreamObserver<Response> responseObserver) {
        LOGGER.info(LOGGER.getName() + String.format("Storing Block on %s:%d", serverIp, serverPort));
        final long start = System.currentTimeMillis();
        Response.Builder responseBuilder = Response.newBuilder();
        String bucket = "bucket";
        String org = "org";
//        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token);
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
        writeApi.writeRecord(bucket, org, WritePrecision.MS, request.getBlockContent().toStringUtf8());
//        client.close();
        final long end = System.currentTimeMillis();
        LOGGER.info(String.format("%s [Inner] DataServer.storeBlockLocal: %d", LOGGER.getName(), (end - start)));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void findBlocksLocal(FindBlocksRequest request, StreamObserver<FindBlocksResponse> responseObserver) {
        LOGGER.info(LOGGER.getName() + String.format("Finding Block on %s:%d", serverIp, serverPort));
        FindBlocksResponse.Builder findBlockResponseBuilder = FindBlocksResponse.newBuilder();
        Set<BlockReplicaInfo> relevantBlocks = new HashSet<>();
        List<Instant> timeChunks = Utils.getTimeChunks(request.getTimeRange(), Constants.TIME_CHUNK_SECONDS);
        List<S2CellId> s2CellIds = Utils.getCellIds(request.getBoundingBox(), Constants.S2_CELL_LEVEL);
        if (!request.getIsAndQuery()) {
            LOGGER.info(LOGGER.getName() + "OR Query");
            if (request.hasBlockId()) {
                UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
                if (blockIdMap.containsKey(blockId)) {
                    relevantBlocks.add(blockIdMap.get(blockId));
                }
            }
            if (request.hasTimeRange()) {
                for (Instant timeChunk : timeChunks) {
                    relevantBlocks.addAll(timeMap.getOrDefault(timeChunk, Constants.EMPTY_LIST_REPLICA));
                }
            }
            if (request.hasBoundingBox()) {
                for (S2CellId s2CellId : s2CellIds) {
                    relevantBlocks.addAll(geoMap.getOrDefault(s2CellId, Constants.EMPTY_LIST_REPLICA));
                }
            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) {
                relevantBlocks.addAll(metaMap.getOrDefault(predicate.getKey(), Constants.EMPTY_MAP_STRING_LIST_REPLICA).getOrDefault(predicate.getValue(), Constants.EMPTY_LIST_REPLICA));
            }
            LOGGER.info(String.valueOf(relevantBlocks.size()));
        }
        else {
            LOGGER.info(LOGGER.getName() + "AND Query");
            boolean flag = false;
            if (request.hasBlockId()) {
                flag = true;
                UUID blockId = Utils.getUuidFromMessage(request.getBlockId());
                if (blockIdMap.containsKey(blockId)) {
                    relevantBlocks.add(blockIdMap.get(blockId));
                }
            }
            if (request.hasTimeRange()) {
                if (flag) {
                    Set<BlockReplicaInfo> timeBlocks = new HashSet<>();
                    for (Instant timeChunk : timeChunks) {
                        timeBlocks.addAll(timeMap.getOrDefault(timeChunk, Constants.EMPTY_LIST_REPLICA));
                    }
                    relevantBlocks.retainAll(timeBlocks);
                }
                else {
                    flag = true;
                    for (Instant timeChunk : timeChunks) {
                        relevantBlocks.addAll(timeMap.getOrDefault(timeChunk, Constants.EMPTY_LIST_REPLICA));
                    }
                }

            }
            if (request.hasBoundingBox()) {
                if (flag) {
                    Set<BlockReplicaInfo> geoBlocks = new HashSet<>();
                    for (S2CellId s2CellId : s2CellIds) {
                        geoBlocks.addAll(geoMap.getOrDefault(s2CellId, Constants.EMPTY_LIST_REPLICA));
                    }
                    relevantBlocks.retainAll(geoBlocks);
                }
                else {
                    flag = true;
                    for (S2CellId s2CellId : s2CellIds) {
                        relevantBlocks.addAll(geoMap.getOrDefault(s2CellId, Constants.EMPTY_LIST_REPLICA));
                    }
                }
            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) {
                relevantBlocks.retainAll(metaMap.getOrDefault(predicate.getKey(), Constants.EMPTY_MAP_STRING_LIST_REPLICA).getOrDefault(predicate.getValue(), Constants.EMPTY_LIST_REPLICA));
            }
        }
        findBlockResponseBuilder.addAllBlockIdReplicasMetadata(
                relevantBlocks.stream().map(BlockReplicaInfo::toMessage).collect(Collectors.toSet()));
        FindBlocksResponse findBlocksResponse = findBlockResponseBuilder.build();
        LOGGER.info(LOGGER.getName() + relevantBlocks);
        LOGGER.info(LOGGER.getName() + serverPort + "\n" + findBlocksResponse);
        responseObserver.onNext(findBlocksResponse);
        responseObserver.onCompleted();
    }

    @Override
    public void execTSDBQueryLocal(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        LOGGER.info(LOGGER.getName() + String.format("Executing Query on %s:%d", serverIp, serverPort));
        TSDBQueryResponse.Builder responseBuilder = TSDBQueryResponse.newBuilder();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String response = queryApi.queryRaw(request.getFluxQuery().toStringUtf8(), "org");
        LOGGER.info(LOGGER.getName() + "InfluxDB Response " + response);
        responseBuilder.setFluxQueryResponse(ByteString.copyFromUtf8(response));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getBlockContentLocal(UUIDMessage request, StreamObserver<BlockContentResponse> responseObserver) {
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

    @Override
    public void backupIndexLocal(Empty request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        LOGGER.log(Level.INFO, LOGGER.getName() + "Backing Up Metadata");
        try {
            Utils.writeObjectToFile(getMetaMap(), String.format("%s/%s/metaMap", BACKUP_DIR_PATH, fogId));
            Utils.writeObjectToFile(getTimeMap(), String.format("%s/%s/timeMap", BACKUP_DIR_PATH, fogId));
            Utils.writeObjectToFile(getGeoMap(), String.format("%s/%s/geoMap", BACKUP_DIR_PATH, fogId));
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
        LOGGER.info(String.format("%sBlocks Indexed: %d", LOGGER.getName(), blockIdMap.size()));
        responseObserver.onNext(Response.newBuilder().setIsSuccess(true).build());
        responseObserver.onCompleted();
    }

    private void indexTimestamp(TimeRange timeRange, BlockReplicaInfo blockReplicaInfo) {
        Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK_SECONDS)
                .forEach(instant -> {
                    timeMap.putIfAbsent(instant, new ConcurrentLinkedQueue<>());
                    timeMap.get(instant).add(blockReplicaInfo);
                });
    }

    private void indexS2CellIds(BoundingBox boundingBox, BlockReplicaInfo blockReplicaInfo) {
        final double minLat = boundingBox.getBottomRightLatLon().getLatitude();
        final double minLon = boundingBox.getTopLeftLatLon().getLongitude();
        final double maxLat = boundingBox.getTopLeftLatLon().getLatitude();
        final double maxLon = boundingBox.getBottomRightLatLon().getLongitude();

        Iterable<S2CellId> cellIds = Utils.getCellIds(minLat, minLon, maxLat, maxLon, Constants.S2_CELL_LEVEL);

        for (S2CellId s2CellId : cellIds) {
            geoMap.putIfAbsent(s2CellId, new ConcurrentLinkedQueue<>());
            geoMap.get(s2CellId).add(blockReplicaInfo);
        }
    }

    public ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<BlockReplicaInfo>>> getMetaMap() {
        return metaMap;
    }

    public ConcurrentMap<Instant, ConcurrentLinkedQueue<BlockReplicaInfo>> getTimeMap() {
        return timeMap;
    }

    public ConcurrentMap<S2CellId, ConcurrentLinkedQueue<BlockReplicaInfo>> getGeoMap() {
        return geoMap;
    }

    public ConcurrentMap<UUID, BlockReplicaInfo> getBlockIdMap() {
        return blockIdMap;
    }

}
