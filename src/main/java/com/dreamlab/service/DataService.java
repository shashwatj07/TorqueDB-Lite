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
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataService extends DataServerGrpc.DataServerImplBase {

    private final Map<String, Map<String, ArrayList<BlockReplicaInfo>>> metaMap;
    private final Map<Instant, ArrayList<BlockReplicaInfo>> timeMap;
    private final Map<S2CellId, ArrayList<BlockReplicaInfo>> geoMap;
    private final UUID fogId;
    private final String serverIp;
    private final char[] token;

    public DataService(String serverIP, int serverPort, UUID fogId, String token) {
        this.fogId = fogId;
        this.serverIp = serverIP;
        metaMap = new HashMap<>();
        timeMap = new HashMap<>();
        geoMap = new HashMap<>();
        this.token = token.toCharArray();
    }
    @Override
    public void indexMetadataLocal(IndexMetadataRequest request, StreamObserver<Response> responseObserver) {
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
                if(!metaMap.containsKey(key)) {
                    metaMap.put(key, new HashMap<>());
                }
                if(!metaMap.get(key).containsKey(value)) {
                    metaMap.get(key).put(value, new ArrayList<>(List.of(blockReplicaInfo)));
                }
                else {
                    metaMap.get(key).get(value).add(blockReplicaInfo);
                }
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
    public void storeBlockLocal(StoreBlockRequest request, StreamObserver<Response> responseObserver) {
        Response.Builder responseBuilder = Response.newBuilder();
        File fogStoreDir = new File(String.format("store/%s", fogId));
        fogStoreDir.mkdirs();
        File contentsFile = new File(String.format("store/%s/%s.bin", fogId, Utils.getUuidFromMessage(request.getBlockId())));
        try (PrintWriter contentsFileWriter = new PrintWriter(contentsFile, StandardCharsets.UTF_8)) {
            contentsFileWriter.println(request.getBlockContent().toStringUtf8());
            contentsFileWriter.close();
            responseBuilder.setIsSuccess(true);
        } catch (IOException e) {
            e.printStackTrace();
            responseBuilder.setIsSuccess(false);
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void findBlocksLocal(FindBlocksRequest request, StreamObserver<FindBlocksResponse> responseObserver) {
        FindBlocksResponse.Builder findBlockResponseBuilder = FindBlocksResponse.newBuilder();
        Set<BlockReplicaInfo> relevantBlocks = new HashSet<>();
        List<Instant> timeChunks = Utils.getTimeChunks(request.getTimeRange(), Constants.TIME_CHUNK);
        List<S2CellId> s2CellIds = Utils.getCellIds(request.getBoundingBox());
        if (request.getIsOrQuery()) {
            for (Instant timeChunk : timeChunks) {
                relevantBlocks.addAll(timeMap.get(timeChunk));
            }
            for (S2CellId s2CellId : s2CellIds) {
                relevantBlocks.addAll(geoMap.get(s2CellId));
            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) {
                relevantBlocks.addAll(metaMap.get(predicate.getKey()).get(predicate.getValue()));
            }
        }
        else {
            for (Instant timeChunk : timeChunks) {
                // TODO: Check if timeChunks is empty
                relevantBlocks.addAll(timeMap.get(timeChunk));
            }
            for (S2CellId s2CellId : s2CellIds) {
                relevantBlocks.retainAll(geoMap.get(s2CellId));
            }
            for (Map.Entry<String, String> predicate : request.getMetadataMapMap().entrySet()) {
                relevantBlocks.retainAll(metaMap.get(predicate.getKey()).get(predicate.getValue()));
            }
        }
        findBlockResponseBuilder.addAllBlockIdReplicasMetadata(
                relevantBlocks.stream().map(BlockReplicaInfo::toMessage).collect(Collectors.toList()));
        responseObserver.onNext(findBlockResponseBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void execTSDBQueryLocal(TSDBQueryRequest request, StreamObserver<TSDBQueryResponse> responseObserver) {
        TSDBQueryResponse.Builder responseBuilder = TSDBQueryResponse.newBuilder();
        InfluxDBClient influxDBClient = InfluxDBClientFactory.create(serverIp + "8086", token);

        QueryApi queryApi = influxDBClient.getQueryApi();
        String response = queryApi.queryRaw(request.getFluxQuery());
        influxDBClient.close();
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

    private void indexTimestamp(TimeRange timeRange, BlockReplicaInfo blockReplicaInfo) {
        Utils.getTimeChunks(timeRange, Constants.TIME_CHUNK)
                .forEach(instant -> {
                    if (timeMap.containsKey(instant)) {
                        timeMap.get(instant).add(blockReplicaInfo);
                    }
                    else {
                        timeMap.put(instant, new ArrayList<>(Collections.singletonList(blockReplicaInfo)));
                    }
                });
    }

    private void indexS2CellIds(BoundingBox boundingBox, BlockReplicaInfo blockReplicaInfo) {
        final double minLat = boundingBox.getBottomRightLatLon().getLatitude();
        final double minLon = boundingBox.getTopLeftLatLon().getLongitude();
        final double maxLat = boundingBox.getTopLeftLatLon().getLatitude();
        final double maxLon = boundingBox.getBottomRightLatLon().getLongitude();

        Iterable<S2CellId> cellIds = Utils.getCellIds(minLat, minLon, maxLat, maxLon);

        for (S2CellId s2CellId : cellIds) {
            if (geoMap.containsKey(s2CellId)) {
                geoMap.get(s2CellId).add(blockReplicaInfo);
            }
            else {
                geoMap.put(s2CellId, new ArrayList<>(Collections.singletonList(blockReplicaInfo)));
            }
        }
    }
}
