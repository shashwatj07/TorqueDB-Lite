package com.dreamlab;

import com.dreamlab.constants.Cache;
import com.dreamlab.constants.Model;
import com.dreamlab.constants.QueryPolicy;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.TSDBQueryRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryResponse;
import com.dreamlab.query.InfluxDBQuery;
import com.dreamlab.types.FogInfo;
import com.dreamlab.utils.Utils;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ExecuteQuery {
    private ExecuteQuery() {
    }

    public static void main(String... args) throws IOException {

        final String fogId = args[0];
        final String fogConfigPath = args[1];
        final String start = args[2];
        final String stop = args[3];
        final String minLat = args[4];
        final String maxLat = args[5];
        final String minLon = args[6];
        final String maxLon = args[7];


        InfluxDBQuery influxDBQuery = new InfluxDBQuery();
        influxDBQuery.addBucketName("bucket");
        Instant startInstant = Utils.getInstantFromString(start);
        Instant endInstant = Utils.getInstantFromString(stop);
//        startInstant = startInstant.minus((timeRange * 60 - 300) / 2, ChronoUnit.SECONDS);
//        endInstant = endInstant.plus((timeRange * 60 - 300) / 2, ChronoUnit.SECONDS);
        influxDBQuery.addRange(Utils.getStringFromInstant(startInstant), Utils.getStringFromInstant(endInstant));
        influxDBQuery.addRegion(minLat, maxLat, minLon, maxLon);
        influxDBQuery.addFilter("pollution", List.of(), List.of());
        List<String> keep = Arrays.asList("_value", "_time");
        influxDBQuery.addKeep(keep);
        Model model = Model.FOG;
        Cache cache = Cache.FALSE;
        QueryPolicy queryPolicy = QueryPolicy.QP1;
        influxDBQuery.addOptionalParameters(model, cache, queryPolicy);
        influxDBQuery.addQueryId();

        final Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogConfigPath);
        final FogInfo parentFogInfo = fogDetails.get(UUID.fromString(fogId));
        ManagedChannel managedChannel = ManagedChannelBuilder
                .forAddress(parentFogInfo.getDeviceIP(), parentFogInfo.getDevicePort())
                .usePlaintext()
                .build();
        CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
        final long t1 = System.currentTimeMillis();
        try {
            ObjectOutputStream ostream;
            ByteArrayOutputStream bstream = new ByteArrayOutputStream();
            ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(influxDBQuery);
            ByteBuffer buffer = ByteBuffer.allocate(bstream.size());
            buffer.put(bstream.toByteArray());
            buffer.flip();
            System.out.println("Sending query object with query id " + influxDBQuery.getQueryId() + " from client.");
            TSDBQueryResponse tsdbQueryResponse = coordinatorServerBlockingStub.execTSDBQuery(TSDBQueryRequest.newBuilder().addFluxQuery(ByteString.copyFrom(buffer)).setQueryId(Utils.getMessageFromUUID(influxDBQuery.getQueryId())).build());
            final long t2 = System.currentTimeMillis();
            System.out.println(String.format("[Query %s] Lines: %d", influxDBQuery.getQueryId(), tsdbQueryResponse.getFluxQueryResponse().toStringUtf8().chars().filter(c -> c == '\n').count()));
            managedChannel.shutdown();
            System.out.println("Client " + "[Outer " + influxDBQuery.getQueryId() + "] CoordinatorServer.execTSDBQuery: " + (t2 - t1));
            System.out.println(tsdbQueryResponse.getFluxQueryResponse().toStringUtf8());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        managedChannel.shutdown();
    }
}
