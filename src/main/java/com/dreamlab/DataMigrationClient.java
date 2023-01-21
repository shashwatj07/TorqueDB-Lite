package com.dreamlab;

import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.Cache;
import com.dreamlab.constants.Model;
import com.dreamlab.constants.QueryPolicy;
import com.dreamlab.edgefs.grpcServices.CoordinatorServerGrpc;
import com.dreamlab.edgefs.grpcServices.TSDBQueryRequest;
import com.dreamlab.edgefs.grpcServices.TSDBQueryResponse;
import com.dreamlab.query.InfluxDBQuery;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

public class DataMigrationClient {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    public static void main(String[] args) throws IOException {
        TSDBQueryResponse answer;
        InfluxDBQuery qu1 = new InfluxDBQuery();
        Model model = Model.FOG;
        Cache cache = Cache.FALSE;
        QueryPolicy queryPolicy = QueryPolicy.QP1;
        qu1.addBucketName("bucket"); // Influx
        qu1.addRange("2023-01-01 23-33-35", "2023-01-01 23-53-55");
        qu1.addRegion("13.0188576", "13.0213035", "77.4809204", "77.4818086");
        // First in filter is measurement, second is tag list, third is field list
        qu1.addFilter("pollution", Arrays.asList("application==traffic", "drone==dji-matrice"),
                Arrays.asList("aqi<1000"));
        qu1.addKeep(Arrays.asList("_value", "_time")); // Influx
//            qu1.addJoin(Arrays.asList("shard", "_time"), JoinType.INNER);
        qu1.addOptionalParameters(model, cache, queryPolicy);

        try {
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(args[0], Integer.parseInt(args[1]))
                    .usePlaintext()
                    .build();
            CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
            qu1.addQueryId();
            final long start = System.currentTimeMillis();
            answer = perform(coordinatorServerBlockingStub, qu1);
            final long end = System.currentTimeMillis();
            managedChannel.shutdown();
            LOGGER.info(LOGGER.getName() + "[Outer] CoordinatorServer.execTSDBQuery: " + (end - start));
            LOGGER.info(answer.getFluxQueryResponse().toStringUtf8());
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    private static TSDBQueryResponse perform(CoordinatorServerGrpc.CoordinatorServerBlockingStub client, TSDBQuery query) {
        try {
            ObjectOutputStream ostream;
            ByteArrayOutputStream bstream = new ByteArrayOutputStream();
            ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(query);
            ByteBuffer buffer = ByteBuffer.allocate(bstream.size());
            buffer.put(bstream.toByteArray());
            buffer.flip();
            LOGGER.info("Sending query object with query id " + query.getQueryId() + " from client.");
            return client.execTSDBQuery(TSDBQueryRequest.newBuilder().setFluxQuery(ByteString.copyFrom(buffer)).build());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
