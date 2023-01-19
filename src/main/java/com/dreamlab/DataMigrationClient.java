package com.dreamlab;

import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.Cache;
import com.dreamlab.constants.JoinType;
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

    static final Logger LOGGER = Logger.getLogger("Client");

    public static void main(String[] args) throws IOException {

        TSDBQueryResponse answer = null;

            InfluxDBQuery qu1 = new InfluxDBQuery();
            InfluxDBQuery qu2 = new InfluxDBQuery();
            InfluxDBQuery qu3 = new InfluxDBQuery();

            Model model = Model.FOG;
            Cache cache = Cache.FALSE;
            QueryPolicy queryPolicy = QueryPolicy.QP1;

            String qClass = "Non-join";


                qu1.addBucketName("bucket"); // Influx
                qu1.addRange("2022-10-28 23-33-35", "2024-10-28 23-33-55");
                qu1.addRegion("13.0188576", "13.0213035", "77.4809204", "77.4818086");
//                "min_lat": 13.0188576,
//                "max_lat": 13.0213035,
//                "min_lon": 77.4809204,
//                "max_lon": 77.4818086,
                // First in filter is measurement, second is tag list, third is field list
                qu1.addFilter("pollution", Arrays.asList("application==traffic"),
                        Arrays.asList("aqi<1000"));
                qu1.addKeep(Arrays.asList("_value", "_time")); // Influx
//            qu1.addJoin(Arrays.asList("shard", "_time"), JoinType.INNER);
                qu1.addOptionalParameters(model, cache, queryPolicy);

                qu2.addBucketName("bucket"); // Influx
                qu2.addRange("2022-10-28 23-33-35", "2022-10-28 23-33-36");
                // First in filter is measurement, second is tag list, third is field list
//            qu2.addFilter("SF", Arrays.asList("city==San_Francisco"), Arrays.asList("temperature<1000"));
                qu2.addFilter("BL", Arrays.asList("application==traffic"), Arrays.asList("aqi>0"));
                qu2.addKeep(Arrays.asList("_value", "_time")); // InfluxDB
                qu2.addJoin(Arrays.asList("shard", "_time"), JoinType.INNER);
                qu2.addOptionalParameters(model, cache, queryPolicy);

//             qu1.addSum();
//            qu1.addMin();
//            qu1.addMax();
//            qu1.addCount();
//            qu1.addMean();
//            qu1.addWindow("1h", "max");

                // FOR JOIN
                qu3.addJoin(qu1, qu2, Arrays.asList("_time"), JoinType.INNER);
                qu3.addOptionalParameters(model, cache, queryPolicy);


            String generatedQueryId = "";
            try {
                ManagedChannel managedChannel = ManagedChannelBuilder
                        .forAddress("127.0.0.1", 8001)
                        .usePlaintext()
                        .build();
                CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);

//                qu1.experiment = experiment;

                if (qClass.equals("Join")) {
                    qu1.addQueryId();
                    qu2.addQueryId();
                    qu3.addQueryId();
                    generatedQueryId = qu3.getQueryId();
                } else if (qClass.equals("Non-join")) {
                    qu1.addQueryId();
                    generatedQueryId = qu1.getQueryId();
                } else {
                    LOGGER.severe("ERROR! SPECIFY QUERY TYPE");
                }

                if (qClass.equals("Join")) {
                    answer = perform(coordinatorServerBlockingStub, qu3);
                } else if (qClass.equals("Non-join")) {
                    answer = perform(coordinatorServerBlockingStub, qu1);
                } else {
                    LOGGER.severe("ERROR! SPECIFY QUERY TYPE");
                }

                managedChannel.shutdown();
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
