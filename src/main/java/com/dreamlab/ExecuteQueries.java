package com.dreamlab;

import com.dreamlab.api.TSDBQuery;
import com.dreamlab.constants.Cache;
import com.dreamlab.constants.Constants;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.logging.Logger;

public class ExecuteQueries {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    public static void main(String[] args) throws IOException, InterruptedException {
        final int count = Integer.parseInt(args[0]);
        final String fogsConfigFilePath = args[1];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        List<UUID> fogIds = new ArrayList<>(fogDetails.keySet());
        List<String> tags = Files.readAllLines(Path.of(args[2]), StandardCharsets.UTF_8);
        List<String> fields = Files.readAllLines(Path.of(args[3]), StandardCharsets.UTF_8);
        List<String> ranges = Files.readAllLines(Path.of(args[4]), StandardCharsets.UTF_8);
        List<String> regions = Files.readAllLines(Path.of(args[5]), StandardCharsets.UTF_8);
        int interval = Integer.parseInt(args[6]);
        List<String> keep = Arrays.asList("_value", "_time");
        String measurement = "pollution";
        String bucket = "bucket";
        Model model = Model.FOG;
        Cache cache = Cache.FALSE;
        QueryPolicy queryPolicy = QueryPolicy.QP1;
        for (int queryIndex = 0; queryIndex < count; queryIndex++) {
            final int fogIndex = Constants.RANDOM.nextInt(fogIds.size());
            final FogInfo fogInfo = fogDetails.get(fogIds.get(fogIndex));
            System.out.println("Executing Query on " + fogInfo.getDeviceId());
            InfluxDBQuery influxDBQuery = new InfluxDBQuery();
            influxDBQuery.addBucketName(bucket);
            String range = ranges.get(Constants.RANDOM.nextInt(ranges.size()));
            StringTokenizer rangeTokenizer = new StringTokenizer(range, ",");
            String start = rangeTokenizer.nextToken();
            String stop = rangeTokenizer.nextToken();
            influxDBQuery.addRange(start, stop);
            String region = regions.get(Constants.RANDOM.nextInt(regions.size()));
            StringTokenizer regionTokenizer = new StringTokenizer(region, ",");
            String minLat = regionTokenizer.nextToken();
            String maxLat = regionTokenizer.nextToken();
            String minLon = regionTokenizer.nextToken();
            String maxLon = regionTokenizer.nextToken();
            influxDBQuery.addRegion(minLat, maxLat, minLon, maxLon);
            influxDBQuery.addFilter(measurement, getTagFilterList(tags.get(Constants.RANDOM.nextInt(tags.size()))),
                    Arrays.asList(fields.get(Constants.RANDOM.nextInt(fields.size()))));
            influxDBQuery.addKeep(keep);
            influxDBQuery.addOptionalParameters(model, cache, queryPolicy);
            influxDBQuery.addQueryId();
            LOGGER.info(LOGGER.getName() + influxDBQuery.getOperations());
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(fogInfo.getDeviceIP(), fogInfo.getDevicePort())
                    .usePlaintext()
                    .build();
            CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
            final long t1 = System.currentTimeMillis();
            TSDBQueryResponse answer = perform(coordinatorServerBlockingStub, influxDBQuery);
            final long t2 = System.currentTimeMillis();
            managedChannel.shutdown();
            LOGGER.info(LOGGER.getName() + "[Outer] CoordinatorServer.execTSDBQuery: " + (t2 - t1));
            final long sleepTime = interval * 1000L - (t2 - t1);
            Thread.sleep(sleepTime >= 0? sleepTime : 0);
            LOGGER.info(answer.getFluxQueryResponse().toStringUtf8());
        }
    }

    private static List<String> getTagFilterList(String tagFilterString) {
        StringTokenizer stringTokenizer = new StringTokenizer(tagFilterString, ",");
        List<String> list = new ArrayList<>();
        while (stringTokenizer.hasMoreTokens()) {
            list.add(stringTokenizer.nextToken());
        }
        return list;
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