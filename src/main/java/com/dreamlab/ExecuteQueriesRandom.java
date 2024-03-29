package com.dreamlab;

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
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ExecuteQueriesRandom {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    public static void main(String[] args) throws IOException, InterruptedException {
        final int count = Integer.parseInt(args[0]);
        final String fogsConfigFilePath = args[1];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        List<UUID> fogIds = new ArrayList<>(fogDetails.keySet());
        List<String> tags = Files.readAllLines(Path.of(args[2]), StandardCharsets.UTF_8);
        List<String> fields = Files.readAllLines(Path.of(args[3]), StandardCharsets.UTF_8);
        String metadataDirPath = args[4];
        int timeRange = Integer.parseInt(args[5]);
        double spatialRegion = Double.parseDouble(args[6]);
        int interval = Integer.parseInt(args[7]);
        String costModel = args[8];
        File metadataDir = new File(metadataDirPath);
        ArrayList<File> metadataFiles = new ArrayList<>();
        for (File dir : metadataDir.listFiles()) {
            for (File file : dir.listFiles()) {
                metadataFiles.add(file);
            }
        }
        List<String> keep = Arrays.asList("_value", "_time");
        String measurement = "pollution";
        String bucket = "bucket";
        Model model = Model.FOG;
        Cache cache = Cache.FALSE;
        QueryPolicy queryPolicy = QueryPolicy.valueOf(costModel);
        for (int queryIndex = 0; queryIndex < count; queryIndex++) {
            final int blockIndex = Constants.RANDOM.nextInt(metadataFiles.size());
            System.out.println(metadataFiles.get(blockIndex).toPath());
            JSONObject jsonObject = new JSONObject(Files.readString(metadataFiles.get(blockIndex).toPath()));
            System.out.println(Files.readString(metadataFiles.get(blockIndex).toPath()));
            final int fogIndex = Constants.RANDOM.nextInt(20); // TODO: Hardcoded
            final FogInfo fogInfo = fogDetails.get(fogIds.get(fogIndex));
            System.out.println("Executing Query on " + fogInfo.getDeviceId());
            InfluxDBQuery influxDBQuery = new InfluxDBQuery();
            influxDBQuery.addBucketName(bucket);
            String start = jsonObject.getString("startTS");
            String end = jsonObject.getString("endTS");
            Instant startInstant = Utils.getInstantFromString(start);
            Instant endInstant = Utils.getInstantFromString(end);
            Duration duration = Duration.between(startInstant, endInstant);
            Instant midInstant = startInstant.plus(duration.dividedBy(2));
            int offset = Constants.RANDOM.nextInt(timeRange * 60);
            startInstant = midInstant.minus(offset, ChronoUnit.SECONDS);
            endInstant = midInstant.plus(timeRange * 60L - offset, ChronoUnit.SECONDS);
            influxDBQuery.addRange(Utils.getStringFromInstant(startInstant), Utils.getStringFromInstant(endInstant));
            double midLat = (jsonObject.getDouble("min_lat") + jsonObject.getDouble("max_lat")) / 2.0;
            double midLon = (jsonObject.getDouble("min_lon") + jsonObject.getDouble("max_lon")) / 2.0;
            double offsetLat = Constants.RANDOM.nextDouble() * spatialRegion;
            String minLat = String.valueOf(midLat - offsetLat);
            String maxLat = String.valueOf(midLat + (spatialRegion - offsetLat));
            double offsetLon = Constants.RANDOM.nextDouble() * spatialRegion;
            String minLon = String.valueOf(midLon - offsetLon);
            String maxLon = String.valueOf(midLon + (spatialRegion - offsetLon));
            if (true) { // TODO
                influxDBQuery.addRegion(minLat, maxLat, minLon, maxLon);
                influxDBQuery.addFilter(measurement, List.of(), List.of());
//                influxDBQuery.addFilter(measurement, getTagFilterList(tags.get(Constants.RANDOM.nextInt(tags.size()))),
//                        List.of());
            }
            else {
                influxDBQuery.addFilter(measurement, getTagFilterList(tags.get(Constants.RANDOM.nextInt(tags.size()))),
                        Collections.singletonList(fields.get(Constants.RANDOM.nextInt(fields.size()))));
            }
            influxDBQuery.addKeep(keep);
            influxDBQuery.addOptionalParameters(model, cache, queryPolicy);
            influxDBQuery.addQueryId();
            LOGGER.info(LOGGER.getName() + influxDBQuery.getOperations());
            int fogNo = Integer.parseInt(fogInfo.getDeviceIP().substring(fogInfo.getDeviceIP().lastIndexOf(".") + 1));
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(String.format("172.17.0.%d", 101 + fogNo), fogInfo.getDevicePort())
                    .usePlaintext().keepAliveTime(Long.MAX_VALUE, TimeUnit.DAYS)
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
                LOGGER.info("Sending query object with query id " + influxDBQuery.getQueryId() + " from client.");
                TSDBQueryResponse tsdbQueryResponse = coordinatorServerBlockingStub.execTSDBQuery(TSDBQueryRequest.newBuilder().addFluxQuery(ByteString.copyFrom(buffer)).setQueryId(Utils.getMessageFromUUID(influxDBQuery.getQueryId())).build());
                final long t2 = System.currentTimeMillis();
                LOGGER.info(String.format("[Query %s] Lines: %d", influxDBQuery.getQueryId(), tsdbQueryResponse.getFluxQueryResponse().toStringUtf8().chars().filter(c -> c == '\n').count()));
                managedChannel.shutdown();
                LOGGER.info(LOGGER.getName() + "[Outer " + influxDBQuery.getQueryId() + "] CoordinatorServer.execTSDBQuery: " + (t2 - t1));
                final long sleepTime = interval * 1000L - (t2 - t1);
                Thread.sleep(sleepTime >= 0? sleepTime : 0);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
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

}
