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
import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.QueryApi;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import okhttp3.OkHttpClient;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ExecuteQueries {

    static final Logger LOGGER = Logger.getLogger("[Client] ");

    public static void main(String[] args) throws IOException, InterruptedException {
        final int count = Integer.parseInt(args[0]);
        final String fogsConfigFilePath = args[1];
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails(fogsConfigFilePath);
        List<UUID> fogIds = new ArrayList<>(fogDetails.keySet());
        String workload = args[2];
        int interval = Integer.parseInt(args[3]);
        final String queryFilePath = args[4];
        int numClients = Integer.parseInt(args[5]);
        int index = Integer.parseInt(args[6]);
        String costModel = args[7];
        boolean queryCloud = Boolean.parseBoolean(args[8]);

        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(Files.readString(Paths.get(queryFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONObject queries = (JSONObject) jsonObject.get(workload);
        List<String> queryIds = new ArrayList<>(queries.keySet());
        int numQueries = count / numClients;
        Collections.sort(queryIds);
        queryIds = queryIds.subList((index - 1) * numQueries, index * numQueries);
        for (String queryId : queryIds) {
            JSONObject params = (JSONObject) queries.get(queryId);
            InfluxDBQuery influxDBQuery = new InfluxDBQuery();
            influxDBQuery.addBucketName("bucket");
            String start = params.getString("start");
            String end = params.getString("stop");
            Instant startInstant = Utils.getInstantFromString(start);
            Instant endInstant = Utils.getInstantFromString(end);
            influxDBQuery.addRange(Utils.getStringFromInstant(startInstant), Utils.getStringFromInstant(endInstant));
            influxDBQuery.addRegion(params.getString("minLat"), params.getString("maxLat"), params.getString("minLon"), params.getString("maxLon"));
            influxDBQuery.addFilter("pollution", List.of(), List.of());
            influxDBQuery.addKeep(Arrays.asList("_value", "_time"));
            Model model = Model.FOG;
            Cache cache = Cache.FALSE;
            QueryPolicy queryPolicy = QueryPolicy.valueOf(costModel);
            influxDBQuery.addOptionalParameters(model, cache, queryPolicy);
            influxDBQuery.addQueryId(queryId);

            System.out.println(influxDBQuery);

//            final int fogIndex = Constants.RANDOM.nextInt(fogIds.size());
//            final FogInfo fogInfo = fogDetails.get(fogIds.get(fogIndex));
            FogInfo fogInfo;
            for (fogInfo = fogDetails.get(fogIds.get(Constants.RANDOM.nextInt(fogIds.size()))); !fogInfo.isActive(); fogInfo = fogDetails.get(fogIds.get(Constants.RANDOM.nextInt(fogIds.size()))));
            System.out.println("Executing Query on " + fogInfo.getDeviceId());
            int fogNo = Integer.parseInt(fogInfo.getDeviceIP().substring(fogInfo.getDeviceIP().lastIndexOf(".") + 1));
            ManagedChannel managedChannel = ManagedChannelBuilder
                    .forAddress(String.format("172.17.0.%d", 101 + fogNo), fogInfo.getDevicePort())
                    .usePlaintext()
                    .build();
            CoordinatorServerGrpc.CoordinatorServerBlockingStub coordinatorServerBlockingStub = CoordinatorServerGrpc.newBlockingStub(managedChannel);
            try {
                ObjectOutputStream ostream;
                ByteArrayOutputStream bstream = new ByteArrayOutputStream();
                ostream = new ObjectOutputStream(bstream);
                ostream.writeObject(influxDBQuery);
                ByteBuffer buffer = ByteBuffer.allocate(bstream.size());
                buffer.put(bstream.toByteArray());
                buffer.flip();
                LOGGER.info("Sending query object with query id " + influxDBQuery.getQueryId() + " from client.");
                final long startTime = System.currentTimeMillis();
                TSDBQueryResponse tsdbQueryResponse = coordinatorServerBlockingStub.execTSDBQuery(TSDBQueryRequest.newBuilder().addFluxQuery(ByteString.copyFrom(buffer)).setQueryId(Utils.getMessageFromUUID(influxDBQuery.getQueryId())).build());
                final long endTime = System.currentTimeMillis();
                LOGGER.info(String.format("[Query %s] Lines: %d", influxDBQuery.getQueryId(), tsdbQueryResponse.getFluxQueryResponse().toStringUtf8().chars().filter(c -> c == '\n').count()));
                managedChannel.shutdown();
                LOGGER.info(LOGGER.getName() + "[Outer " + influxDBQuery.getQueryId() + "] CoordinatorServer.execTSDBQuery: " + (endTime - startTime));
//                System.out.println(queryId + " " + (endTime - startTime));
                final long sleepTime = interval * 1000L - (endTime - startTime);
                Thread.sleep(sleepTime >= 0? sleepTime : 0);
            }
            catch (Exception ex) {
                System.out.println(queryId + " Failed");
            }

            if (queryCloud) {
                String host = args[9];
                String token = args[10];
                OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                        .connectTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                        .writeTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                        .readTimeout(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)
                        .retryOnConnectionFailure(true);
                InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                        .authenticateToken(token.toCharArray())
                        .org("org")
                        .connectionString(String.format("http://%s:8086?readTimeout=60m&connectTimeout=60m&writeTimeout=60m", host)) // ?readTimeout=1m&connectTimeout=1m&writeTimeout=1m
                        .okHttpClient(okHttpClient)
                        .logLevel(LogLevel.BASIC)
                        .bucket("bucket")
                        .build();
                final InfluxDBClient influxDBClient = InfluxDBClientFactory.create(influxDBClientOptions);
                QueryApi queryApi = influxDBClient.getQueryApi();
                try {
                    long startTime = System.currentTimeMillis();
                    String result = queryApi.queryRaw(getFluxQuery(influxDBQuery));
                    long endTime = System.currentTimeMillis();
                    LOGGER.info(String.format("[Query %s] Lines: %d", influxDBQuery.getQueryId(), result.chars().filter(c -> c == '\n').count()));
                    influxDBClient.close();
                    LOGGER.info(LOGGER.getName() + "[Outer " + influxDBQuery.getQueryId() + "] CoordinatorServer.execCloudQuery: " + (endTime - startTime));
//                    System.out.println(queryId + " " + (endTime - startTime));
                }
                catch (Exception ex) {
                    System.out.println(queryId + " Failed");
                }
            }
        }
    }

    public static String getFluxQuery(InfluxDBQuery influxDBQuery) {
        StringBuilder query = new StringBuilder();
        query.append("import \"experimental/geo\" ");
        query.append("from(bucket:\"").append(influxDBQuery.getBucket()).append("\")");

        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;

        LocalDateTime start = LocalDateTime.parse(
                influxDBQuery.getOperations().get("range").get("start"),
                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);
        LocalDateTime stop = LocalDateTime.parse(
                influxDBQuery.getOperations().get("range").get("stop"),
                DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN)).minusMinutes(330);

        LocalDateTime temp_date = stop.minusSeconds(1);

        query.append("|> range(start:").append(start.format(formatter)).append("Z,stop:")
                .append(temp_date.format(formatter)).append("Z)");


        HashMap<String, String> region = influxDBQuery.getOperations().get("region");
        query.append("|> geo.filterRows(region: ")
                .append("{ minLat: ").append(region.get("minLat"))
                .append(", maxLat: ").append(region.get("maxLat"))
                .append(", minLon: ").append(region.get("minLon"))
                .append(", maxLon: ").append(region.get("maxLon"))
                .append(" }, strict: true)");

        query.append("|>keep(columns:[");
        Map<String, String> keepMap = influxDBQuery.getOperations().get("keep");
        for (String k : keepMap.keySet()) {
            query.append("\"").append(keepMap.get(k)).append("\"").append(",");
        }
        query = new StringBuilder(query.substring(0, query.length() - 1));
        query.append("])");

        return query.toString();
    }
}
