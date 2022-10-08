import com.dreamlab.edgefs.grpcServices.BlockReplica;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

public final class Utils {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int S2_CELL_LEVEL = 22;
    private static final String DATE_TIME_PATTERN = "uuuu-MM-dd HH-mm-ss";
    private static final String ZONE_ID = "Asia/Kolkata";

    private Utils() {
    }

    public static UUID getUuidFromMessage(UUIDMessage uuidMessage) {
        return new UUID(uuidMessage.getMsb(), uuidMessage.getLsb());
    }

    public static UUIDMessage getMessageFromUUID(UUID uuid) {
        return UUIDMessage
                .newBuilder()
                .setLsb(uuid.getLeastSignificantBits())
                .setMsb(uuid.getMostSignificantBits())
                .build();
    }

    public static DeviceInfo getReplicaFromMessage(BlockReplica blockReplica) {
        DeviceType deviceType;
        if (blockReplica.getDeviceType() == 0) {
            deviceType = DeviceType.FOG;
        } else {
            deviceType = DeviceType.EDGE;
        }
        return new DeviceInfo(getUuidFromMessage(blockReplica.getDeviceId()),
                ByteBuffer.wrap(blockReplica.getIp().toByteArray()), blockReplica.getPort(), deviceType);
    }

    public static BlockReplica getMessageFromReplica(DeviceInfo replicaLocation) {
        int deviceType;
        if (replicaLocation.getDeviceType() == DeviceType.FOG) {
            deviceType = 0;
        } else {
            deviceType = 1;
        }
        return BlockReplica
                .newBuilder()
                .setDeviceId(getMessageFromUUID(replicaLocation.getDeviceId()))
                .setIp(ByteString.copyFrom(replicaLocation.getDeviceIP().array()))
                .setPort(replicaLocation.getDevicePort())
                .setDeviceType(deviceType)
                .build();
    }

    public static S2LatLngRect toS2Rectangle(double minLat, double minLon, double maxLat, double maxLon) {
        return new S2LatLngRect(S2LatLng.fromDegrees(minLat, minLon), S2LatLng.fromDegrees(maxLat, maxLon));
    }

    public static Iterable<S2CellId> getCellIds(double minLat, double minLon, double maxLat, double maxLon) {
        S2LatLngRect s2LatLngRect = toS2Rectangle(minLat, minLon, maxLat, maxLon);
        S2RegionCoverer s2RegionCoverer = S2RegionCoverer.builder()
                .setMaxLevel(S2_CELL_LEVEL)
                .setMinLevel(S2_CELL_LEVEL)
                .setMaxCells(Integer.MAX_VALUE)
                .build();
        return s2RegionCoverer.getCovering(s2LatLngRect).cellIds();
    }

    public static List<Instant> getTimeChunks(TimeRange timeRange, int chunkSizeInMinutes) {
        Instant startInstant = getInstantFromTimestampMessage(timeRange.getStartTimestamp());
        Instant endInstant = getInstantFromTimestampMessage(timeRange.getEndTimestamp());
        List<Instant> timeChunks = new ArrayList<>();
        for (Instant instant = startInstant; instant.isBefore(endInstant); instant = instant.plus(chunkSizeInMinutes, ChronoUnit.MINUTES)) {
            timeChunks.add(instant);
        }
        return timeChunks;
    }

    public static Instant getInstantFromTimestampMessage(Timestamp timestamp) {
        return Instant
                .ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos())
                .atZone(ZoneId.of(ZONE_ID)).toInstant();
    }

    public static Timestamp getTimestampMessageFromInstant(Instant instant) {
        return Timestamp
                .newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static Instant getInstantFromString(String timestamp) {
        return LocalDateTime.parse(timestamp,
            DateTimeFormatter.ofPattern( DATE_TIME_PATTERN, Locale.US)
        ).atZone(ZoneId.of(ZONE_ID)).toInstant();
    }

    public static Coordinate getCoordinateFromFogInfo(FogInfo fogInfo) {
        return new Coordinate(fogInfo.getLongitude(), fogInfo.getLatitude());
    }

    public static FogPartition getFogPartition(FogInfo fogInfo, Polygon polygon) {
        return new FogPartition(fogInfo, polygon);
    }

    public static Map<UUID, FogInfo> readFogDetails(String fogsConfigFilePath) throws IOException {
        Map<UUID, FogInfo> fogDetails = new HashMap<>();
        JSONArray jsonArray = new JSONArray(Files.readString(Paths.get(fogsConfigFilePath)));
        for(Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            UUID fogId = UUID.fromString((String) jsonObject.get("id"));
            FogInfo fogInfo = new FogInfo(
                    fogId,
                    ByteBuffer.wrap(((String) jsonObject.get("ip")).getBytes(StandardCharsets.UTF_8)),
                    (Integer) jsonObject.get("port"),
                    Double.parseDouble((String) jsonObject.get("latitude")),
                    Double.parseDouble((String) jsonObject.get("longitude")),
                    (String) jsonObject.get("token")
            );
            fogDetails.put(fogId, fogInfo);
        }
        return fogDetails;
    }

    public static List<Integer> getMembershipFogIndices(UUID edgeId) {
        // TODO
        List<Integer> membershipFogIndices = new ArrayList<>();
        membershipFogIndices.add(0);
        membershipFogIndices.add(1);
        membershipFogIndices.add(2);
        return membershipFogIndices;
    }

    public static FogInfo getParentFog(Map<UUID, FogInfo> fogDetails, double latitude, double longitude) {
        double minDistance = Double.POSITIVE_INFINITY;
        FogInfo nearestFog = null;
        for (Map.Entry<UUID, FogInfo> entry : fogDetails.entrySet()) {
            double fogDistance = Point2D.distance(longitude, latitude, entry.getValue().getLongitude(), entry.getValue().getLatitude());
            if (fogDistance < minDistance) {
                nearestFog = entry.getValue();
                minDistance = fogDistance;
            }
        }
        return nearestFog;
    }

    public static <T> T getRandomElement(List<T> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }

    public static Polygon createPolygon(double minLat, double maxLat, double minLon, double maxLon){
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
        shapeFactory.setNumPoints(4);
        shapeFactory.setCentre(new Coordinate((minLon + maxLon) / 2, (minLat + maxLat) / 2));
        shapeFactory.setWidth(maxLon - minLon);
        shapeFactory.setHeight(maxLat - minLat);
        return shapeFactory.createRectangle();
    }
}
