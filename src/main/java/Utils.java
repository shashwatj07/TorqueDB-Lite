import com.dreamlab.edgefs.grpcServices.BlockReplica;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;

import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;

public final class Utils {

    private static final SecureRandom RANDOM = new SecureRandom();

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
                blockReplica.getIp(), blockReplica.getPort(), deviceType);
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
                .setIp(replicaLocation.getDeviceIP())
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
                .setMaxLevel(22)
                .setMinLevel(22)
                .setMaxCells(Integer.MAX_VALUE)
                .build();
        return s2RegionCoverer.getCovering(s2LatLngRect).cellIds();
    }

    public static List<Instant> getTimeChunks(TimeRange timeRange, int chunkSizeInMinutes) {
        Instant startInstant = Instant
                .ofEpochSecond(timeRange.getStartTimestamp().getSeconds(), timeRange.getStartTimestamp().getNanos())
                .atZone(ZoneId.of("Asia/Kolkata")).toInstant();
        Instant endInstant = Instant
                .ofEpochSecond(timeRange.getEndTimestamp().getSeconds(), timeRange.getEndTimestamp().getNanos())
                .atZone(ZoneId.of("Asia/Kolkata")).toInstant();
        List<Instant> timeChunks = new ArrayList<>();
        for (Instant instant = startInstant; instant.isBefore(endInstant); instant = instant.plus(chunkSizeInMinutes, ChronoUnit.MINUTES)) {
            timeChunks.add(instant);
        }
        return timeChunks;
    }

    public static Coordinate getCoordinateFromFogInfo(FogInfo fogInfo) {
        return new Coordinate(fogInfo.getLongitude(), fogInfo.getLatitude());
    }

    public static FogPartition getFogPartition(FogInfo fogInfo, Polygon polygon) {
        return new FogPartition(fogInfo, polygon);
    }

    public static <T> T getRandomElement(List<T> list) {
        return list.get(RANDOM.nextInt(list.size()));
    }
}
