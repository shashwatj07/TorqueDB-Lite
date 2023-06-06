package com.dreamlab.utils;

import com.dreamlab.constants.Constants;
import com.dreamlab.constants.DeviceType;
import com.dreamlab.edgefs.grpcServices.BlockReplica;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.edgefs.grpcServices.UUIDMessage;
import com.dreamlab.types.DeviceInfo;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

import java.awt.geom.Point2D;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public final class Utils {

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
        DeviceType deviceType = DeviceType.get(blockReplica.getDeviceType());
        return new DeviceInfo(getUuidFromMessage(blockReplica.getDeviceId()),
                blockReplica.getIp(), blockReplica.getPort(), deviceType);
    }

    public static BlockReplica getMessageFromReplica(DeviceInfo replicaLocation) {
        int deviceType = replicaLocation.getDeviceType().ordinal();
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

    public static List<S2CellId> getCellIds(BoundingBox boundingBox, int maxLevel) {
        return getCellIds(boundingBox.getBottomRightLatLon().getLatitude(), boundingBox.getTopLeftLatLon().getLongitude(),
                boundingBox.getTopLeftLatLon().getLatitude(), boundingBox.getBottomRightLatLon().getLongitude(), maxLevel);
    }

    public static List<S2CellId> getCellIds(double minLat, double minLon, double maxLat, double maxLon, int maxLevel) {
        S2LatLngRect s2LatLngRect = toS2Rectangle(minLat, minLon, maxLat, maxLon);
        S2RegionCoverer s2RegionCoverer = S2RegionCoverer.builder()
                .setMaxLevel(maxLevel)
                .setMaxCells(Integer.MAX_VALUE)
                .build();
        ArrayList<S2CellId> s2CellIds = s2RegionCoverer.getCovering(s2LatLngRect).cellIds();
//        s2RegionCoverer.getCovering(s2LatLngRect, s2CellIds);
        return s2CellIds;
    }

    public static boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public static List<Instant> getTimeChunks(TimeRange timeRange, int chunkSizeSeconds) {
        final Instant exactStartInstant = getInstantFromTimestampMessage(timeRange.getStartTimestamp());
        final Instant endInstant = getInstantFromTimestampMessage(timeRange.getEndTimestamp());
        return getTimeChunks(exactStartInstant, endInstant, chunkSizeSeconds);
    }

    public static List<Instant> getTimeChunks(String start, String end, int chunkSizeSeconds) {
        final Instant exactStartInstant = getInstantFromString(start);
        final Instant endInstant = getInstantFromString(end);
        return getTimeChunks(exactStartInstant, endInstant, chunkSizeSeconds);
    }

    public static List<Instant> getTimeChunks(Instant exactStartInstant, Instant endInstant, int chunkSizeSeconds) {
        final Instant startInstant = Instant.ofEpochSecond(exactStartInstant.getEpochSecond() - ((exactStartInstant.getEpochSecond() - Instant.MIN.getEpochSecond()) % chunkSizeSeconds));
        List<Instant> timeChunks = new ArrayList<>();
        for (Instant instant = startInstant; instant.isBefore(endInstant); instant = instant.plus(chunkSizeSeconds, ChronoUnit.SECONDS)) {
            timeChunks.add(instant);
        }
        return timeChunks;
    }

    public static Instant getInstantFromTimestampMessage(Timestamp timestamp) {
        return Instant
                .ofEpochSecond(timestamp.getSeconds())
                .atZone(ZoneId.of(Constants.ZONE_ID)).toInstant();
    }

    public static Timestamp getTimestampMessageFromInstant(Instant instant) {
        return Timestamp
                .newBuilder()
                .setSeconds(instant.getEpochSecond())
                .build();
    }

    public static Instant getInstantFromString(String timestamp) {
        return LocalDateTime.parse(timestamp,
            DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN, Locale.US)
        ).atZone(ZoneId.of(Constants.ZONE_ID)).toInstant();
    }

    public static String getStringFromInstant(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneId.of(Constants.ZONE_ID))
                .format(DateTimeFormatter.ofPattern(Constants.DATE_TIME_PATTERN, Locale.US));
    }

    public static Coordinate getCoordinateFromFogInfo(FogInfo fogInfo) {
        return new Coordinate(fogInfo.getLongitude(), fogInfo.getLatitude());
    }

    public static FogPartition getFogPartition(FogInfo fogInfo, Polygon polygon) {
        return new FogPartition(fogInfo, polygon);
    }

    public static Map<UUID, FogInfo> readFogDetails(String fogsConfigFilePath) {
        Map<UUID, FogInfo> fogDetails = new HashMap<>();
        JSONArray jsonArray = null;
        try {
            jsonArray = new JSONArray(Files.readString(Paths.get(fogsConfigFilePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(Object obj : jsonArray) {
            JSONObject jsonObject = (JSONObject) obj;
            UUID fogId = UUID.fromString((String) jsonObject.get("id"));
            FogInfo fogInfo = new FogInfo(
                    fogId,
                    (String) jsonObject.get("ip"),
                    (Integer) jsonObject.get("port"),
                    Double.parseDouble((String) jsonObject.get("latitude")),
                    Double.parseDouble((String) jsonObject.get("longitude")),
                    (String) jsonObject.get("token"),
                    jsonObject.getBoolean("active")
            );
            fogDetails.put(fogId, fogInfo);
        }
        return fogDetails;
    }

    public static List<Integer> getMembershipFogIndices(UUID edgeId, int numFogs) {
        List<Integer> membershipFogIndices = new ArrayList<>();
        int uuidInt = Math.abs(edgeId.hashCode());
        membershipFogIndices.add(uuidInt % numFogs);
        membershipFogIndices.add((uuidInt + 1) % numFogs);
        membershipFogIndices.add((uuidInt + 2) % numFogs);
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
        return list.get(Constants.RANDOM.nextInt(list.size()));
    }

    public static Polygon createPolygon(BoundingBox boundingBox) {
        return createPolygon(Math.max(boundingBox.getBottomRightLatLon().getLatitude(), Constants.MIN_LAT),
                Math.min(boundingBox.getTopLeftLatLon().getLatitude(), Constants.MAX_LAT),
                Math.max(boundingBox.getTopLeftLatLon().getLongitude(), Constants.MIN_LON),
                Math.min(boundingBox.getBottomRightLatLon().getLongitude(), Constants.MAX_LON));
    }

    public static Polygon createPolygon(double minLat, double maxLat, double minLon, double maxLon){
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
        shapeFactory.setNumPoints(4);
        shapeFactory.setCentre(new Coordinate((minLon + maxLon) / 2, (minLat + maxLat) / 2));
        shapeFactory.setWidth(maxLon - minLon);
        shapeFactory.setHeight(maxLat - minLat);
        return shapeFactory.createRectangle();
    }

    public static ByteString getBytes(String first) throws IOException {
        byte[] bytes = Files.readAllBytes(Path.of(first));
        return ByteString.copyFrom(bytes);
    }

    public static void writeObjectToFile(Object object, String filePath) throws IOException {
        File file = new File(filePath);
        file.getParentFile().mkdirs();
        file.createNewFile();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
        objectOutputStream.writeObject(object);
        objectOutputStream.close();
    }

    public static Object readObjectFromFile(String filePath) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(filePath));
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        return object;
    }

    public static ByteBuffer serializeObject(Object obj)
    {
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
            oos.writeObject(obj);
            oos.flush();
            byte[] bytes = bytesOut.toByteArray();
            bytesOut.close();
            oos.close();
            return ByteBuffer.wrap(bytes);
        } catch (IOException e) {
            return ByteBuffer.wrap(new byte[0]);
        }
    }
}
