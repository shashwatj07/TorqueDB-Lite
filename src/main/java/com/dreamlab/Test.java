package com.dreamlab;

import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.edgefs.grpcServices.TimeRange;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.Utils;
import com.google.common.geometry.S2CellId;
import com.google.protobuf.Timestamp;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import javax.swing.plaf.synth.SynthLookAndFeel;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class Test {

    static Map<UUID, FogPartition> fogPartitions = null;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        fogPartitions = generateFogPartitions(new ArrayList<>(Utils.readFogDetails("C:\\Users\\Shashwat\\TorqueDB-Lite\\src\\main\\resources\\fogs20.json").values()));
        double minLon=77.4586565, maxLat=12.862230000000002, minLat=12.85223, maxLon=77.4686565;
        BoundingBox boundingBox = BoundingBox
                .newBuilder()
                .setBottomRightLatLon(
                        Point.newBuilder()
                                .setLatitude(minLat)
                                .setLongitude(maxLon)
                                .build())
                .setTopLeftLatLon(
                        Point.newBuilder()
                                .setLatitude(maxLat)
                                .setLongitude(minLon)
                                .build())
                .build();
        List<S2CellId> s2CellIds = Utils.getCellIds(boundingBox, Constants.S2_CELL_LEVEL);
        System.out.println(s2CellIds);
        System.out.println(getSpatialShortlist(boundingBox));

        minLat = 12.941887000000001; maxLat = 12.941889; minLon = 77.534278; maxLon = 77.53428;
        BoundingBox boundingBox1 = BoundingBox
                .newBuilder()
                .setBottomRightLatLon(
                        Point.newBuilder()
                                .setLatitude(minLat)
                                .setLongitude(maxLon)
                                .build())
                .setTopLeftLatLon(
                        Point.newBuilder()
                                .setLatitude(maxLat)
                                .setLongitude(minLon)
                                .build())
                .build();
        Iterable<S2CellId> cellIds = Utils.getCellIds(boundingBox1, Constants.S2_CELL_LEVEL);
        System.out.println(cellIds);

    }

    private static List<UUID> getSpatialShortlist(BoundingBox boundingBox) {
        return getSpatialShortlist(Utils.createPolygon(boundingBox));
    }


    private static Map<UUID, FogPartition> generateFogPartitions(List<FogInfo> fogDevices) {
        List<Polygon> polygons = generateVoronoiPolygons(fogDevices);
        return generateFogPolygonMap(fogDevices, polygons);
    }

    private static Map<UUID, FogPartition> generateFogPolygonMap(List<FogInfo> fogDevices, List<Polygon> polygons) {
        Map<UUID, FogPartition> fogPartitionMap = new HashMap<>();
        for (FogInfo fog : fogDevices) {
            for (Polygon polygon : polygons) {
                Coordinate coordinate = new Coordinate(fog.getLongitude(), fog.getLatitude());
                if (polygon.contains(GeometryFactory.createPointFromInternalCoord(coordinate, polygon))) {
                    fogPartitionMap.put(fog.getDeviceId(), Utils.getFogPartition(fog, polygon));
                }
            }
        }
        return fogPartitionMap;
    }

    private static List<Polygon> generateVoronoiPolygons(List<FogInfo> fogDevices) {
        final Polygon region = Utils.createPolygon(Constants.MIN_LAT, Constants.MAX_LAT, Constants.MIN_LON, Constants.MAX_LON);
        List<Coordinate> coordinates = fogDevices.stream().map(Utils::getCoordinateFromFogInfo).collect(Collectors.toList());
        VoronoiDiagramBuilder diagramBuilder = new VoronoiDiagramBuilder();
        diagramBuilder.setSites(coordinates);
        diagramBuilder.setClipEnvelope(region.getEnvelopeInternal());
        Geometry polygonCollection = diagramBuilder.getDiagram(region.getFactory());

        List<Polygon> voronoiPolygons = new ArrayList<>();

        if (polygonCollection instanceof GeometryCollection) {
            GeometryCollection geometryCollection = (GeometryCollection) polygonCollection;
            for (int polygonIndex = 0; polygonIndex < geometryCollection.getNumGeometries(); polygonIndex++) {
                Polygon polygon = (Polygon) geometryCollection.getGeometryN(polygonIndex);
                voronoiPolygons.add(polygon);
            }
        }
        return voronoiPolygons;
    }

    private static List<UUID> getSpatialShortlist(Polygon queryPolygon) {
        final long start = System.currentTimeMillis();
        List<UUID> spatialShortlist =  fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
//        LOGGER.info(String.format("%s[Local] CoordinatorServer.getSpatialShortlist: %d", LOGGER.getName(), (end - start)));
//        LOGGER.info(String.format("%s[Count] CoordinatorServer.spatialShortlist: %d", LOGGER.getName(), spatialShortlist.size()));
        return spatialShortlist;
    }
}
