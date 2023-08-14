package com.dreamlab;

import com.dreamlab.api.Condition;
import com.dreamlab.constants.Constants;
import com.dreamlab.edgefs.grpcServices.BoundingBox;
import com.dreamlab.edgefs.grpcServices.Point;
import com.dreamlab.types.FogInfo;
import com.dreamlab.types.FogPartition;
import com.dreamlab.utils.Utils;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class TestSpatialShortlist {
    static Map<UUID, FogPartition> fogPartitions;
    public static void main(String[] args) {
        Map<UUID, FogInfo> fogDetails = Utils.readFogDetails("src/main/resources/fogs20.json");
        fogPartitions = generateFogPartitions(new ArrayList<>(fogDetails.values()));
        double minLon=77.56002799999999, maxLat=12.984888999999999, minLat=12.982889, maxLon=77.562028;

        BoundingBox boundingBox = BoundingBox.newBuilder()
                .setTopLeftLatLon(Point.newBuilder().setLatitude(12.984888999999999).setLongitude(77.56002799999999).build())
                .setBottomRightLatLon(Point.newBuilder().setLatitude(12.982889).setLongitude(77.562028).build())
                .build();
        System.out.println(getSpatialShortlist(boundingBox, UUID.fromString("b593772a-2ebe-4afc-a6c5-8c674924c4bf")));
        System.out.println(getSpatialShortlist(minLat, maxLat, minLon, maxLon, UUID.fromString("b593772a-2ebe-4afc-a6c5-8c674924c4bf")));
    }

    private static List<UUID> getSpatialShortlist(BoundingBox boundingBox, UUID queryId) {
        return getSpatialShortlist(Utils.createPolygon(boundingBox), queryId);
    }

    private static List<UUID> getSpatialShortlist(double minLat, double maxLat, double minLon, double maxLon, UUID queryId) {
        return getSpatialShortlist(Utils.createPolygon(minLat, maxLat, minLon, maxLon), queryId);
    }

    private static List<UUID> getSpatialShortlist(String minLat, String maxLat, String minLon, String maxLon, UUID queryId) {
        return getSpatialShortlist(Double.parseDouble(minLat),
                Double.parseDouble(maxLat), Double.parseDouble(minLon), Double.parseDouble(maxLon), queryId);
    }

    private static List<UUID> getSpatialShortlist(Polygon queryPolygon, UUID queryId) {
        final long start = System.currentTimeMillis();
        List<UUID> spatialShortlist =  fogPartitions.keySet().stream().filter(fogId -> fogPartitions.get(fogId).getPolygon().intersects(queryPolygon)).collect(Collectors.toList());
        final long end = System.currentTimeMillis();
        return spatialShortlist;
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
}
