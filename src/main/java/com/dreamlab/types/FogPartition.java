package com.dreamlab.types;

import org.locationtech.jts.geom.Polygon;

import java.io.Serializable;

public class FogPartition extends FogInfo implements Serializable {

    private static final long serialVersionUID = 340163714945927422L;
    private Polygon polygon;

    public FogPartition(FogInfo fogInfo, Polygon polygon) {
        super(fogInfo.getDeviceId(), fogInfo.getDeviceIP(), fogInfo.getDevicePort(), fogInfo.getLatitude(), fogInfo.getLongitude(), fogInfo.getToken());
        this.polygon = polygon;
    }

    public Polygon getPolygon() {
        return polygon;
    }

    public void setPolygon(Polygon polygon) {
        this.polygon = polygon;
    }

}
