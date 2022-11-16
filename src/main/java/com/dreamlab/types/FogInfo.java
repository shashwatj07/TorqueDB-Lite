package com.dreamlab.types;

import com.dreamlab.constants.DeviceType;

import java.io.Serializable;
import java.util.UUID;

public class FogInfo extends DeviceInfo implements Serializable {

    private static final long serialVersionUID = 5728320985779197204L;
    private final double latitude;
    private final double longitude;
    private final String token;

    public FogInfo(UUID deviceId, String deviceIP, int devicePort, double latitude, double longitude, String token) {
        super(deviceId, deviceIP, devicePort, DeviceType.FOG);
        this.latitude = latitude;
        this.longitude = longitude;
        this.token = token;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getToken() {
        return token;
    }
}
