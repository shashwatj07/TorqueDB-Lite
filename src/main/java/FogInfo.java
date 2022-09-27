import com.google.protobuf.ByteString;

import java.util.UUID;

public class FogInfo extends DeviceInfo {

    private final double latitude;
    private final double longitude;

    public FogInfo(UUID deviceId, ByteString deviceIP, int devicePort, double latitude, double longitude) {
        super(deviceId, deviceIP, devicePort, DeviceType.FOG);
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }
}
