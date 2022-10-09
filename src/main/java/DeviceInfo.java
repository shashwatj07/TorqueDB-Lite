import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

class DeviceInfo {
    private final UUID deviceId;
    private final String deviceIP;
    private final int devicePort;
    private final DeviceType deviceType;

    public DeviceInfo(UUID deviceId, String deviceIP, int devicePort, DeviceType deviceType) {
        this.deviceId = deviceId;
        this.deviceIP = deviceIP;
        this.devicePort = devicePort;
        this.deviceType = deviceType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeviceInfo obj = (DeviceInfo) o;
        return deviceId.equals(obj.deviceId) && Objects.equals(deviceIP, obj.deviceIP) && devicePort == obj.devicePort && deviceType == obj.deviceType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(deviceId, deviceIP, devicePort, deviceType);
    }

    public UUID getDeviceId() {
        return deviceId;
    }

    public String getDeviceIP() {
        return deviceIP;
    }

    public int getDevicePort() {
        return devicePort;
    }

    public DeviceType getDeviceType() {
        return deviceType;
    }
}

