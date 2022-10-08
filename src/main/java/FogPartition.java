import org.locationtech.jts.geom.Polygon;

public class FogPartition extends FogInfo {

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
