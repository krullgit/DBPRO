package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

// TODO: maybe change the AscendingTimestampExtractor because of following messages:
// WARN  org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor  - Timestamp monotony violated: 1329929424054 < 1329929424994

public class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;
    @Override
    public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
        return element.getTimeStampMs();
    }
}