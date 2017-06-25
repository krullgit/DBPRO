package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

// In the timestamp extractor we want to handle following issue:
// WARN  org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor  - Timestamp monotony violated: 1329929424054 < 1329929424994
// It means that the tuples of the stream come out of order.

// Thats why we choose the AssignerWithPeriodicWatermarks because it waits a specific time before it decides not to wait any more for late cumming tuples
public class ExtractTimestamp implements AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;

    private final long maxOutOfOrderness = 0; // time to wait for late cumming tuples
    private long currentMaxTimestamp;

    // returns timestamp
    @Override
    public long extractTimestamp(KeyedDataPoint<Double> element, long previousElementTimestamp) {
        long timestamp = element.getTimeStampMs();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
    // returns watermark to set a border for late cumming tuples
    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}
