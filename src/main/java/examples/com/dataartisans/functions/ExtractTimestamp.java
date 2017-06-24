package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

// TODO: maybe change the AscendingTimestampExtractor because of following messages:

// WARN  org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor  - Timestamp monotony violated: 1329929424054 < 1329929424994


public class ExtractTimestamp implements AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;

    private final long maxOutOfOrderness = 3500; // 3.5 seconds
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(KeyedDataPoint<Double> element, long previousElementTimestamp) {
        System.out.println("test");
        long timestamp = element.getTimeStampMs();

        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
    // TODO: Why is the return of the getCurrentWatermark not a plus : currentMaxTimestamp + maxOutOfOrderness
    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

/*
public class ExtractTimestamp implements AssignerWithPeriodicWatermarks<KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;

    public static long delay = 0;
    public static long delaysum = 0;
    public static long count = 0;
    public static long timeone = 0;
    public static long timetwo = 0;
    public static long count1000 = 0;


    private final long maxOutOfOrderness = 3500; // 3.5 seconds
    private static long currentMaxTimestamp;

    @Override
    public long extractTimestamp(KeyedDataPoint<Double> element, long previousElementTimestamp) {
        long timestamp = element.getTimeStampMs();
        if(count == 0) {
            System.out.println("COUNT AUF 0");
            currentMaxTimestamp = timestamp;
        }
        if(timestamp - currentMaxTimestamp <= 0 ){
            delay = Math.abs(timestamp - currentMaxTimestamp);

            count1000 ++;

            delaysum += delay;

            timeone=timestamp;
            timetwo=currentMaxTimestamp;
        }
        count ++;

        currentMaxTimestamp = timestamp;
        return timestamp;
    }
    // TODO: Why is the return of the getCurrentWatermark not a plus : currentMaxTimestamp + maxOutOfOrderness
    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}*/