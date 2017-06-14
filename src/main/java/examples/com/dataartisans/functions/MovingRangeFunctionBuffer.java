package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;

import static java.time.temporal.ChronoUnit.SECONDS;

public class MovingRangeFunctionBuffer implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
    public boolean bufferextend = false;
    public long bufferAlertElementTime = 0;
    public ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>();
    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        double max = 0;
        double min = 1000000000;
        String winKey = input.iterator().next().getKey();

        // get max and min of the elements in the window
        for (KeyedDataPoint<Double> in: input) {

            if (in.getValue() < min) {
                min = in.getValue();
            } else if (in.getValue() > max) {
                max = in.getValue();
            }
            bufferlist.add(in);
        }
        //TODO: Send Buffer as far as an ELement arrives
        if (bufferextend == false) {
            while (Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime().minus(20, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(0).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) > 0) {
                bufferlist.remove(0);
            }
        } else {
            if (Instant.ofEpochMilli(bufferAlertElementTime).atZone(ZoneId.of("UTC+1")).toLocalTime().plus(70, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(bufferlist.size()-1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) < 0){
                bufferextend = false;

                for (KeyedDataPoint<Double> ready : bufferlist) {
                    //System.out.println(Instant.ofEpochMilli(ready.getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime());
                    out.collect(ready);
                }
                bufferlist.clear();
            }
        }

        Double range = (max-min)/max;

        if (range > 0.27){
            bufferAlertElementTime = window.getEnd();
            bufferextend = true;
        }
    }
}
