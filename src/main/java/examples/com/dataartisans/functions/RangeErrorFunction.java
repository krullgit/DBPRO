package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RangeErrorFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        double range = 0;
        double test = -1;
        String winKey = input.iterator().next().getKey();;

        for (KeyedDataPoint<Double> in: input) {
            range = in.getValue();
            if (range > 0.27) {
                test = 1;
            } else {
                test = 0;
            }
            winKey = in.getKey();
        }
        KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), test);
        out.collect(windowAvg);
    }
}

