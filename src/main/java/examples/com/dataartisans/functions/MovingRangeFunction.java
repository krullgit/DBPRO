package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by franziska on 08.06.17.
 */
public class MovingRangeFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        double max = 0;
        double min = 1000000000;
        String winKey = "";

        // get max and min of the elements in the window
        for (KeyedDataPoint<Double> in: input) {
            if (in.getValue() < min) {
                min = in.getValue();
            } else if (in.getValue() > max) {
                max = in.getValue();
            }
            winKey = in.getKey(); // TODO: this just need to be done once ...??? also counting would not be necessary, how to get the size of this window?
        }

        Double range = (max-min)/max;
        System.out.println("MovingRangeFunction: max=" +  max + "  min=" + min + " range = " + range + "  time=" + window.getStart());

        KeyedDataPoint<Double> windowRange = new KeyedDataPoint<>(winKey, window.getEnd(), range);

        out.collect(windowRange);

    }
}
