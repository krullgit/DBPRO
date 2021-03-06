package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MovingRange implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {

        // variables for the range calculation
        String winKey = input.iterator().next().getKey();
        Double rangeMf01 = 0.0;
        Double rangeMf02 = 0.0;
        Double rangeMf03 = 0.0;
        double maxMf01 = 0;
        double minMf01 = 1000000000;
        double maxMf02 = 0;
        double minMf02 = 1000000000;
        double maxMf03 = 0;
        double minMf03 = 1000000000;

        // get max and min of the elements in the window
        for (KeyedDataPoint<Double> in: input) {
            if (in.getMf01() < minMf01) {
                minMf01 = in.getMf01();}
            if (in.getMf01() > maxMf01) {
                maxMf01 = in.getMf01();}
            if (in.getMf02() < minMf02) {
                minMf02 = in.getMf02();}
            if (in.getMf02() > maxMf02) {
                maxMf02 = in.getMf02();}
            if (in.getMf03() < minMf03) {
                minMf03 = in.getMf03();}
            if (in.getMf03() > maxMf03) {
                maxMf03 = in.getMf03();}
        }

        // calculate ranges
        rangeMf01 = (maxMf01-minMf01)/maxMf01;
        rangeMf02 = (maxMf02-minMf02)/maxMf02;
        rangeMf03 = (maxMf03-minMf03)/maxMf03;

        // return ranges
        KeyedDataPoint<Double> windowRange = new KeyedDataPoint<>(winKey, window.getEnd(), rangeMf01,rangeMf02,rangeMf03);
        out.collect(windowRange);
    }
}
