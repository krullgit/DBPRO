package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MovingRangeError implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    public double errortreshold = 0; // treshold for the anomaly
    public double errorCountPrev = 0;
    public double errorCountThis = 0;

    // constructor
    public MovingRangeError(double errortreshold){
        this.errortreshold = errortreshold;
    }

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {

        // get the ranges
        double rangeMf01 = input.iterator().next().getMf01();
        double rangeMf02 = input.iterator().next().getMf02();
        double rangeMf03 = input.iterator().next().getMf03();
        // default errors
        double errorMf01 = 0;
        double errorMf02 = 0;
        double errorMf03 = 0;
        String winKey = input.iterator().next().getKey();;

        // if range too high -> error
        if (rangeMf01 > errortreshold) {
            errorCountThis ++;
            errorMf01 = 1;
        } else {
            errorMf01 = 0;
        }
        if (rangeMf02 > errortreshold) {
            errorMf02 = 1;
            errorCountThis ++;
        } else {
            errorMf02 = 0;
        }
        if (rangeMf03 > errortreshold) {
            errorMf03 = 1;
            errorCountThis ++;
        } else {
            errorMf03 = 0;
        }

        if(errorCountPrev != errorCountThis){
            System.out.println(errorCountThis);
        }
        errorCountPrev = errorCountThis;

        // return errors
        KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), errorMf01,errorMf02,errorMf03);
        out.collect(windowAvg);
    }
}

