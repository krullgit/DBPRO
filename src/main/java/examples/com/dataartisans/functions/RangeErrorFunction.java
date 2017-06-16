package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RangeErrorFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        double rangeMf01 = 0;
        double rangeMf02 = 0;
        double rangeMf03 = 0;
        double errorMf01 = 0;
        double errorMf02 = 0;
        double errorMf03 = 0;
        double errortreshold = 0.27;
        String winKey = input.iterator().next().getKey();;
        if(winKey.equals("mf01")){
            for (KeyedDataPoint<Double> in : input) {
                rangeMf01 = in.getMf01();
                if (rangeMf01 > errortreshold) {
                    errorMf01 = 1;
                } else {
                    errorMf01 = 0;
                }
                winKey = in.getKey();
            }
        }
        if(winKey.equals("mf02")){
            for (KeyedDataPoint<Double> in : input) {
                rangeMf02 = in.getMf02();
                if (rangeMf02 > errortreshold) {
                    errorMf02 = 1;
                } else {
                    errorMf02 = 0;
                }
                winKey = in.getKey();
            }
        }
        if(winKey.equals("mf03")){
            for (KeyedDataPoint<Double> in : input) {
                rangeMf03 = in.getMf03();
                if (rangeMf03 > errortreshold) {
                    errorMf03 = 1;
                } else {
                    errorMf03 = 0;
                }
                winKey = in.getKey();
            }
        }
        KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), errorMf01,errorMf02,errorMf03);
        out.collect(windowAvg);
    }
}

