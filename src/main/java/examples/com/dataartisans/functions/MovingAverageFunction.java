package examples.com.dataartisans.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import examples.com.dataartisans.data.KeyedDataPoint;

public class MovingAverageFunction implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
	@Override
	public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
		int count = 0;
		double winsum = 0;
		String winKey = input.iterator().next().getKey();

		Double avgMf01 = 0.0;
		Double avgMf02 = 0.0;
		Double avgMf03 = 0.0;

		// get max and min of the elements in the window
		if(winKey.equals("mf01")){
			for (KeyedDataPoint<Double> in: input) {
				winsum = winsum + in.getMf01();
				count++;
			}
			avgMf01 = winsum/(1.0 * count);
		}
		if(winKey.equals("mf02")){
			for (KeyedDataPoint<Double> in: input) {
				winsum = winsum + in.getMf02();
				count++;
			}
			avgMf02 = winsum/(1.0 * count);
		}
		if(winKey.equals("mf03")){
			for (KeyedDataPoint<Double> in: input) {
				winsum = winsum + in.getMf03();
				count++;
			}
			avgMf03 = winsum/(1.0 * count);
		}

		KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), avgMf01,avgMf02,avgMf03);

		out.collect(windowAvg);
	}
}