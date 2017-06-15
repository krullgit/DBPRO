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

		// get the sum of the elements in the window
		for (KeyedDataPoint<Double> in: input) {
			winsum = winsum + in.getValue();
			count++;
		}

		Double avg = winsum/(1.0 * count);

		KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), avg);

		out.collect(windowAvg);
	}
}