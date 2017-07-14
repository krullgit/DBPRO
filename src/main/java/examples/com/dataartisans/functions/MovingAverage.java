package examples.com.dataartisans.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import examples.com.dataartisans.data.KeyedDataPoint;

import java.sql.Timestamp;

public class MovingAverage implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {


	@Override
	public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
		int count = 0;
		double winsumMf01 = 0;
		double winsumMf02 = 0;
		double winsumMf03 = 0;
		String winKey = input.iterator().next().getKey();



		Double avgMf01 = 0.0;
		Double avgMf02 = 0.0;
		Double avgMf03 = 0.0;

		// get max and min of the elements in the window
		for (KeyedDataPoint<Double> in: input) {
			winsumMf01 = winsumMf01 + in.getMf01();
			winsumMf02 = winsumMf02 + in.getMf02();
			winsumMf03 = winsumMf03 + in.getMf03();
			count++;
		}
		avgMf01 = winsumMf01/(1.0 * count);
		avgMf02 = winsumMf02/(1.0 * count);
		avgMf03 = winsumMf03/(1.0 * count);


		KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), avgMf01,avgMf02,avgMf03);

		out.collect(windowAvg);
	}
}