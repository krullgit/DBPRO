package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MovingAverageFunctionOperator implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
	@Override
	public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {

		double avg = input.iterator().next().getValue();
		String winKey = input.iterator().next().getKey();


		// get the sum of the elements in the window

		double pwr = 208/(Math.pow(avg,(1.0/3)));

		KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), pwr);

		out.collect(windowAvg);
	}
}