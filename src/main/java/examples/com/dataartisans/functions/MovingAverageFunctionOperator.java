package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class MovingAverageFunctionOperator implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {


	@Override
	public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {



		String winKey = input.iterator().next().getKey();




		Double pwrMf01 = 0.0;
		Double pwrMf02 = 0.0;
		Double pwrMf03 = 0.0;

		// get max and min of the elements in the window
		if(winKey.equals("mf01")){
			double avgMf01 = input.iterator().next().getMf01();
			pwrMf01 = 208/(Math.pow(avgMf01,(1.0/3)));
		}
		if(winKey.equals("mf02")){
			double avgMf02 = input.iterator().next().getMf02();
			pwrMf02 = 208/(Math.pow(avgMf02,(1.0/3)));
		}
		if(winKey.equals("mf03")){
			double avgMf03 = input.iterator().next().getMf03();
			pwrMf03 = 208/(Math.pow(avgMf03,(1.0/3)));
		}

		KeyedDataPoint<Double> windowAvg = new KeyedDataPoint<>(winKey,window.getEnd(), pwrMf01,pwrMf02,pwrMf03);
		out.collect(windowAvg);
	}
}