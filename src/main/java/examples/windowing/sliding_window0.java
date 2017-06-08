package examples.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Very first draft version of an sliding window
 * to run this use as parameters:
 *    -input ./src/main/resources/elecsales_keyed0.csv
 *    
 * TODO: how to work with data like: R/data/elecsales.csv  in streamAPI it seems there is no reader for csv
 */
public class sliding_window0 {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setGlobalJobParameters(params);
		//env.setParallelism(1);

		@SuppressWarnings({"rawtypes", "serial"})
		DataStream<Tuple3<Integer,Integer,Double>> salesData;
		salesData = env.readTextFile(params.get("input")).map(new ParseSalesData());
		
		
		DataStream<Tuple3<Integer, Integer, Double>> topSpeeds = salesData
				// TODO: it seems we need to add time stamps to the data, how do we do when the timestamps do not come from the file or stream ???
				.assignTimestampsAndWatermarks(new SalesTimestamp())
				// TODO: this is another issue, if our stream is not keyed then how do we do?
				//       the problem is that the window functions most of the time are applied to keyed windows
				.keyBy(0)
				// TODO: this is another issue, how the scale in time stamps are handled
				//       how timestamps are handled in general
				//       SlidingEventTimeWindows.of(Time size, Time slide)
				.window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(1)))
				// TODO: how to define and apply other window functions... here for example we need to get average and divide by the length of the window
				//       also we need to be carefull with the borders...
				.sum(2);


		if (params.has("output")) {
			topSpeeds.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			topSpeeds.print();
		}

		env.execute("CarTopSpeedWindowingExample");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************



	private static class ParseSalesData extends RichMapFunction<String, Tuple3<Integer,Integer, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<Integer,Integer, Double> map(String record) {
			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split(",");
			return new Tuple3<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]));
		}
	}

	private static class SalesTimestamp extends AscendingTimestampExtractor<Tuple3<Integer,Integer, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Tuple3<Integer,Integer, Double> element) {
			return element.f1;
		}
	}

}
