package test.mva;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Count;
import org.apache.flink.api.common.functions.FlatMapFunction;

/**
 * Test with the MVA csv file
 */
public class MVA {

	// *************************************************************************
	// PROGRAM
	// creates a moving average
	// input parameters: -input filePath
	// windowsize(Integer)
	// handles odd and even mva - windows
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setGlobalJobParameters(params);
		env.setParallelism(1);

		@SuppressWarnings({ "rawtypes", "serial" })
		DataStream<Tuple3<Integer, Integer, Double>> salesData;
		salesData = env.readTextFile(params.get("input")).flatMap(
				new ParseSalesData());

		// TODO MArcela pls add windowsize to params, and show me how it works
		// :D
		// final int initialWindowSize = Integer.parseInt(args[2]);
		final int initialWindowSize = 4;
		int windowdummy;
		final int startTime = 1989;
		if (initialWindowSize % 2 == 0)
			windowdummy = initialWindowSize + 1;

		else
			windowdummy = initialWindowSize;

		final int windowsize = windowdummy;

		/**
		 * This class is a internal comparator for the following schema:
		 * Tuple3<key, timestamp, measurement> it is used to apply the moving
		 * average to the measurement in the centre of the window
		 * 
		 * @author Ariane
		 *
		 */
		class WindowComparator implements
				Comparator<Tuple3<Integer, Integer, Double>> {
			@Override
			public int compare(Tuple3<Integer, Integer, Double> one,
					Tuple3<Integer, Integer, Double> two) {
				if (one.f1 > two.f1)
					return 1;

				if (one.f1 < two.f1)
					return -1;

				return 0;
			}
		}

		DataStream<Tuple3<Integer, Integer, Double>> topSpeeds = salesData
				// TODO: it seems we need to add time stamps to the data, how do
				// we do when the timestamps do not come from the file or stream
				// TODO Marcela: If we deal with TimeSeries isn't it that we
				// need
				// to have an information of the event time? Else we need to
				// apply
				// moving average by processing time, isn't?
				.assignTimestampsAndWatermarks(new SalesTimestamp())
				// TODO: this is another issue, if our stream is not keyed then
				// how do we do?
				// TODO MArcela: We can just apply a map-Function and add a
				// random Integer as key
				// to the dataset, I would suggest this as a preproccessing task
				// depending
				// on the data
				// the problem is that the window functions most of the time are
				// applied to keyed windows
				.keyBy(0)
				// TODO: this is another issue, how the scale in time stamps are
				// handled
				// how timestamps are handled in general
				// SlidingEventTimeWindows.of(Time size, Time slide)
				.window(SlidingEventTimeWindows.of(
						Time.milliseconds(windowsize), Time.milliseconds(1)))
				.apply(new WindowFunction<Tuple3<Integer, Integer, Double>, Tuple3<Integer, Integer, Double>, Tuple, TimeWindow>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void apply(Tuple arg0, TimeWindow arg1,
							Iterable<Tuple3<Integer, Integer, Double>> arg2,
							Collector<Tuple3<Integer, Integer, Double>> out)
							throws Exception {

						double sumOdd = 0;
						double sumEven = 0;

						List<Tuple3<Integer, Integer, Double>> test = new ArrayList<Tuple3<Integer, Integer, Double>>();
						// put window points in a list to control order and
						// access on certain points
						for (Tuple3<Integer, Integer, Double> t : arg2) {
							test.add(t);
							// sum up all measurements
							if (initialWindowSize % 2 != 0)
								sumOdd += t.f2;
						}

						// sort the List by time to be sure that you have the
						// correct order
						test.sort(new WindowComparator());
						// handle borders
						// here: we leave the orginal values
						// else: remove out.collect and the values are NA for
						// further calcs
						if (test.size() != windowsize) {

							if (test.get(0).f1 == startTime) {
								if(test.get(test.size()-1).f1 < startTime+((windowsize-1)/2)){
								out.collect(new Tuple3<Integer, Integer, Double>(
										test.get(test.size() - 1).f0, test
												.get(test.size() - 1).f1, test
												.get(test.size() - 1).f2));
								}
								else{
									//do nothing
								}
							} else
								if(test.get(0).f1 <= test.get(test.size()-1).f1-((windowsize-1)/2)){
									// do nothing	
								}
								else{
								out.collect(new Tuple3<Integer, Integer, Double>(
										test.get(0).f0, test.get(0).f1, test
												.get(0).f2));
								}}// borders
						else {
							// size == middle of list, the point we calculate
							// the
							// mva for
							int size = (test.size() + 1) / 2;

							if (initialWindowSize % 2 != 0) {
								// process for odd windows

								double mvaOdd = sumOdd / (test.size());
								// if window has enough points

								out.collect(new Tuple3<Integer, Integer, Double>(
										test.get(size - 1).f0, test
												.get(size - 1).f1, mvaOdd));
							}// end odd
								// even
							else {
								double sum1 = 0;
								double sum2 = 0;

								for (int i = 0; i <= test.size() - 2; i++) {
									sum1 = sum1 + test.get(i).f2;
									sum2 = sum2 + test.get(i + 1).f2;
								}

								double mvaEven = 0.5
										* (sum1 / (test.size() - 1)) + 0.5
										* (sum2 / (test.size() - 1));
								// if window has enough points

								out.collect(new Tuple3<Integer, Integer, Double>(
										test.get(size - 1).f0, test
												.get(size - 1).f1, mvaEven));
							}// end odd

						}// correct windowsize

					}

				});

		// TODO: how to define and apply other window functions... here for
		// example we need to get average and divide by the length of the window
		// also we need to be carefull with the borders...
		// .sum(2)
		// .map(new MapFunction<Tuple3<Integer, Integer, Double>,Tuple3<Integer,
		// Integer, Double>>(){
		//
		// @Override
		// public Tuple3<Integer, Integer, Double> map(
		// Tuple3<Integer, Integer, Double> arg0)
		// throws Exception {
		// double mva = arg0.f2/(double)windowsize;
		// return new Tuple3<Integer, Integer, Double>(arg0.f0, arg0.f1, mva);
		// }
		//
		// });

		if (params.has("output")) {
			topSpeeds.writeAsText(params.get("output"));
		} else {
			System.out
					.println("Printing result to stdout. Use --output to specify output path.");
			topSpeeds.print();
		}

		env.execute("CarTopSpeedWindowingExample");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class ParseSalesData implements
			FlatMapFunction<String, Tuple3<Integer, Integer, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String arg0,
				Collector<Tuple3<Integer, Integer, Double>> arg1)
				throws Exception {
			// if data has no key, key here

			String rawData = arg0;
			String[] data = rawData.split(",");

			try {
				arg1.collect(new Tuple3<>(1, Integer.parseInt(data[0]), Double
						.valueOf(data[1])));

			} catch (Exception e) {

			} finally {

			}
		}

		// @Override
		// public void flatMap(String record) {

		// }
	}

	private static class SalesTimestamp extends
			AscendingTimestampExtractor<Tuple3<Integer, Integer, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(
				Tuple3<Integer, Integer, Double> element) {
			return element.f1;
		}
	}

}
