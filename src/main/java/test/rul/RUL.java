package test.rul;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;
//import org.apache.flink.api.scala._;

public class RUL {

	/**
	 * dataset paths (local)
	 */

	static String train01FilePath = "C:\\Users\\Ariane\\Downloads\\RUL\\train_FD001.txt";
	String test01FilePath = "C:\\Users\\Ariane\\Downloads\\RUL\\test_FD001.txt";
	String rul01FilePath = "C:\\Users\\Ariane\\Downloads\\RUL\\RUL_FD001.txt";

	public static void main(String[] args) throws Exception {

		/**
		 * set up the execution environment read in the data files
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		
		DataStreamSource<String> train = env.readTextFile(train01FilePath);
		
		
		/**
		 * Preprocessing
		 */

		DataStream<Tuple3<Integer, Integer, List<String>>> train01 = train
				.map(new MapFunction<String, Tuple3<Integer, Integer, List<String>>>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<Integer, Integer, List<String>> map(
							String arg0) throws Exception {
						// final varaibles
						int ID = 0;
						int cycle = 0;

						List<String> Stringmeasurements = Arrays.asList(arg0
								.split(" "));
						List<String> ReducedStringMeasurements = new ArrayList<String>();
						try {
							for (int i = 0; i < Stringmeasurements.size(); i++) {
								if (i == 0) {
									// engineID
									ID = Integer.valueOf(Stringmeasurements
											.get(i));

								} else if (i == 1) {

									cycle = Integer.valueOf(Stringmeasurements
											.get(i));
								} else
									ReducedStringMeasurements.add(i - 2,
											Stringmeasurements.get(i));

							}// for
						} catch (Exception e) {

						}// catch

						return new Tuple3<Integer, Integer, List<String>>(ID,
								cycle, Stringmeasurements);
					}

				})
				//.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Integer, Integer, List<Double>>>() {
//
//			        @Override
//			        public long extractAscendingTimestamp(Tuple3<Integer, Integer, List<Double> element) {
//			        	long time = element.f1;  
//			            return time;
//			        }
			//});
			;// first map from String to List

		// should contain max cycles per engine (1, 192), (2, 235), ...
		DataStream<Tuple2<Integer, Integer>> maxCycle = train01
				.map(new MapFunction<Tuple3<Integer, Integer, List<String>>, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> map(
							Tuple3<Integer, Integer, List<String>> arg0)
							throws Exception {

						return new Tuple2<Integer, Integer>(arg0.f0, 1);
					}
				}).keyBy(0)
				.timeWindow(Time.seconds(5), Time.seconds(1))
				.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {

					/**
			 * 
			 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> reduce(
							Tuple2<Integer, Integer> arg0,
							Tuple2<Integer, Integer> arg1) throws Exception {

						return new Tuple2<Integer, Integer>(arg0.f0, arg0.f1
								+ arg1.f1);
					}
				});

		// functioniert so relativ gut, mit Abweichungen
		// maxCycle.print();

		// Tuple3 = engineID, cycle, list of setting and sensorvalues
		DataStream<Tuple3<Integer, Integer, List<Double>>> train02 = train01
				//.keyBy(0)
				//.timeWindow(Time.seconds(5), Time.seconds(1))
				.flatMap(
						new FlatMapFunction<Tuple3<Integer, Integer, List<String>>, Tuple3<Integer, Integer, List<Double>>>() {

							private static final long serialVersionUID = 1L;

							@Override
							public void flatMap(
									Tuple3<Integer, Integer, List<String>> arg0,
									Collector<Tuple3<Integer, Integer, List<Double>>> out)
									throws Exception {
								// final varaibles
								List<Double> measures = new ArrayList<Double>();

								// double check size()
								try {
									for (int i = 0; i < arg0.f2.size(); i++) {

										Double sensor = Double.valueOf(arg0.f2
												.get(i));
										measures.add(i, sensor);
									}// for

									Tuple3<Integer, Integer, List<Double>> train1 = new Tuple3<Integer, Integer, List<Double>>(
											arg0.f0, arg0.f1, measures);

									out.collect(train1);
								} catch (Exception e) {

								}// catch
							}
						});// prep1 finish

		// System.out.println(train02.toString());
		//maxCycle.print();
		DataStream<Tuple4<Integer, Integer, List<Double>, Integer>> train03 = maxCycle
				.join(train02)
				.where(new KeySelector<Tuple2<Integer, Integer>, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer getKey(Tuple2<Integer, Integer> arg0)
							throws Exception {

						return arg0.f0;
					}

				})
				.equalTo(
						new KeySelector<Tuple3<Integer, Integer, List<Double>>, Integer>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Integer getKey(
									Tuple3<Integer, Integer, List<Double>> arg0)
									throws Exception {

								return arg0.f0;
							}

						})
				
				.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
				.apply(new JoinFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, List<Double>>, Tuple4<Integer, Integer, List<Double>, Integer>>() {
					// here rul is calculated with maxCycles - current cycle 
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple4<Integer, Integer, List<Double>, Integer> join(
							Tuple2<Integer, Integer> arg0,
							Tuple3<Integer, Integer, List<Double>> arg1)
							throws Exception {
						int rul = 1;
						if(arg0.f1>arg0.f1){
						rul = arg0.f1-arg1.f1;
						}
						
						return new Tuple4<Integer, Integer, List<Double>, Integer>(arg1.f0, arg1.f1, arg1.f2,rul);
					}

				});
		
		//val learner = MultiLinearRegression(); 
		train03.print();
		System.out.println("fertig!");
		// execute program
		env.execute("Rul");

	}// main

}