package examples.windowing;

import java.util.ArrayList;
import java.util.Properties;

import examples.com.dataartisans.functions.*;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import examples.com.dataartisans.data.KeyedDataPoint;
import examples.com.dataartisans.sinks.InfluxDBSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import examples.com.dataartisans.functions.ExtractTimestamp;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


public class slidingWindowDebsChallenge {

	public static ArrayList<KeyedDataPoint<Double>> testlist = new ArrayList<KeyedDataPoint<Double>>();
	public static void main(String[] args) throws Exception {

		final double errortreshold = 0.3;

		final ParameterTool params = ParameterTool.fromArgs(args);

		// setup flink StreamExecutionEnvironment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// we need event time since we since it fits to our window constraints (1 and 60 seconds of event time)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


		// READ FROM KAFKA
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		/*FlinkKafkaConsumer010<String> myConsumer =
				new FlinkKafkaConsumer010<>("debsData4", new SimpleStringSchema(), properties);

		// Parse Data
		DataStream<KeyedDataPoint<Double>> debsData = env
				.setParallelism(1)
				.addSource(myConsumer)
				.map(new ParseData());*/

		// READ FROM FILE
		// test with this parameters: -input ./src/main/resources/DEBS2012-ChallengeData-Sample.csv
		DataStream<KeyedDataPoint<Double>> debsData = env.readTextFile(params.get("input"))
				.setParallelism(1)
				.map(new ParseData());

		/*debsData.addSink(new InfluxDBSink<>("debsData"));*/

		// Save 20 sec before and 70 sec after an error
		DataStream<KeyedDataPoint<Double>> debsDataRangeBuffer = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeBuffer(errortreshold));
		debsDataRangeBuffer.addSink(new InfluxDBSink<>("debsDataRangeBuffer"));

		// calculate the range
		DataStream<KeyedDataPoint<Double>> debsDataRange = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRange());
		debsDataRange.addSink(new InfluxDBSink<>("debsDataRange"));

		// calculate the errors based in the range
		DataStream<KeyedDataPoint<Double>> debsDataRangeErrors = debsDataRange
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeError(errortreshold));
		debsDataRangeErrors.addSink(new InfluxDBSink<>("debsDataRangeErrors"));

		// calculate the avg
		DataStream<KeyedDataPoint<Double>> debsDataAvg = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingAverage());
		debsDataAvg.addSink(new InfluxDBSink<>("debsDataAvg"));

		// calculate power consumption
		DataStream<KeyedDataPoint<Double>> debsDataAvgPwr = debsDataAvg
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(60)))
				.apply(new MovingAveragePwr());
		debsDataAvgPwr.addSink(new InfluxDBSink<>("debsDataAvgPwr"));
		
		env.execute("debsChallenge");
	}
}
		/*DataStream<KeyedDataPoint<Double>> debsdata20sec70sec = debsData
				.flatMap(new buffer());

		debsdata20sec70sec.addSink(new InfluxDBSink<>("debsdata20sec70sec"));*/
		/*
		SingleOutputStreamOperator<KeyedDataPoint<Double>> keyedDataPointSingleOutputStreamOperator = debsData.connect(debsDataRangeErrors).keyBy("key", "key").flatMap(
				new CoFlatMapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, KeyedDataPoint<Double>>() {
					private transient ValueState<KeyedDataPoint<Double>> state;

					private final ValueStateDescriptor<Integer> count =
							new ValueStateDescriptor<>("count", IntSerializer.INSTANCE, 0);


					@Override
					public void flatMap2(KeyedDataPoint<Double> value, Collector<KeyedDataPoint<Double>> out) throws Exception {

					}

					@Override
					public void flatMap1(KeyedDataPoint<Double> value, Collector<KeyedDataPoint<Double>> out) throws Exception {

						System.out.println(count.getDefaultValue());
						System.out.println("OUT1");

						out.collect(value);
					}
				});
		*/

		/*
		DataStream<KeyedDataPoint<Double>> debsDatajoin = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRange());
		*/
		/*
		//∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨
		DataStream<KeyedDataPoint<Double>> debsDatajoined = debsDataRange.join(debsDataRangeErrors)
				.where(new NameKeySelector()).equalTo(new NameKeySelector())
    			.window(TumblingEventTimeWindows.of(Time.seconds(1)))
				.apply (new MyJoinFunction());


		debsDatajoined.addSink(new InfluxDBSink<>("debsDataJoined"));

		//∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧
		*/



	//∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨∨

	/*
	public static class MyJoinFunction
			implements
			JoinFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, KeyedDataPoint<Double>> {

		private static final long serialVersionUID = 1L;

		private KeyedDataPoint<Double> joined = new KeyedDataPoint<>();

		@Override
		public KeyedDataPoint<Double> join(KeyedDataPoint<Double> first,
										   KeyedDataPoint<Double> second) throws Exception {


			return new KeyedDataPoint<Double>("mf01", first.getTimeStampMs(), second.getValue()+1.0);
		}
	}
	private static class NameKeySelector implements KeySelector<KeyedDataPoint<Double>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(KeyedDataPoint<Double> value) throws Exception {
			return String.valueOf(value.getTimeStampMs());
		}
	}
	//∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧∧
	*/


