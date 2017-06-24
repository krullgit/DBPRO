package examples.windowing;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import examples.com.dataartisans.functions.*;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import examples.com.dataartisans.data.KeyedDataPoint;
import examples.com.dataartisans.sinks.InfluxDBSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import examples.com.dataartisans.functions.ExtractTimestamp;



public class slidingWindowDebsChallenge {

	public static ArrayList<KeyedDataPoint<Double>> testlist = new ArrayList<KeyedDataPoint<Double>>();
	public static void main(String[] args) throws Exception {



		final ParameterTool params = ParameterTool.fromArgs(args);


		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);






		/*Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer08<String> myConsumer =
				new FlinkKafkaConsumer08<>("HealthTest3", new SimpleStringSchema(), properties);



		DataStream<KeyedDataPoint<Double>> debsDataMf01 = env
				.setParallelism(1)
				.addSource(myConsumer)
				.map(new ParseData("mf01"));*/










		//mf01
		// Read and parse the original data
		DataStream<KeyedDataPoint<Double>> debsData;
		debsData = env.readTextFile(params.get("input"))
				.setParallelism(1)
				.map(new ParseData());

		// test with this parameters: -input ./src/main/resources/DEBS2012-ChallengeData-Sample.csv
		DataStream<KeyedDataPoint<Double>> debsDataMf01 = 	debsData
				.map(new ParseData2("mf01"));
		debsDataMf01.addSink(new InfluxDBSink<>("debsDataMf01"));

		DataStream<KeyedDataPoint<Double>> debsDataMf02 = 	debsData
				.map(new ParseData2("mf02"));
		debsDataMf02.addSink(new InfluxDBSink<>("debsDataMf02"));

		DataStream<KeyedDataPoint<Double>> debsDataMf03 = 	debsData
				.map(new ParseData2("mf03"));
		debsDataMf03.addSink(new InfluxDBSink<>("debsDataMf03"));


		DataStream<KeyedDataPoint<Double>> debsDataRangeBufferMf01 = debsDataMf01
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeFunctionBuffer());
		debsDataRangeBufferMf01.addSink(new InfluxDBSink<>("debsDataRangeBuffer"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeBufferMf02 = debsDataMf02
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeFunctionBuffer());
		debsDataRangeBufferMf02.addSink(new InfluxDBSink<>("debsDataRangeBuffer"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeBufferMf03 = debsDataMf03
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeFunctionBuffer());
		debsDataRangeBufferMf03.addSink(new InfluxDBSink<>("debsDataRangeBuffer"));


		DataStream<KeyedDataPoint<Double>> debsDataRangeMf01 = debsDataMf01
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				// .trigger(CountTrigger.of(10)) why should be use a trigger? By the way: I don't get why triggers exist :(
				.apply(new MovingRangeFunction());
		debsDataRangeMf01.addSink(new InfluxDBSink<>("debsDataRangeMf01"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeMf02 = debsDataMf02
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeFunction());
		debsDataRangeMf02.addSink(new InfluxDBSink<>("debsDataRangeMf02"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeMf03 = debsDataMf03
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingRangeFunction());
		debsDataRangeMf03.addSink(new InfluxDBSink<>("debsDataRangeMf03"));


		DataStream<KeyedDataPoint<Double>> debsDataRangeErrors = debsDataRangeMf01
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new RangeErrorFunction());
		debsDataRangeErrors.addSink(new InfluxDBSink<>("debsDataRangeErrorsMf01"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeErrorsMf02 = debsDataRangeMf02
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new RangeErrorFunction());
		debsDataRangeErrorsMf02.addSink(new InfluxDBSink<>("debsDataRangeErrorsMf02"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeErrorsMf03 = debsDataRangeMf03
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new RangeErrorFunction());
		debsDataRangeErrorsMf03.addSink(new InfluxDBSink<>("debsDataRangeErrorsMf03"));


		DataStream<KeyedDataPoint<Double>> debsDataAvgMf01 = debsDataMf01
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.setParallelism(1)
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingAverageFunction());
		debsDataAvgMf01.addSink(new InfluxDBSink<>("debsDataAvgMf01"));

		DataStream<KeyedDataPoint<Double>> debsDataAvgMf02 = debsDataMf02
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingAverageFunction());
		debsDataAvgMf02.addSink(new InfluxDBSink<>("debsDataAvgMf02"));

		DataStream<KeyedDataPoint<Double>> debsDataAvgMf03 = debsDataMf03
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new MovingAverageFunction());
		debsDataAvgMf03.addSink(new InfluxDBSink<>("debsDataAvgMf03"));


		DataStream<KeyedDataPoint<Double>> debsDataAvgPwrMf01 = debsDataAvgMf01
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(60)))
				.apply(new MovingAverageFunctionOperator());
		debsDataAvgPwrMf01.addSink(new InfluxDBSink<>("debsDataAvgPwrMf01"));

		DataStream<KeyedDataPoint<Double>> debsDataAvgPwrMf02 = debsDataAvgMf02
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(60)))
				.apply(new MovingAverageFunctionOperator());
		debsDataAvgPwrMf02.addSink(new InfluxDBSink<>("debsDataAvgPwrMf02"));

		DataStream<KeyedDataPoint<Double>> debsDataAvgPwrMf03 = debsDataAvgMf03
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(60)))
				.apply(new MovingAverageFunctionOperator());
		debsDataAvgPwrMf03.addSink(new InfluxDBSink<>("debsDataAvgPwrMf03"));


		env.execute("debsChallenge");
		/*System.out.println("Avg delay: "+ExtractTimestamp.delaysum/ExtractTimestamp.count);
		System.out.println("timeone: "+ExtractTimestamp.timeone);
		System.out.println("timetwo: "+ExtractTimestamp.timetwo);
		System.out.println("delaysum: "+ExtractTimestamp.delaysum);
		System.out.println("count ALL: "+ExtractTimestamp.count);
		System.out.println("count: "+ExtractTimestamp.count1000);*/

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
				.apply(new MovingRangeFunction());
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


