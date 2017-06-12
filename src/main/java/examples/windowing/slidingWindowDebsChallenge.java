package examples.windowing;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import scala.Tuple2;


public class slidingWindowDebsChallenge {

	public static ArrayList<KeyedDataPoint<Double>> testlist = new ArrayList<KeyedDataPoint<Double>>();
	public static void main(String[] args) throws Exception {


		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);





		// Read and parse the original data
		DataStream<KeyedDataPoint<Double>> debsData;

		// test with this parameters: -input ./src/main/resources/DEBS2012-ChallengeData-Sample.csv
		debsData = 	env.readTextFile(params.get("input"))
				.map(new ParseData());

		debsData.addSink(new InfluxDBSink<>("debsData"));



		// Operator 1 -> compute the avg and the range of mf01
		// TODO: do the same for mf02 and mf03
		// TODO: allowedLateness must be implemented to consider late cumming events
		// TODO: read about watermarks in flink. surely not unimportant
		// TODO: set parallelism to what? 3?
		DataStream<KeyedDataPoint<Double>> debsDataRangeBuffer = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				// .trigger(CountTrigger.of(10)) why should be use a trigger? By the way: I don't get why triggers exist :(
				.apply(new MovingRangeFunctionBuffer());

		debsDataRangeBuffer.addSink(new InfluxDBSink<>("debsDataRangeBuffer"));

		DataStream<KeyedDataPoint<Double>> debsDataRange = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				// .trigger(CountTrigger.of(10)) why should be use a trigger? By the way: I don't get why triggers exist :(
				.apply(new MovingRangeFunction());

		debsDataRange.addSink(new InfluxDBSink<>("debsDataRange"));

		// Operator 4 -> "is range over 0.3?"
		DataStream<KeyedDataPoint<Double>> debsDataRangeErrors = debsDataRange
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1)))
				.apply(new RangeErrorFunction());

		debsDataRangeErrors.addSink(new InfluxDBSink<>("debsDataRangeErrors"));

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
		env.execute("slidingWindowExample");
		System.out.println( "TEST" );
		System.out.println( testlist );
		System.out.println( "TEST" );
		System.out.println(buffer.bufferextend + "test");
	}
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

	private static class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override    //Tuple4<key, timestamp, nano, measure
		public KeyedDataPoint<Double> map(String record) {

			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split("\t");


			// the data look like this... and we want to process mf01 <- field 2
			// for this example I remove the first line...
			// ts	index	mf01	mf02	mf03	pc13	pc14	pc15	pc25	pc26	pc27	res	bm05	bm06	bm07	bm08	bm09	bm10	pp01	pp02	pp03	pp04	pp05	pp06	pp07	pp08	pp09	pp10	pp11	pp12	pp13	pp14	pp15	pp16	pp17	pp18	pp19	pp20	pp21	pp31	pp32	pp33	pp34	pp35	pp36	pc01	pc02	pc03	pc04	pc05	pc06	pc19	pc20	pc21	pc22	pc23
			// 2012-02-22T16:46:28.9670320+00:00	2556001	13056	14406	8119	0071	0193	0150	0000	0000	0000	0000	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	1	0	0	0	0	0	0	1	0	0	1	0	0	0	0	0	0	0	0	0	0	0
			// 2012-02-22T16:46:28.9770284+00:00	2556002	13054	14405	8119	0069	0192	0151	0000	0000	0000	0000	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	1	0	0	0	0	0	0	1	0	0	1	0	0	0	0	0	0	0	0	0	0	0
			// 2012-02-22T16:46:28.9870216+00:00	2556003	13049	14404	8119	0070	0194	0152	0000	0000	0000	0000	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	0	1	1	0	0	0	0	0	0	1	0	0	1	0	0	0	0	0	0	0	0	0	0	0						   


			Instant ts = LocalDateTime.parse(data[0], DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSXXX"))
					//.atZone(ZoneId.systemDefault())
					.atZone(ZoneId.of("UTC"))
					.toInstant();

			int ts_nano = ts.getNano();
			long millisSinceEpoch = ts.toEpochMilli() + (ts_nano/1000000);
			//System.out.println("  ts:" + data[0] + "   " + millisSinceEpoch + "  nano:" + ts_nano + " mf01:" + Double.valueOf(data[2]));


			return new KeyedDataPoint<Double>("mf01", millisSinceEpoch, Double.valueOf(data[2]),Double.valueOf(data[3]),Double.valueOf(data[4]));
		}
	}
	// TODO: maybe change the AscendingTimestampExtractor because of following messages:
	// WARN  org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor  - Timestamp monotony violated: 1329929424054 < 1329929424994
	private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
			return element.getTimeStampMs();
		}
	}

}
