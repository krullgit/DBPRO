package examples.windowing;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import examples.com.dataartisans.functions.MovingRangeFunction;
import examples.com.dataartisans.functions.RangeErrorFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import examples.com.dataartisans.data.KeyedDataPoint;
import examples.com.dataartisans.functions.AverageWindowFunction;
import examples.com.dataartisans.functions.MovingAverageFunction;
import examples.com.dataartisans.sinks.InfluxDBSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;


public class slidingWindowDebsChallenge {


	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		@SuppressWarnings({"rawtypes", "serial"})
		
		
		// Read and parse the original data
		DataStream<KeyedDataPoint<Double>> debsData;
		
		// test with this parameters: -input ./src/main/resources/DEBS2012-ChallengeData-Sample.csv
		debsData = 	env.readTextFile(params.get("input"))				      
				      .map(new ParseData());
				    
		// This is the original data
		debsData.addSink(new InfluxDBSink<>("debsData"));

		// First aggregate the data per second
		DataStream<KeyedDataPoint<Double>> debsDataAggSec = debsData
				// TODO: it seems we need to add time stamps to the data, how do we do when the timestamps do not come from the file or stream ???
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				// 
				.keyBy("key")
				// DEBS data is sampled 1000Hz <-- 1000 samples per second
				.timeWindow(Time.seconds(1)).trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
				//			
				.apply(new AverageWindowFunction());					   
			     
		debsDataAggSec.addSink(new InfluxDBSink<>("debsDataAggSec"));

		
		// Now apply a window average function to smooth the data
		debsDataAggSec
		        .keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1))).trigger(CountTrigger.of(10))
				//
				.apply(new MovingAverageFunction())
				// save the average data for every key in a different series
			    .name("debsDataMovAvg")
			    //.print();
			    .addSink(new InfluxDBSink<>("debsDataMovAvg"));

		DataStream<KeyedDataPoint<Double>> debsDataRange = debsData
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1))).trigger(CountTrigger.of(10))
				.apply(new MovingRangeFunction());

				debsDataRange.addSink(new InfluxDBSink<>("debsDataRange"));

		DataStream<KeyedDataPoint<Double>> debsDataRangeErrors = debsDataRange
				.keyBy("key")
				.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.seconds(1))).trigger(CountTrigger.of(10))
				.apply(new RangeErrorFunction());

				debsDataRangeErrors.addSink(new InfluxDBSink<>("debsDataRangeErrors"));

		env.execute("slidingWindowExample");
	}

                                                                       
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
			System.out.println("  ts:" + data[0] + "   " + millisSinceEpoch + "  nano:" + ts_nano + " mf01:" + Double.valueOf(data[2]));
		
			
			return new KeyedDataPoint<Double>("mf01", millisSinceEpoch, Double.valueOf(data[2]));
		}
	}
	
	private static class ExtractTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
			return element.getTimeStampMs();
		}
	}
	
}
