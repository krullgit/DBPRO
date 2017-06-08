package examples.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import examples.com.dataartisans.data.KeyedDataPoint;
import examples.com.dataartisans.functions.MovingAverageFunction;
import examples.com.dataartisans.sinks.InfluxDBSink;


public class slidingWindowWaterLevel {
	
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		@SuppressWarnings({"rawtypes", "serial"})
		
		DataStream<KeyedDataPoint<Double>> waterData;
		// test with file in: src/main/resources/elecsales_keyed.csv
		waterData = env.readTextFile(params.get("input")).map(new ParseData());
		
		waterData.addSink(new InfluxDBSink<>("waterLevelData"));
		
		//waterData.print();
		
		
		waterData
				// TODO: it seems we need to add time stamps to the data, how do we do when the timestamps do not come from the file or stream ???
				.assignTimestampsAndWatermarks(new ExtractTimestamp())
				// TODO: this is another issue, if our stream is not keyed then how do we do?
				//       the problem is that the window functions most of the time are applied to keyed windows
				.keyBy("key")
				// since the noaa_water_level.csv is sampled every 6 minutes then to get almost the same result as in grafana:
				// here the size of the window is 600 (100 points or samples) and step of 6 minutes (1 sample in grafana)			
				// SOLVED: adding the trigger will ensure that the window will not produce any value unless the window contains already the expected size
				//         here very point is taken every 6 minutes we want a moving average every 600 minutes, and we want then to trigger just when the window
				//         has this amount of data, in this case every 100 points or samples...
				.window(SlidingEventTimeWindows.of(Time.minutes(600), Time.minutes(6))).trigger(CountTrigger.of(100))
				//			
				.apply(new MovingAverageFunction())
				
				// save the average data for every key in a different series
			    .name("waterLevelDataAvg")
			    //.print();				
			    .addSink(new InfluxDBSink<>("waterLevelDataAvg"));
		
		//waterData.print();
		    

		
		env.execute("slidingWindowExample");
	}


	
	private static class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		
		@Override
		public KeyedDataPoint<Double> map(String record) {
			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split(",");
						
			// the data look like this...
			// measure,location,water_level,timestamp
			// h2o_feet,coyote_creek,8.120,1439856000
			// h2o_feet,coyote_creek,8.005,1439856360
			// h2o_feet,coyote_creek,7.887,1439856720
			// h2o_feet,coyote_creek,7.762,1439857080		            		
			return new KeyedDataPoint<Double>(data[1], Long.valueOf(data[3])*1000, Double.valueOf(data[2]));
			
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
