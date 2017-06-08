package examples.windowing;


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
import org.apache.flink.util.Collector;

import examples.com.dataartisans.data.KeyedDataPoint;
import examples.com.dataartisans.functions.AssignKeyFunction;
import examples.com.dataartisans.functions.MovingAverageFunction;
import examples.com.dataartisans.sinks.InfluxDBSink;


/**
 * Very first draft version of an sliding window
 * to run this use as parameters:
 *    -input ./src/main/resources/elecsales_keyed0.csv
 *    
 * TODO: how to work with data like: R/data/elecsales.csv  in streamAPI it seems there is no reader for csv
 */
public class sliding_window_energy {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		//env.getConfig().setGlobalJobParameters(params);
		//env.setParallelism(1);

		@SuppressWarnings({"rawtypes", "serial"})
		
		DataStream<KeyedDataPoint<Double>> salesData;
		// test with file in: src/main/resources/elecsales_keyed.csv
		// /projects/data/Energy/reduced/7252_reduced_data.csv
		salesData = env.readTextFile(params.get("input")).map(new ParseSalesData());
		
		salesData.addSink(new InfluxDBSink<>("energyData"));
		
		
		//DataStream<KeyedDataPoint<Double>> topSpeeds = 
		salesData
				// TODO: it seems we need to add time stamps to the data, how do we do when the timestamps do not come from the file or stream ???
				.assignTimestampsAndWatermarks(new SalesTimestamp())
				// TODO: this is another issue, if our stream is not keyed then how do we do?
				//       the problem is that the window functions most of the time are applied to keyed windows
				.keyBy("key")
				// TODO: this is another issue, how the scale in time stamps are handled
				//       how timestamps are handled in general
				//       SlidingEventTimeWindows.of(Time size, Time slide)
				.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
				// TODO: how to define and apply other window functions... here for example we need to get average and divide by the length of the window
				//       also we need to be careful with the borders...
				.apply(new MovingAverageFunction())				
			    .name("mySalesData")
			    //.print();
			    .addSink(new InfluxDBSink<>("mySalesData"));
			    

/*
		if (params.has("output")) {
			topSpeeds.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			topSpeeds.print();
		}
*/
		
		env.execute("CarTopSpeedWindowingExample");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	  public static class MovingAverageFunction1 implements WindowFunction<Tuple3<Integer, Integer, Double>, 
	                                                                       Tuple3<Integer, Integer, Double>, Tuple, TimeWindow> {

		  @Override
		  public void apply(Tuple arg0, TimeWindow window, Iterable<Tuple3<Integer, Integer, Double>> input, 
				                                          Collector<Tuple3<Integer, Integer, Double>> out) {
		    int count = 0;
		    double winsum = 0;
		    int key = 0;
		    
		    // get the sum of the elements in the window
		    for (Tuple3<Integer, Integer, Double> in: input) {
		      winsum = winsum + in.f2; 
		      count++;
		      key = in.f0;  // TODO: how to get the key of this window ??? I just need to do this once because all the elements
		                    //       of this window should have the same key, isn't it?
		    }
		    
		    Double avg = winsum/(1.0 * count);
		    
		    System.out.println("winsum=" +  winsum + "  count=" + count + "  avg=" + avg + "  time=" + window.getStart());
		    
		    int t = (int)window.getStart();
		   
		    out.collect(new Tuple3<Integer, Integer, Double>(key, t, avg));
		    
		  }
		}
	  
	
	

	private static class ParseSalesData extends RichMapFunction<String, KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public KeyedDataPoint<Double> map(String record) {
			//String rawData = record.substring(1, record.length() - 1);
			String rawData = record;
			String[] data = rawData.split(",");
						
			
			return new KeyedDataPoint<Double>(data[0], Long.valueOf(data[1])*100000, Double.valueOf(data[2]));
			
		}
	}

	private static class SalesTimestamp extends AscendingTimestampExtractor<KeyedDataPoint<Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(KeyedDataPoint<Double> element) {
			return element.getTimeStampMs();
		}
	}

}