package examples.com.dataartisans.jobs;

import examples.com.dataartisans.functions.*;
import examples.com.dataartisans.sinks.InfluxDBSink;
import examples.com.dataartisans.sources.TimestampSource;
import examples.com.dataartisans.data.DataPoint;
import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class OsconJob {

  public static void main(String[] args) throws Exception {

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();
    
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env);

    // Write this sensor stream out to InfluxDB
    
    sensorStream
      .addSink(new InfluxDBSink<>("mysensor"));
   
    sensorStream.print();
    
    
    sensorStream
    .keyBy("key")
    //.timeWindow(Time.seconds(5),Time.seconds(1))                                // Processing time will not give correct results
    //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))  //  Processing time will not give correct results 
    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))  // NOTE: this require to set env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                                                                           //       when it is necessary to set timestamps and watermarks???          
    .apply(new MovingAverageFunction())
    .name("mysensor")
    .addSink(new InfluxDBSink<>("mysensor"));
    

    // execute program
    env.execute("OSCON Example");
  }

  
  
  
  private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env) {

    // boiler plate for this demo
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.setParallelism(1);
    env.disableOperatorChaining();

    final int SLOWDOWN_FACTOR = 1;  // TODO: what for is this slowdown factor?
    final int PERIOD_MS = 1000;    // 1000=1sec 100=100msec

    // Initial data - just timestamped messages
    DataStreamSource<DataPoint<Long>> timestampSource =
      env.addSource(new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR), "test data");

    // Transform into sawtooth pattern
    SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
      .map(new SawtoothFunction(10))
      .name("sawTooth");

    // Make sine wave and use for pressure sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> pressureStream = sawtoothStream
      .map(new SineWaveFunction())
      .name("sineWave")
      .map(new AssignKeyFunction("pressure"))
      .name("assignKey(pressure");

    return pressureStream;
  }

}
