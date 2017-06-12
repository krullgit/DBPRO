package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;



public class buffer implements FlatMapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>> {

    public static boolean bufferextend = false;
    public static KeyedDataPoint<Double> bufferAlertElement = null;
    public static ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>();

    @Override
    public void flatMap(KeyedDataPoint<Double> value, Collector<KeyedDataPoint<Double>> out) throws Exception {


        bufferlist.add(value);
        if (bufferextend == false) {
            System.out.println("BEFORE2");
            while (Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime().plus(30, SECONDS).compareTo(Instant.ofEpochMilli(value.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime()) == 0) {
                System.out.println("BEFORE3");
                bufferlist.remove(bufferlist.size() - 1);
                System.out.println("AFTER1");
            }
        } else {
            System.out.println("AFTER2");
            if (Instant.ofEpochMilli(bufferAlertElement.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime().plus(70, SECONDS).compareTo(Instant.ofEpochMilli(value.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime()) == 1){
                bufferextend = false;

                for (KeyedDataPoint<Double> ready : bufferlist) {
                    out.collect(ready);
                }
                bufferlist.clear();
            }
        }
    }
}









/*
public class buffer implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {

    public static boolean bufferextend = false;
    public static KeyedDataPoint<Double> bufferAlertElement = null;
    public static ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>();

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {

        for (KeyedDataPoint<Double> in : input) {


            bufferlist.add(in);
            if (bufferextend == false) {
                while (Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime().plus(30, SECONDS).compareTo(Instant.ofEpochMilli(in.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime()) == 0) {
                    bufferlist.remove(bufferlist.size() - 1);
                }
            } else {

                if (Instant.ofEpochMilli(bufferAlertElement.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime().plus(70, SECONDS).compareTo(Instant.ofEpochMilli(in.getTimeStampMs()).atZone(ZoneId.of("UTC")).toLocalTime()) == 1){
                    bufferextend = false;

                    for (KeyedDataPoint<Double> ready : bufferlist) {
                        out.collect(ready);
                    }
                    bufferlist.clear();
                }
            }
        }
    }
}
*/
