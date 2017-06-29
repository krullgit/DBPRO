package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;

import static java.time.temporal.ChronoUnit.SECONDS;

public class MovingRangeBuffer implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
    public boolean bufferextend = false; // if false -> save 20 sec in past, if true -> save additionally 70 sec in future
    public long bufferAlertElementTime = 0; // saves time of last alert
    public ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>(); // buffer for the 20 + 70 seconds to save
    public double errortreshold = 0; // treshold for the anomaly

    // constructor
    public MovingRangeBuffer(double errortreshold){
        this.errortreshold = errortreshold;
    }

    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        String winKey = input.iterator().next().getKey();

        // variables for the range calculation
        Double rangeMf01 = 0.0;
        Double rangeMf02 = 0.0;
        Double rangeMf03 = 0.0;
        double maxMf01 = 0;
        double minMf01 = 1000000000;
        double maxMf02 = 0;
        double minMf02 = 1000000000;
        double maxMf03 = 0;
        double minMf03 = 1000000000;


        // get max and min of the elements in the window
        for (KeyedDataPoint<Double> in: input) {
            if (in.getMf01() < minMf01) {
                minMf01 = in.getMf01();}
            if (in.getMf01() > maxMf01) {
                maxMf01 = in.getMf01();}
            if (in.getMf02() < minMf02) {
                minMf02 = in.getMf02();}
            if (in.getMf02() > maxMf02) {
                maxMf02 = in.getMf02();}
            if (in.getMf03() < minMf03) {
                minMf03 = in.getMf03();}
            if (in.getMf03() > maxMf03) {
                maxMf03 = in.getMf03();}
            //put every tuple in the buffer
            bufferlist.add(in);
        }

        // calculate ranges
        rangeMf01 = (maxMf01-minMf01)/maxMf01;
        rangeMf02 = (maxMf02-minMf02)/maxMf02;
        rangeMf03 = (maxMf03-minMf03)/maxMf03;


        // if bufferlist is not empty
        //TODO: Send Buffer as far as an ELement arrives
        if(bufferlist.isEmpty() == false) {
            // check if we want to out past 20 sec in the buffer
            if (bufferextend == false) {
                while (Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime().minus(20, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(0).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) > 0) {
                    //delete every tuple that has come later than 20 sec before
                    bufferlist.remove(0);
                }
            // or additional the future 70 sec
            } else {

                // is the latest tuple 70 sec in future of the last anomaly?
                if (Instant.ofEpochMilli(bufferAlertElementTime).atZone(ZoneId.of("UTC+1")).toLocalTime().plus(70, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) < 0) {
                    bufferextend = false; // since buffer is complete reset the bufferextend variable
                }
                // save hole buffer to influx
                for (KeyedDataPoint<Double> ready : bufferlist) {
                    out.collect(ready);
                }
                bufferlist.clear(); // empty buffer

            }
            // set bufferextend to true if one treshold is too high
            if (rangeMf01 > errortreshold || rangeMf02 > errortreshold || rangeMf03 > errortreshold) {
                bufferAlertElementTime = input.iterator().next().getTimeStampMs();
                bufferextend = true;
            }
        }
    }
}
