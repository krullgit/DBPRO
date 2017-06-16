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

public class MovingRangeFunctionBuffer implements WindowFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>, Tuple, TimeWindow> {
    public boolean bufferextend = false;
    public long bufferAlertElementTime = 0;
    public ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>();
    @Override
    public void apply(Tuple arg0, TimeWindow window, Iterable<KeyedDataPoint<Double>> input, Collector<KeyedDataPoint<Double>> out) {
        String winKey = input.iterator().next().getKey();
        Double rangeMf01 = 0.0;
        Double rangeMf02 = 0.0;
        Double rangeMf03 = 0.0;
        double errortreshold = 0.27;

        // get max and min of the elements in the window
        if(winKey.equals("mf01")){
            double maxMf01 = 0;
            double minMf01 = 1000000000;

            for (KeyedDataPoint<Double> in: input) {
                if (in.getMf01() < minMf01) {
                    minMf01 = in.getMf01();}
                if (in.getMf01() > maxMf01) {
                    maxMf01 = in.getMf01();
                   }
                bufferlist.add(in);
            }
            rangeMf01 = (maxMf01-minMf01)/maxMf01;

        }
        if(winKey.equals("mf02")){
            double maxMf02 = 0;
            double minMf02 = 1000000000;

            for (KeyedDataPoint<Double> in: input) {
                if (in.getMf02() < minMf02) {
                    minMf02 = in.getMf02();}
                if (in.getMf02() > maxMf02) {
                    maxMf02 = in.getMf02();
                    }
                bufferlist.add(in);
            }
            rangeMf02 = (maxMf02-minMf02)/maxMf02;
        }
        if(winKey.equals("mf03")){
            double maxMf03 = 0;
            double minMf03 = 1000000000;

            for (KeyedDataPoint<Double> in: input) {
                if (in.getMf03() < minMf03) {
                    minMf03 = in.getMf03();}
                if (in.getMf03() > maxMf03) {
                    maxMf03 = in.getMf03();
                    }
                bufferlist.add(in);
            }
            rangeMf03 = (maxMf03-minMf03)/maxMf03;
        }


        //TODO: Send Buffer as far as an ELement arrives
        if(bufferlist.isEmpty() == false) {
            if (bufferextend == false) {
                while (Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime().minus(20, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(0).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) > 0) {
                    bufferlist.remove(0);
                }
            } else {
                if (Instant.ofEpochMilli(bufferAlertElementTime).atZone(ZoneId.of("UTC+1")).toLocalTime().plus(70, SECONDS).compareTo(Instant.ofEpochMilli(bufferlist.get(bufferlist.size() - 1).getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime()) < 0) {
                    bufferextend = false;

                    for (KeyedDataPoint<Double> ready : bufferlist) {
                        //System.out.println(Instant.ofEpochMilli(ready.getTimeStampMs()).atZone(ZoneId.of("UTC+1")).toLocalTime());
                        out.collect(ready);
                    }
                    bufferlist.clear();
                }
            }
            if (rangeMf01 > errortreshold || rangeMf02 > errortreshold || rangeMf03 > errortreshold) {
                bufferAlertElementTime = input.iterator().next().getTimeStampMs();
                bufferextend = true;
            }
        }

    }
}
