package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


public class bufferdata{

    public static boolean bufferextend = false;
    public static KeyedDataPoint<Double> bufferAlertElement = null;
    public static ArrayList<KeyedDataPoint<Double>> bufferlist = new ArrayList<KeyedDataPoint<Double>>();


}







