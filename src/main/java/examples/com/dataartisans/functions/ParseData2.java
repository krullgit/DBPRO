package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ParseData2 implements MapFunction<KeyedDataPoint<Double>, KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;


    String datatype = "";

    public ParseData2(String datatype){
        this.datatype = datatype;

    }




    @Override
    public KeyedDataPoint<Double> map(KeyedDataPoint<Double> record) {


        return new KeyedDataPoint<Double>(datatype, record.getTimeStampMs(), record.getMf01(),record.getMf02(),record.getMf03());
    }
}