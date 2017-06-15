package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;
    String datatype = "";
    int index = -1;
    public ParseData(String datatype){
        this.datatype = datatype;
        switch (datatype) {
            case "mf01": index = 2;
                break;
            case "mf02": index = 3;
                break;
            case "mf03": index = 4;
                break;
        }
    }
    long lastTime = 0;
    long wait = 0;
    @Override
    public KeyedDataPoint<Double> map(String record) {

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
        /*
        if(lastTime == 0){
            lastTime = millisSinceEpoch;
        }
        if (lastTime - millisSinceEpoch > 0){
            wait = lastTime - millisSinceEpoch;
        }else{
            wait = 0;
        }*/

        try {
            TimeUnit.MILLISECONDS.sleep(40);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return new KeyedDataPoint<Double>(datatype, millisSinceEpoch, Double.valueOf(data[index]));
    }
}