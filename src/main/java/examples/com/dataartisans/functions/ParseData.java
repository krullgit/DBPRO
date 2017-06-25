package examples.com.dataartisans.functions;

import examples.com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class ParseData extends RichMapFunction<String, KeyedDataPoint<Double>> {
    private static final long serialVersionUID = 1L;

    String datatype = "test"; // set test key

    // variables for measuring time
    long start = 0;
    public long sum = 0;
    public long countSum = 0;

    @Override
    public KeyedDataPoint<Double> map(String record) {

            // measure time in nanos
            if(start == 0){
                start = System.nanoTime();
                System.out.println("FIRST");
            }
            sum += Math.abs(System.nanoTime() - start);
            start = System.nanoTime();
            countSum++;
            if (countSum%10000==0){ // print the avg every 10000 tuples
                System.out.println(sum/countSum);
            }

        //parse tuple to KeyedDataPoint
        String rawData = record;
        String[] data = rawData.split("\t");

        // the data look like this...
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

        return new KeyedDataPoint<Double>(datatype, millisSinceEpoch, Double.valueOf(data[2]),Double.valueOf(data[3]),Double.valueOf(data[4]));
    }
}