## Apache Flink stream processing exercises

- **src/main/java** <br>
   - **wordcount** <br>            
   - **quickstart** <br>
   - **examples**:<br>
      - **com.dataartisans** <-- this contains a simple version of the oscon example from dataartisans (see below the link),                       plus one implementation of moving average using KeyedDapoint <br>
      - **examples.com.dataartisans.functions.MovingAverageFunction** (NOTE: this function is used in example   sliding_window2.java )<br>    
   - **windowing**: <br>
      - GroupedProcessingTimeWindowExample.java <br>
      - SessionWindowing.java<br>
      - TopSpeedWindowing.java<br>
      - WindowWordCount.java<br>
      - **sliding_window0.java** <-- simple example of sliding window <br>
      - **sliding_window1.java** <-- implements moving average usinf Tuple<br>
      - **slidingWindowWaterLevel.java** <-- based on the oscon example, this loads water temperature data from                                 resources/noaa_water_level.csv , the idea here is to calculate moving average of this data; this data contains timestamps so there is no need to generate them. With this funtion we obtain the same results as in grafana - moving_average(100)
      - **slidingWindowMimicData.java** <-- Example with data from medical domain https://physionet.org/challenge/2009/training-set.shtml one example is copied in resources/a40834n.csv
      - **slidingWindowDebsChallenge.java** <- Example with data from the DEBS 2012 challenge http://debs.org/?p=38 a short example is copied in resources/DEBS2012-ChallengeData-Sample.csv

## Links:

- quickstart contains examples from the archetype in: http://dataartisans.github.io/flink-training/devEnvSetup.html

- examples.windowing <br> 
 from: https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing
    
- examples.wordcount <br> 
 from: https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java

- examples.com.dataartisans <br>
 example based on the oscon tutorial: https://data-artisans.com/blog/robust-stream-processing-flink-walkthrough 
 this example used influxdb and grafana
   

## Data in resources
    car_data_ori.csv <- this data includes timestamps in millisecs
        0,55,15.277777777777777,1424951918630
        1,45,12.5,1424951918632
        0,50,29.166666666666664,1424951919632
        1,50,26.38888888888889,1424951919632
        0,55,44.44444444444444,1424951920633
        ...

    elecsales.csv <- simple data containing sales per year 
        year,sales
        1989,2354.34
        1990,2379.71
        1991,2318.52
        ...

    noaa_water_level.csv <- data used in the tutorial of influxdb: 
        https://docs.influxdata.com/influxdb/v1.2/query_language/data_download/
        measure,location,water_level,timestamp(secs)
        h2o_feet,coyote_creek,8.120,1439856000
        h2o_feet,coyote_creek,8.005,1439856360
        h2o_feet,coyote_creek,7.887,1439856720
        h2o_feet,coyote_creek,7.762,1439857080
        ...

