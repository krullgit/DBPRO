package examples.com.dataartisans.kafka;

/**
 * Created by matthes on 24.06.17.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import org.apache.kafka.clients.producer.*;


public class kafkaProducer{

    public static void main(String[] args){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topicName = "debsData6";
        String key = "Key";

        File testfile = new File("/Users/matthes/Desktop/passt_nicht_in_gdrive/dbpro/DEBS2012-ChallengeData-Sample.csv");
        Producer<String, String> producer = new KafkaProducer <>(props);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(testfile));
            String line;
            while ((line = reader.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key, line);
                producer.send(record);

                Thread.sleep(1+(int)(Math.random()*10));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}

