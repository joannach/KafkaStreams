package com.studia.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

/**
 * Created by Bartek on 11.05.2017.
 */
public class Main {
    public static int counter = 0;
    
    public static void main(String[] args) {

        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tttt");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream("test");

       // source
       //         .map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
       //
        //            public KeyValue<String, String> apply(String key, String value) {
        //                System.out.println(value);
        //                return new KeyValue<String, String>(value, value);
        //            }
        //        }).print();
        
        KTable<String, String> counts;
        counts = source
                .flatMapValues(new ValueMapper<String, Iterable<Long>>() {
                    @Override
                    public Iterable<String> apply(String value) { 
                        double currentVal = Double.parseDouble(value);
                        if(currentVal>2)
                            counter++;
                        return Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" "));
                    }
                });

        // need to override value serde to Long type
        counts.to(Serdes.String(), Serdes.String(), "streams-wordcount-output");
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

    }
}
