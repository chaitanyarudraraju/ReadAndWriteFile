package org.file.transfer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Properties;

public class WriteFile {
    public static void main(String[] args) throws FileNotFoundException {
        //set the values to properties and configure consumer
        Properties writing= new Properties();
        writing.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        writing.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        writing.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        writing.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Writer");
        writing.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//create kafkaconsumer Object
        KafkaConsumer<String,String> write= new KafkaConsumer<>(writing);
        write.subscribe(Arrays.asList("transfer"));


        //Writing the read output to file line by line
        File line=new File("C:\\Users\\incrudr\\recieve.txt");
        FileOutputStream output=new FileOutputStream(line);
        BufferedWriter saveLine= new BufferedWriter(new OutputStreamWriter(output));

        while(true)
        {
            ConsumerRecords<String,String> records= write.poll(100);
            for (ConsumerRecord<String,String> record: records)
            {
                try {
                    saveLine.write(record.value().toString());
                    System.out.println(record.value().toString());
                    saveLine.newLine();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

    }
}
