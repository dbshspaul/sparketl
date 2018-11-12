package com.globalids.util;

import com.databricks.spark.avro.SchemaConverters;
import com.globalids.data.generator.GeneratorDemo;
import com.globalids.spark.StructuredStreamProcessor;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Properties;

public class ConsumerCreator {
    private static Injection<GenericRecord, byte[]> recordInjection;
    private static StructType type;
    private static Schema.Parser parser = new Schema.Parser();
    private static Schema schema = parser.parse(GeneratorDemo.USER_SCHEMA);

    static {
        //once per VM, lazily
        recordInjection = GenericAvroCodecs.toBinary(schema);
        type = (StructType) SchemaConverters.toSqlType(schema).dataType();

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        Consumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("mytopic"));

        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

        while (true) {
            ConsumerRecords<String, byte[]> consumerRecords = consumer.poll(1000);
            consumerRecords.forEach(record -> {
                try {
                    System.out.println(StructuredStreamProcessor.deserialize(record.value(), schema));
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
//                GenericRecord record1 = null;
//                try {
//                    record1 = datumReader.read(null, decoder);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
                try {
                    GenericRecord record1 = recordInjection.invert(record.value()).get();
                    System.out.println(record1);
                } catch (Exception e) {
                    System.out.println("Err");
                }
            });

        }
    }
}