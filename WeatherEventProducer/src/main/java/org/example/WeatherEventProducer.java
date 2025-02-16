package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.VoidSerializer;

import java.util.Properties;

public class WeatherEventProducer {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,WeatherEventJsonSerializer.class);

        Producer<Void, WeatherEvent> producer = new KafkaProducer<>(config);

        WeatherEvent message = new WeatherEvent();
        message.setTemperature(33.5);
        message.setLatitude(6.1944);
        message.setLongitude(106.8229);

        String topic = "weather-events";


        ProducerRecord<Void, WeatherEvent> record = new ProducerRecord<>(topic, message);

        producer.send(record);
        producer.close();

    }
}
