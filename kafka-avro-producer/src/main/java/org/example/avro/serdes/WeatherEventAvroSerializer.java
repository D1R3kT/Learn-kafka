package org.example.avro.serdes;


import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Serializer;
import org.example.avro.WeatherEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HexFormat;

public class WeatherEventAvroSerializer implements Serializer<WeatherEvent> {
    private static final Logger log = LoggerFactory.getLogger(WeatherEventAvroSerializer.class);
    private static final HexFormat hexFormat = HexFormat.ofDelimiter(":");

    @Override
    public byte[] serialize(String topic, WeatherEvent event) {
        if(event == null) {
            return null;
        }

        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            DatumWriter<WeatherEvent> datumWriter = new SpecificDatumWriter<>(WeatherEvent.class);
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

            //сериализуем данные
            datumWriter.write(event, encoder);

            //сбравсываем все данные в поток
            encoder.flush();

            //возвращаем сериализованные даныне
            byte[] bytes = outputStream.toByteArray();

            log.info("Данные успешно сериализованы в формат Avro:\n{}", hexFormat.formatHex(bytes));

            return bytes;
        } catch (IOException exp) {
            throw new SerializationException("Ошибка сериализации экземпляра WeatherEvent", exp);
        }
    }
}
