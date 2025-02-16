package org.example.avro.serdes;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.avro.WeatherEvent;

public class WeatherEventAvroDeserializer implements Deserializer<WeatherEvent> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private final DatumReader<WeatherEvent> reader = new SpecificDatumReader<>(WeatherEvent.getClassSchema());

    @Override
    public WeatherEvent deserialize(String topic, byte[] data) {
        try {
            if (data != null) {
                BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
                return this.reader.read(null, decoder);
            }
            return null;
        } catch (Exception exp) {
            throw new DeserializationException("Ошибка десереализации данных из топика [" + topic + "]", exp);
        }
    }
}
