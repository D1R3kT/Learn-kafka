package org.example.avro.serdes;

public class SerializationException extends RuntimeException {
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
