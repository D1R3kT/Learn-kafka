package org.example;

import lombok.Data;

@Data
public class WeatherEvent {
    private double latitude;
    private double longitude;
    private double temperature;

    @Override
    public String toString() {
        return String.format("WeatherEvent{latitude=%s, longtitude=%s temperature=%s}",
                latitude, longitude, temperature);
    }
}
