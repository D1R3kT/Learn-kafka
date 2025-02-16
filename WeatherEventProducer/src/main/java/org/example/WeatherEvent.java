package org.example;

import lombok.Data;

@Data
public class WeatherEvent {
    private double latitude;
    private double longitude;
    private double temperature;
}
