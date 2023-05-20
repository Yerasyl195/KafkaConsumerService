package com.example.kafkaconsumerservice.kafka;

import com.example.kafkaconsumerservice.model.ParkingSensor;
import com.example.kafkaconsumerservice.model.ParkingSpot;
import com.example.kafkaconsumerservice.service.ParkingService;
import com.example.kafkaconsumerservice.socket.ParkingSpotWebSocketHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    @Autowired
    private ParkingService parkingService;
    @Autowired
    private ParkingSpotWebSocketHandler parkingSpotWebSocketHandler;

    @KafkaListener(topics = "parking-sensor-topic", groupId = "parking-sensor-group")
    public void consume(String message) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ParkingSensor parkingSensor = objectMapper.readValue(message, ParkingSensor.class);

        System.out.println("Received parking sensor data: " + parkingSensor.toString());

        ParkingSpot parkingSpot = parkingService.getParkingSpotBySensorId(parkingSensor.getSensorId());

        System.out.println(parkingSpot.toString());
        if (parkingSpot.getIsOccupied() != parkingSensor.getIsOccupied() && parkingSensor.getIsOccupied()) {
            parkingService.startTimer(parkingSpot.getId(), parkingSensor.getIsOccupied());
        } else if (parkingSpot.getIsOccupied() != parkingSensor.getIsOccupied() && !parkingSensor.getIsOccupied()) {
            parkingService.stopParkingSession(parkingSpot.getId());
        }

        parkingSpotWebSocketHandler.sendParkingSpotUpdate(objectMapper.writeValueAsString(parkingSpot));
    }
}
