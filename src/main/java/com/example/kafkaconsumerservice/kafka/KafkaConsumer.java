package com.example.kafkaconsumerservice.kafka;


import com.example.kafkaconsumerservice.grpc.ParkingSpotServiceImpl;
import com.example.kafkaconsumerservice.grpc.ParkingSpotOuterClass;
import com.example.kafkaconsumerservice.model.ParkingSensor;
import com.example.kafkaconsumerservice.model.ParkingSpot;
import com.example.kafkaconsumerservice.service.ParkingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.format.DateTimeFormatter;

@Service
public class KafkaConsumer {
    @Autowired
    private ParkingService parkingService;
    @Autowired
    private ParkingSpotServiceImpl parkingSpotServiceImpl;
    @KafkaListener(topics = "parking-sensor-topic", groupId = "parking-sensor-group")
    public void consume(String message) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ParkingSensor parkingSensor = objectMapper.readValue(message, ParkingSensor.class);

        System.out.println("Received parking sensor data: " + parkingSensor.toString());

        //parkingSpotWebSocketHandler.sendParkingSpotUpdate(message);
        ParkingSpot parkingSpot = parkingService.getParkingSpotBySensorId(parkingSensor.getSensorId());
        // Обновляем состояние парковочного места
        System.out.println(parkingSpot.toString());
        if(parkingSpot.getIsOccupied() != parkingSensor.getIsOccupied() && parkingSensor.getIsOccupied() == true){
            // Сохраняем обновленное состояние парковочного места
            parkingService.startTimer(parkingSpot.getId(), parkingSensor.getIsOccupied());
        } else if (parkingSpot.getIsOccupied() != parkingSensor.getIsOccupied() && parkingSensor.getIsOccupied() == false) {
            parkingService.stopParkingSession(parkingSpot.getId());
        }
        //System.out.println("Data from postgre: "+ parkingSpot);

        ParkingSpot updatedParkingSpot = parkingService.getParkingSpotBySensorId(parkingSensor.getSensorId());
        ParkingSpotOuterClass.ParkingSpot grpcParkingSpot = convertToGrpcParkingSpot(updatedParkingSpot);
        parkingSpotServiceImpl.addParkingSpot(grpcParkingSpot);
    }
    private ParkingSpotOuterClass.ParkingSpot convertToGrpcParkingSpot(ParkingSpot parkingSpot) {
        // Преобразование java.time.LocalDateTime в строку формата ISO-8601
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        String startTime = parkingSpot.getStartTime() != null ? parkingSpot.getStartTime().format(formatter) : "";
        String endTime = parkingSpot.getEndTime() != null ? parkingSpot.getEndTime().format(formatter) : "";
        Long currentUserId = parkingSpot.getCurrentUserId() != null ? parkingSpot.getCurrentUserId() : -1L;
        String currentCarNumber = parkingSpot.getCurrentCarNumber() != null ? parkingSpot.getCurrentCarNumber() : "";

        // Здесь преобразуйте parkingSpot в ParkingSpotOuterClass.ParkingSpot
        ParkingSpotOuterClass.ParkingSpot grpcParkingSpot = ParkingSpotOuterClass.ParkingSpot.newBuilder()
                .setId(parkingSpot.getId())
                .setSensorId(parkingSpot.getSensorId())
                .setIsOccupied(parkingSpot.getIsOccupied())
                .setSpotNumber(parkingSpot.getSpotNumber())
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setLatitude(parkingSpot.getLatitude())
                .setLongitude(parkingSpot.getLongitude())
                .setCurrentUserId(currentUserId)
                .setCurrentCarNumber(currentCarNumber)
                .build();

        return grpcParkingSpot;
    }
}

