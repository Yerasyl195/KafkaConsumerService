package com.example.kafkaconsumerservice.service;


import com.example.kafkaconsumerservice.model.ParkingSpot;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface ParkingService {
    List<ParkingSpot> getAllParkingSpots();
    ParkingSpot addParkingSpot(ParkingSpot parkingSpot);
    ParkingSpot getParkingSpotBySensorId(String id);
    ParkingSpot getParkingSpotById(Long id);
    //ParkingSpot updateParkingSpotStatus(String id, boolean isOccupied);

    ParkingSpot startTimer(Long sensorId, boolean isOccupied);
    ParkingSpot startParkingSession(Long sensorId, Long userId, String currentCarNumber);
    ParkingSpot stopParkingSession(Long sensorId);
    //double calculateParkingFee(String id, LocalDateTime startTime, LocalDateTime endTime);
    List<ParkingSpot> getNearbyPossibleSpots(double latitude, double longitude, double radius);

    void deleteParkingSpot(Long id);

    void checkGrpcConnection(ParkingSpot parkingSpot);
}
