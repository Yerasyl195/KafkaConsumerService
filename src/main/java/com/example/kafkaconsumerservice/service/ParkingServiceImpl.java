package com.example.kafkaconsumerservice.service;

import com.example.kafkaconsumerservice.ParkingSpotNotFoundException;
import com.example.kafkaconsumerservice.client.ParkingServiceClient;
import com.example.kafkaconsumerservice.grpc.ParkingSessionServiceImpl;
import com.example.kafkaconsumerservice.grpc.ParkingSpotOuterClass;
import com.example.kafkaconsumerservice.grpc.ParkingSpotServiceImpl;
import com.example.kafkaconsumerservice.model.ParkingSession;
import com.example.kafkaconsumerservice.respository.ParkingSpotRepository;
import com.example.kafkaconsumerservice.model.ParkingSpot;
import com.example.kafkaconsumerservice.violation.ParkingViolationService;
import kz.aparking.parkingsession.ParkingSessionOuterClass;
import kz.aparking.parkingsession.ParkingSessionServiceGrpc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class ParkingServiceImpl implements ParkingService {

    private final ParkingSpotRepository parkingSpotRepository;
    @Autowired
    private ParkingViolationService parkingViolationService;
    @Autowired
    private ParkingSessionServiceImpl parkingSessionService;
    @Autowired
    private ParkingSpotServiceImpl parkingSpotServiceImpl;

    @Autowired
    public ParkingServiceImpl(ParkingSpotRepository parkingSpotRepository) {
        this.parkingSpotRepository = parkingSpotRepository;
    }

    @Override
    public List<ParkingSpot> getAllParkingSpots() {
        return parkingSpotRepository.findAll();
    }

    @Override
    public ParkingSpot addParkingSpot(ParkingSpot parkingSpot) {
        return this.parkingSpotRepository.save(parkingSpot);
    }

    @Override
    public ParkingSpot getParkingSpotBySensorId(String id) {
        //return parkingSpotRepository.findById(Long.valueOf(id)).orElse(null);
        return parkingSpotRepository.findBySensorId(id);
    }

    @Override
    public ParkingSpot getParkingSpotById(Long id) {
        return parkingSpotRepository.findById(Long.valueOf(id)).orElse(null);
    }

    @Override
    public ParkingSpot startTimer(Long id, boolean isOccupied){
        ParkingSpot parkingSpot = parkingSpotRepository.findById(Long.valueOf(id)).orElse(null);
        if(isOccupied && parkingSpot.getCurrentUserId() == null){
            parkingSpot.setStartTime(LocalDateTime.now());
            parkingSpot.setOccupied(true);
        }
        return parkingSpotRepository.save(parkingSpot);
    }
    @Override
    public ParkingSpot startParkingSession(Long id, Long userId, String currentCarNumber) {
        ParkingSpot parkingSpot = parkingSpotRepository.findById(Long.valueOf(id)).orElse(null);
        if (parkingSpot != null && parkingSpot.getIsOccupied() == true && parkingSpot.getCurrentUserId() == null) {
            parkingSpot.setCurrentUserId(userId);
            parkingSpot.setCurrentCarNumber(currentCarNumber);
            return parkingSpotRepository.save(parkingSpot);
        }
        throw new IllegalArgumentException("Parking spot is not available");
    }
    @Override
    public ParkingSpot stopParkingSession(Long id) {
        ParkingSpot parkingSpot = parkingSpotRepository.findById(Long.valueOf(id)).orElse(null);
        if (parkingSpot != null && parkingSpot.getCurrentUserId() != null) {
            parkingSpot.setEndTime(LocalDateTime.now());
            ParkingServiceClient client = new ParkingServiceClient();
            client.addParkingHistory(parkingSpot);
            ParkingSession parkingSession = new ParkingSession(parkingSpot.getSpotNumber(),
                    parkingSpot.getStartTime(), parkingSpot.getEndTime(),
                    parkingSpot.getCurrentUserId(), parkingSpot.getCurrentCarNumber());
            ParkingSessionOuterClass.ParkingSession grpcParkingSession = convertToGrpcParkingSession(parkingSession);
            parkingSessionService.addParkingSession(parkingSpot.getCurrentUserId(), grpcParkingSession);
        }
        parkingSpot.resetParkingOccupancy();
        return parkingSpotRepository.save(parkingSpot);
    }
    private ParkingSessionOuterClass.ParkingSession convertToGrpcParkingSession(ParkingSession parkingSession) {
        // Преобразование java.time.LocalDateTime в строку формата ISO-8601
        DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
        String startTime = parkingSession.getStartTime() != null ? parkingSession.getStartTime().format(formatter) : "";
        String endTime = parkingSession.getEndTime() != null ? parkingSession.getEndTime().format(formatter) : "";

        // Здесь преобразуйте parkingSession в ParkingSessionProto.ParkingSession
        ParkingSessionOuterClass.ParkingSession grpcParkingSession = ParkingSessionOuterClass.ParkingSession.newBuilder()
                .setParkingSpotNumber(parkingSession.getParkingSpotNumber())
                .setStartTime(startTime)
                .setEndTime(endTime)
                .setCarNumber(parkingSession.getCarNumber())
                .setUserId(parkingSession.getUserId())
                .setPrice(parkingSession.getPrice())
                .setIsPaid(parkingSession.isPaid())
                .build();

        return grpcParkingSession;
    }

    @Override
    public List<ParkingSpot> getNearbyAvailableSpots(double latitude, double longitude, double radius) {
        List<ParkingSpot> allSpots = parkingSpotRepository.findAll();
        List<ParkingSpot> nearbyAvailableSpots = new ArrayList<>();

        for (ParkingSpot spot : allSpots) {
            if (!spot.getIsOccupied()) {
                double distance = calculateDistance(latitude, longitude, spot.getLatitude(), spot.getLongitude());
                if (distance <= radius) {
                    nearbyAvailableSpots.add(spot);
                }
            }
        }

        return nearbyAvailableSpots;
    }
    @Override
    public List<ParkingSpot> getNearbyPossibleSpots(double latitude, double longitude, double radius) {
        List<ParkingSpot> allSpots = parkingSpotRepository.findAll();
        List<ParkingSpot> nearbyPossibleSpots = new ArrayList<>();

        for (ParkingSpot spot : allSpots) {
            if (spot.getCurrentUserId() == null && isOccupiedForDuration(spot.getStartTime())) {
                double distance = calculateDistance(latitude, longitude, spot.getLatitude(), spot.getLongitude());
                if (distance <= radius) {
                    nearbyPossibleSpots.add(spot);
                }
            }
        }

        return nearbyPossibleSpots;
    }
    private boolean isOccupiedForDuration(LocalDateTime startTime) {
        LocalDateTime currentTime = LocalDateTime.now();
        LocalDateTime occupiedUntilTime = startTime.plusMinutes(10);
        return currentTime.isBefore(occupiedUntilTime);
    }

    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double earthRadius = 6371e3; // радиус Земли в метрах
        double lat1Radians = Math.toRadians(lat1);
        double lat2Radians = Math.toRadians(lat2);
        double deltaLat = Math.toRadians(lat2 - lat1);
        double deltaLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
                Math.cos(lat1Radians) * Math.cos(lat2Radians) *
                        Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return earthRadius * c;
    }

    @Override
    public void deleteParkingSpot(Long id) {
        ParkingSpot parkingSpot = parkingSpotRepository.findById(id)
                .orElseThrow(() -> new ParkingSpotNotFoundException("User not found with id: " + id));
        parkingSpotRepository.delete(parkingSpot);
    }

    @Scheduled(fixedDelay = 60 * 1000) // Каждую минуту
    public void checkForSpotViolations() { // проверка занятости
        List<ParkingSpot> parkingSpots = getAllParkingSpots();
        for (ParkingSpot spot : parkingSpots) {
            if (spot.getIsOccupied() && spot.getCurrentUserId() == null && spot.getStartTime().plusMinutes(15).isBefore(LocalDateTime.now())) {
                // Нарушение - место занято, но пользователь не зарегистрирован
//                LocalDateTime now = LocalDateTime.now();
//                Duration duration = Duration.between(spot.getStartTime(), now);
                //spot.setViolation(true);
                //тут сокет для регулировщика

//                parkingSpotRepository.save(spot);
//                if (duration.toMinutes() >= 15) {
//                    parkingViolationService.addViolation(spot);
//                }
//            } else if (!spot.getIsOccupied() && spot) {
//                // Проверяем, оплачено ли парковочное место
//                PaymentData paymentData = paymentService.getPaymentDataForUser(spot.getCurrentUserId());
//                if (paymentData != null && !paymentData.isPaid() && spot.getEndTime().plusMinutes(15).isBefore(LocalDateTime.now())) {
//                    // Нарушение - оплата не была произведена в течение 15 минут после окончания сессии
//                    parkingViolationService.addViolation(spot);
//                }
//              }
            }
        }
    }
    @Scheduled(cron = "0 0 0 * * *") // Каждые сутки
    public void checkForPaymentViolations() { // проверка оплаты
        ParkingServiceClient parkingServiceClient = new ParkingServiceClient();
        //parkingServiceClient.getUser();
        //getAllUser
        List<ParkingSpot> parkingSpots = getAllParkingSpots();

        for (ParkingSpot spot : parkingSpots) {
            if (spot.getIsOccupied() && spot.getCurrentUserId() != null && spot.getEndTime().plusMinutes(15).isBefore(LocalDateTime.now())) {
                // Нарушение - место занято, но пользователь не зарегистрирован
                parkingViolationService.addPaymentViolation(spot);
            }
        }
    }

    @Override
    public void checkGrpcConnection(ParkingSpot parkingSpot) {
        ParkingSpotOuterClass.ParkingSpot grpcParkingSpot = convertToGrpcParkingSpot(parkingSpot);
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
