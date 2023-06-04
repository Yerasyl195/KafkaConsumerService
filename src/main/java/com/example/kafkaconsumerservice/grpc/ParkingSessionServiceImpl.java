package com.example.kafkaconsumerservice.grpc;

import io.grpc.stub.StreamObserver;
import kz.aparking.parkingsession.ParkingSessionOuterClass;
import kz.aparking.parkingsession.ParkingSessionServiceGrpc;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Service
public class ParkingSessionServiceImpl extends ParkingSessionServiceGrpc.ParkingSessionServiceImplBase {
    private final ConcurrentHashMap<Long, StreamObserver<ParkingSessionOuterClass.ParkingSession>> observers = new ConcurrentHashMap<>();
    public void addParkingSession(Long userId, ParkingSessionOuterClass.ParkingSession parkingSession) {
        StreamObserver<ParkingSessionOuterClass.ParkingSession> observer = observers.get(userId);
        if(observer != null) {
            try {
                observer.onNext(parkingSession);
            } catch (Exception e) {
                observers.remove(userId);
            }
        }
    }
    @Override
    public void streamParkingSessions(ParkingSessionOuterClass.StreamRequest request, StreamObserver<ParkingSessionOuterClass.ParkingSession> responseObserver) {
        Long userId = request.getUserId();
        observers.put(userId, responseObserver);
    }
}

