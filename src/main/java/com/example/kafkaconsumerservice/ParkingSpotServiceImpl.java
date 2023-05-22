package com.example.kafkaconsumerservice;

import com.example.kafkaconsumerservice.grpc.ParkingSpotOuterClass;
import com.example.kafkaconsumerservice.grpc.ParkingSpotServiceGrpc;

import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class ParkingSpotServiceImpl extends ParkingSpotServiceGrpc.ParkingSpotServiceImplBase {
    private final BlockingQueue<ParkingSpotOuterClass.ParkingSpot> parkingSpotQueue = new LinkedBlockingQueue<>();

    public void addParkingSpot(ParkingSpotOuterClass.ParkingSpot parkingSpot) {
        parkingSpotQueue.add(parkingSpot);
    }

    @Override
    public void streamParkingSpots(ParkingSpotOuterClass.StreamRequest request, StreamObserver<ParkingSpotOuterClass.ParkingSpot> responseObserver) {
        while (true) {
            try {
                ParkingSpotOuterClass.ParkingSpot parkingSpot = parkingSpotQueue.take(); // Блокируется, пока не появится новый элемент.
                responseObserver.onNext(parkingSpot);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                responseObserver.onError(e);
                break;
            }
        }
    }
}