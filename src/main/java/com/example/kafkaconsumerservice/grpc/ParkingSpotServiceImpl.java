package com.example.kafkaconsumerservice.grpc;

import io.grpc.stub.StreamObserver;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;


@Service
public class ParkingSpotServiceImpl extends ParkingSpotServiceGrpc.ParkingSpotServiceImplBase {
    private final ConcurrentLinkedQueue<StreamObserver<ParkingSpotOuterClass.ParkingSpot>> observers = new ConcurrentLinkedQueue<>();

    public void addParkingSpot(ParkingSpotOuterClass.ParkingSpot parkingSpot) {
        Iterator<StreamObserver<ParkingSpotOuterClass.ParkingSpot>> iterator = observers.iterator();
        while (iterator.hasNext()) {
            StreamObserver<ParkingSpotOuterClass.ParkingSpot> observer = iterator.next();
            try {
                observer.onNext(parkingSpot);
            } catch (Exception e) {
                // Если произошла ошибка при отправке данных, удаляем observer из списка
                iterator.remove();
            }
        }
    }

    @Override
    public void streamParkingSpots(ParkingSpotOuterClass.StreamRequest request, StreamObserver<ParkingSpotOuterClass.ParkingSpot> responseObserver) {
        // Добавить observer в список при подключении нового клиента
        observers.add(responseObserver);

        // Если серверу нужно отключить соединение с клиентом, он может вызвать responseObserver.onCompleted();
        // Если серверу нужно сообщить клиенту об ошибке, он может вызвать responseObserver.onError(t);
    }
}