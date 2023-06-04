package com.example.kafkaconsumerservice.client;

import com.example.kafkaconsumerservice.model.ParkingSpot;
import com.example.kafkaconsumerservice.model.User;
import okhttp3.*;

import java.io.IOException;
import java.time.LocalDateTime;

public class ParkingServiceClient {
    private static final String BASE_URL = "http://209.38.249.233:8080";

    private final OkHttpClient httpClient;

    public ParkingServiceClient() {
        this.httpClient = new OkHttpClient();
    }

    public void getUser(String userId) {
        String url = BASE_URL + "/users/" + userId;
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call call = httpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected code " + response);
                }
                String responseBody = response.body().string();
                // здесь парсим responseBody в объект User
                //User user = new Gson().fromJson(responseBody, User.class);
                System.out.println(responseBody);
            }
        });
    }

    public void getAllUser(String userId) {
        String url = BASE_URL + "/users/" + userId;
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call call = httpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected code " + response);
                }
                String responseBody = response.body().string();
                // здесь парсим responseBody в объект User
                //User user = new Gson().fromJson(responseBody, User.class);
                System.out.println(responseBody);
            }
        });
    }

    public void addParkingHistory(ParkingSpot parkingSpot) {
        String url = BASE_URL + "/users/add/history/" + parkingSpot.getCurrentUserId();
        double price = (parkingSpot.getEndTime().getMinute() - parkingSpot.getStartTime().getMinute())*100/60;

        if(parkingSpot.getCurrentUserId() == null){
            System.out.println("Couldn't fix user");
            return;
        }

        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create(mediaType, "{"
                + "\"parkingSpotNumber\": \"" + parkingSpot.getSpotNumber() + "\","
                + "\"startTime\": \"" + parkingSpot.getStartTime() + "\","
                + "\"endTime\": \"" + parkingSpot.getEndTime() + "\","
                + "\"price\": \"" + price + "\","
                + "\"carNumber\": \"" + parkingSpot.getCurrentCarNumber() + "\","
                + "\"paid\": false"
                + "}");

        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .addHeader("Content-Type", "application/json")
                .build();
        Call call = httpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected code " + response);
                }
                String responseBody = response.body().string();
                System.out.println(responseBody);
            }
        });
    }
    public void getAllUserHistoryForLast24Hours() {
        String url = BASE_URL + "/users/history/all/";
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call call = httpClient.newCall(request);
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                e.printStackTrace();
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                if (!response.isSuccessful()) {
                    throw new IOException("Unexpected code " + response);
                }
                String responseBody = response.body().string();
                // здесь парсим responseBody в объект User
                //User user = new Gson().fromJson(responseBody, User.class);
                System.out.println(responseBody);
            }
        });
    }
}
