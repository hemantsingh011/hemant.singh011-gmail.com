package com.sample.logstash_client;

import java.util.concurrent.CompletableFuture;

public class Main {
    private static int counter =0;
    public static void main(String[] args) {
        LogstashClient logstashClient = new LogstashClient();
        CompletableFuture<Boolean> connect = logstashClient.connect();
        connect.thenAccept(result -> {
            System.out.println("connected " + result);
            if (result) {
                while(counter<5000){
                    String json = "{\"brand\":\"Jeep\", \"doors\": "+counter+"}";
                    counter++;
                    logstashClient.sendMessage(json);
                }
                System.out.println("Done");

            }
        });
    }
}
