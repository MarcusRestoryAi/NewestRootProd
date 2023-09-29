package org.example;

import marcus.kafka.payload.*;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws MalformedURLException, org.json.simple.parser.ParseException {
        System.out.println("Hello world!");

        userMenu();

        /*

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fetchingGroup");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");


        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("your-topic"));
        */



        //Hämta data från topic
        //getDataFromKafka("javaJsonGuides");

        //System.out.println("Project done!");
    }

    public static void userMenu() {
        String userChoise = "";

        do {

            printMenu();

            Scanner scan = new Scanner(System.in);
            System.out.print("Gör ditt val: ");
            userChoise = scan.nextLine();

            //SwitchCase
            switch (userChoise) {
                case "1": {
                    userInputForKafka();
                    break;
                }
                case "2": {
                    getDataFromKafka("javaJsonGuides");
                    break;
                }
                case "0": {
                    break;
                }
                default: {
                    System.out.println("Felaktig input. Försök igen");
                    break;
                }
            }

            if (!userChoise.equals("0")) {
                System.out.println("Press any key to continue");
                scan.nextLine();
            }

        } while (!userChoise.equals("0"));
    }

    public static void printMenu() {
        /* Writing up the meny */
        System.out.println("Gör dit val!");
        System.out.println("------------");
        System.out.println("1. Skriv data till Kafka Server");
        System.out.println("2. Hämta data från Kafka Server");
        System.out.println("0. Avsluta");
    }

    public static void userInputForKafka() {
        User user = new User();

        /*Logik flr att låta användaren mata in data*/

        user.setId(11L);
        user.setFirstName("Cihan");
        user.setLastName("Vivera");

        JSONObject myObj = new JSONObject();
        myObj.put("id", user.getId());
        myObj.put("firstName", user.getFirstName());
        myObj.put("lastName", user.getLastName());

        // URL url = new URL("http://localhost:8080/api/v1/kafka/publish");

        //Skicka Payload till WebAPI via en Request
        sendToWebAPI(myObj);
    }

    public static void sendToWebAPI(JSONObject myObj) {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost("http://localhost:8080/api/v1/kafka/publish");

            // Skapa en JSON-förfrågningskropp
            String jsonPayload = myObj.toJSONString();
            StringEntity entity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON);
            httpPost.setEntity(entity);

            // Skicka förfrågan och hantera svaret
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                HttpEntity responseEntity = response.getEntity();
                if (responseEntity != null) {
                    String responseString = EntityUtils.toString(responseEntity);
                    System.out.println("Svar från server: " + responseString);
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    public static void getDataFromKafka(String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fetchingGroup");
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put("spring.json.trusted.packages", "*");

        Consumer<String, User> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Collections.singletonList(topicName));
        consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));

        //Gå till början av Topic
        consumer.seekToBeginning(consumer.assignment());

        //Create User list
        ArrayList<User> users = new ArrayList<User>();

        //WhileLoop osm hämtar i JSON format
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) continue;
            for (ConsumerRecord<String, User> record : records) {
                users.add(record.value());
                //System.out.println(record.value());
                //System.out.println(record.value().getClass().toString());
/*
                //Spara datan tillbaka till ett JSONObject
                JSONObject fetchData = (JSONObject) new JSONParser().parse(record.value());

                //Skriva ut data
                System.out.println(fetchData.get("id"));
                System.out.println(fetchData.get("firstName"));
                System.out.println(fetchData.get("lastName"));

*/
            }
            break;
        }

        for (User user : users) {
            System.out.println(user.getFirstName());
        }
/*
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());

                //Spara datan tillbaka till ett JSONObject
                JSONObject fetchData = (JSONObject) new JSONParser().parse(record.value());

                //Skriva ut data
                System.out.println(fetchData.get("id"));
                System.out.println(fetchData.get("firstName"));
                System.out.println(fetchData.get("lastName"));


            }
        }
*/
    }
}