package com.aliyun.openservice.logservice.samples;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ProducerSample {

    private static List<String> hostnames = readFileByLine("hostname.sample");
    private static List<String> user_agents = readFileByLine("useragents.sample");
    private static List<String> ip_addresses = readFileByLine("ip_address");

    public static List<String> readFileByLine(String strFile) {
        try {
            String path = "/Users/kel/Github/kafka-connect-logservice/src/test/resources/" + strFile;
            File file = new File(path);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String strLine = null;
            List<String> data = new ArrayList<String>();
            while (null != (strLine = bufferedReader.readLine())) {
                data.add(strLine);
            }
            return data;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ArrayList<String>();
    }

    private static int randomNum(int max) {
        return (new Random()).nextInt(max);
    }

    private static String getMethod() {
        List<String> methods = Arrays.asList(
                "GET", "POST", "PUT", "DELETE"
        );
        int x = randomNum(methods.size());
        return methods.get(x);
    }


    public static String generateLog() {
        String method = getMethod();
        double request_time = (randomNum(1000) + 1) / 200.0;
        double upstream_response_time = request_time;
        String uri = "/";
        String request_uri = String.format("/openapi/v1/users/%s", "" + randomNum(1000));
        int body_bytes_sent = randomNum(1000);
        if (method.equalsIgnoreCase("GET")) {
            body_bytes_sent = 0;
        }
        int request_length = randomNum(300);
        List<Integer> statuses = Arrays.asList(200, 400, 401, 403, 500, 502);
        int status = 200;
        if (randomNum(1000) > 800) {
            status = statuses.get(randomNum(statuses.size()));
        }
        String http_referer = "-";
        if (randomNum(1000) > 750) {
            http_referer = hostnames.get(randomNum(hostnames.size()));
        }
        String user_agent = user_agents.get(randomNum(user_agents.size()));

        String remote_addr = ip_addresses.get(randomNum(ip_addresses.size()));
        String http_x_forwarded_for = ip_addresses.get(randomNum(ip_addresses.size()));
        String host = hostnames.get(randomNum(hostnames.size()));
        String remote_user = "-";
        DateFormat df7 = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM);
        String time_local = df7.format(new Date());

        JSONObject object = new JSONObject();
        object.put("remote_addr", remote_addr);
        object.put("remote_user", remote_user);
        object.put("time_local", time_local);
        object.put("request_method", method);
        object.put("request_uri", request_uri);
        object.put("http_host", host);
        object.put("request", "-");
        object.put("status", status);
        object.put("request_length", request_length);
        object.put("body_bytes_sent", body_bytes_sent);
        object.put("http_referer", http_referer);
        object.put("http_user_agent", user_agent);
        object.put("request_time", request_time);
        object.put("upstream_response_time", upstream_response_time);
        object.put("http_x_forwarded_for", http_x_forwarded_for);
        return object.toJSONString();
    }

    public static void main(String[] args) throws Exception {
        String topicName = "mytopic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100000; i++) {
            for (int j = 0; j < randomNum(100); j++) {
                String r = generateLog();
                producer.send(new ProducerRecord<String, String>(topicName, r));
            }
            Thread.sleep(randomNum(1000));
        }
        producer.close();
    }
}