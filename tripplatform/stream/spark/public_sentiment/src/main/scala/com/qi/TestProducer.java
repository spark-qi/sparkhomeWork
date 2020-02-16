package com.qi;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {

	private static Properties properties = new Properties();
	private static KafkaProducer<String, String> producer;
	private static Random random=new Random();
	private static String[] strArr=new String[]{"cpu","主板","网络","磁盘"};
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	static {
		properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");

		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(properties);

	}

	public static void sendMsg(String key, String value) {

		ProducerRecord<String, String> record = new ProducerRecord<>("test3", key, value);

		producer.send(record);
	}

	public static void close() {
		if (producer != null) {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String[] args) {
			while (true){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				String msg=strArr[random.nextInt(strArr.length)];
				sendMsg("key " + random.nextInt(100), sdf.format(new Date())+"|"+msg+"|"+random.nextInt(100));

			}
	}
}
