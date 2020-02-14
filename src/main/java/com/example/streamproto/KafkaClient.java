package com.example.streamproto;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaClient {

	//private static final String KAFKA_TOPIC = "topsitesgeo4";

	private static boolean doSendToKafka = true;
	private static boolean withgeo = true;
	private static LatLon[] geos;

	private static String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
	private static String domainsFile = System.getenv("DOMAINS_FILE_NAME");
	private static String countsFile = System.getenv("COUNTS_FILE_NAME");
	private static String kafkaTopic = System.getenv("KAFKA_TOPIC");
	private static String trustStore = System.getenv("TRUST_STORE");
	private static String trustStorePassword = System.getenv("TRUST_STORE_PASSWORD");
	
	static {
		geos = new LatLon[4];
		geos[0] = new LatLon(14.5712961, 121.0478008);
		geos[1] = new LatLon(10.378734, 123.7058629);
		geos[2] = new LatLon(7.2532785, 125.170182);
		geos[3] = new LatLon(6.1374348, 125.017084);
	}

	public static Integer[] loadCounts(String filename) {

		List<Integer> probabilities = new ArrayList<Integer>();

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			String line = null;
			while (null != (line = reader.readLine())) {
				Integer prob = Integer.parseInt(line);
				probabilities.add(prob);
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {

			try {
				if (null != reader)
					reader.close();
			} catch (Exception ex) {
				// TODO: handle exception
				ex.printStackTrace();
			}
		}

		return probabilities.toArray(new Integer[0]);

	}

	public static List<Record> loadJson(String filename) throws Exception {
		BufferedReader reader = null;
		List<Record> jsons = new ArrayList<Record>();

		try {
			reader = new BufferedReader(new FileReader(filename));
			String line = null;
			while (null != (line = reader.readLine())) {
				System.out.println(line);
				StringTokenizer st = new StringTokenizer(line, ",");
				Record r = new Record();
				while (st.hasMoreTokens()) {
					r.rank = Integer.parseInt(st.nextToken());
					r.domain = st.nextToken();
					r.category = st.nextToken();
					jsons.add(r);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				if (null != reader) {
					reader.close();
				}
			} catch (Exception e2) {
				throw e2;
			}
		}

		return jsons;
	}

	public static LatLon generateGeolocation(Random random) {
		double r1 = random.nextGaussian() * 0.01;
		double s1 = random.nextDouble();
		double s2 = random.nextDouble();
		int pick = random.nextInt(geos.length);
		// pick a latlon
		LatLon latlng = geos[pick];
		if (s1 > 0.5) {
			if (s2 > 0.5) {
				latlng.lat += r1;
				latlng.lng += r1;
			} else {
				latlng.lat += r1;
				latlng.lng -= r1;
			}
		} else {
			if (s2 > 0.5) {
				latlng.lat -= r1;
				latlng.lng += r1;
			} else {
				latlng.lat -= r1;
				latlng.lng -= r1;
			}
		}

		latlng.lat = Math.round(latlng.lat * 100.0) / 100.0;
		latlng.lng = Math.round(latlng.lng * 100.0) / 100.0;
		return latlng;
	}

	public static void main(String[] args) throws Exception {

		System.out.println("Starting KafkaClient");
		
		Properties props = new Properties();
		// props.put("bootstrap.servers", "10.1.2.2:9092");
		props.put("bootstrap.servers", bootstrapServers);
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		if (null != trustStore) {
			props.put("ssl.truststore.location", trustStore);
		}

		if (null != trustStorePassword) {
			props.put("ssl.truststore.password", trustStorePassword);
		}
		Producer<String, String> producer = new KafkaProducer<>(props);

		// String domainsFile = "/mnt/top500Domains.csv";
		// String countsFile = "/mnt/counts.csv";
		// read probabilities file
		Integer[] counts = loadCounts(countsFile);

		List<Record> objs = loadJson(domainsFile);
		System.out.println(objs.get(0).domain);

		int sum = 0;
		for (int i = 0; i < counts.length; i++) {
			sum += counts[i];
		}

		int[] longArray = new int[sum];
		int offset = 0;
		for (int i = 0; i < counts.length; i++) {
			int c = counts[i];

			for (int j = 0; j < c; j++) {
				System.out.println("Setting value " + (offset + j) + " to " + i);
				longArray[offset + j] = i;

			}
			offset = offset + c;
			System.out.println("c=" + c + ",i=" + i);

		}

		Random random = new Random();
		SimpleDateFormat sfmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		long d = new Date().getTime();
		for (int i = 0; i < 1000000; i++) {
			int random_number = random.nextInt(longArray.length);

			int index = longArray[random_number];
			Record r = objs.get(index);
			d += 1000;
			// System.out.println(d);
			r.date = new Date(d);
			// System.out.println(r.date);
			ObjectMapper mapper = new ObjectMapper();

			/*
			 * JSONObject obj = new JSONObject(); obj.put("domain", r.domain);
			 * obj.put("category", r.category); obj.put("date", sfmt.format(r.date));
			 */
			
			r.formattedDate = sfmt.format(r.date);

			if (withgeo) {
				LatLon latLng = generateGeolocation(random);
				// obj.put("lat", latLng.lat);
				// obj.put("lng", latLng.lng);
				r.lat = latLng.lat;
				r.lng = latLng.lng;
			}

			// System.out.println(obj.toString());
			// String json = obj.toString();
			String json = mapper.writeValueAsString(r);
			System.out.println("> " + json);

			if (doSendToKafka) {
				sendToKafka(producer, json);
			}

			System.out.println("Sleeping...");
			Thread.sleep(1000);

		}
	}

	private static void sendToKafka(Producer<String, String> producer, String json) {
		producer.send(new ProducerRecord<String, String>(kafkaTopic, "key", json), new Callback() {

			@Override
			public void onCompletion(RecordMetadata m, Exception e) {
				if (e != null) {
					e.printStackTrace();
				} else {
					System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(),
							m.partition(), m.offset());
				}

			}
		});
	}
}

class Record {
	String domain;
	int rank;
	String category;
	Date date;
	String formattedDate;
	
	double lat;
	double lng;
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public int getRank() {
		return rank;
	}
	public void setRank(int rank) {
		this.rank = rank;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public Date getDate() {
		return date;
	}
	public void setDate(Date date) {
		this.date = date;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public double getLng() {
		return lng;
	}
	public void setLng(double lng) {
		this.lng = lng;
	}
	public String getFormattedDate() {
		return formattedDate;
	}
	public void setFormattedDate(String formattedDate) {
		this.formattedDate = formattedDate;
	}
	
	
}

class LatLon {
	double lat;
	double lng;

	public LatLon(double lat, double lng) {
		this.lat = lat;
		this.lng = lng;
	}
}
