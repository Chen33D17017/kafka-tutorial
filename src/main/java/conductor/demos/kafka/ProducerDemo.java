package conductor.demos.kafka;


import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

  public static void main(String[] args) {
    log.info("Hello World");

    // 1. create Producer Properties



    // 2. create Producer
    Properties properties = KafkaUtils.getKafkaProperties();
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World");

    // 3. send data
    producer.send(producerRecord);
    
    // 4. flush & close Producer
    producer.flush();
    producer.close();
  }

}
