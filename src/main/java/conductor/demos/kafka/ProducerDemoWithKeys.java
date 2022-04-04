package conductor.demos.kafka;


import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
  private static final String TOPIC = "demo_java";
  private static final String value = "Hello Keys %d";
  private static final String key = "id_%d";

  public static void main(String[] args) {
    log.info("Hello Kafka, let's test for key");

    // 1. create Producer Properties

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // 2. create Producer
    for (int i = 0; i < 10; i++) {

      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
          String.format(key, i % 4),
          String.format("Hello %d, previous partition", i));

      // 3. send data
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes every time a record is successfully sent or an exception is thrown
          if (e == null) {
            log.info("\n==========\n" +
                    "Topic: {}\n" +
                    "Key: {}\n" +
                    "Partition: {}\n" +
                    "Offset: {}\n"
                , recordMetadata.topic(),producerRecord.key(), recordMetadata.partition(), recordMetadata.offset());
          } else {
            log.error("Error while producing", e);
          }
        }
      });

      // 4. flush & close Producer
      producer.flush();
      producer.close();
    }

  }

}
