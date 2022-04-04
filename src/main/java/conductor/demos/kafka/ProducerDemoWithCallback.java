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

public class ProducerDemoWithCallback {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

  public static void main(String[] args) {
    log.info("Hello World");

    // 1. create Producer Properties
    Properties properties = KafkaUtils.getKafkaProperties();

    // 2. create Producer
    for (int i = 0; i < 10; i++) {
      KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
          String.format("Hello %d, previous partition", i));

      // 3. send data
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes every time a record is successfully sent or an exception is thrown
          if (e == null) {
            log.info("Received new metadata\n" +
                    "Topic: {}\n" +
                    "Partition: {}\n" +
                    "Offset: {}\n" +
                    "Timestamp: {}\n"
                , recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                recordMetadata.timestamp());
          } else {
            log.error("Error while producing", e);
          }
        }
      });

      // 4. flush & close Producer
      producer.flush();
      producer.close();

      try{
        Thread.sleep(1000);
      } catch (InterruptedException e){
        e.printStackTrace();
      }
    }

  }

}
