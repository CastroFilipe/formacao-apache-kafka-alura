package ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Representa um consumidor para o tópico ECOMMERCE_NEW_ORDER.
 * A cada mensagem recebida representa uma nova ordem de compra, essa classe simula o recebimento e 
 * processamento de um sistema anti-fraudes.
 * 
 * */
public class FraudDetectorService {

	public static void main(String[] args) {
		//Instanciando o consumer com as propriedades declarada.
		var consumer = new KafkaConsumer<String, String>(properties());
		
		//Fará o consumer se inscrever no tópico ECOMMERCE_NEW_ORDER para a leitura de mensagens que serão enviadas pelos producers.
		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
		
		
		while (true) {
			
			//Faz a leitura e verifica se não existem novas mensagens.
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " registros");
				for (var record : records) {
					System.out.println("------------------------------------------");
					System.out.println("Processing new order, checking for fraud");
					System.out.println(record.key());
					System.out.println(record.value());
					System.out.println(record.partition());
					System.out.println(record.offset());
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// ignoring
						e.printStackTrace();
					}
					System.out.println("Order processed");
				}
			}
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
		return properties;
	}
}
