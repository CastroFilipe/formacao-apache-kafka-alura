package ecommerce;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Representa um consumidor para o tópico ECOMMERCE_SEND_EMAIL.
 * 
 * */
public class EmailService {

	public static void main(String[] args) {
		//Instanciando o consumer com as propriedades declarada.
		var consumer = new KafkaConsumer<String, String>(properties());
		
		//Fará o consumer se inscrever no tópico ECOMMERCE_SEND_EMAIL
		consumer.subscribe(Collections.singletonList("ECOMMERCE_SEND_EMAIL"));
		
		
		while (true) {
			
			//Faz a leitura e verifica se não existem novas mensagens.
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.println("Encontrei " + records.count() + " registros");
				for (var record : records) {
					System.out.println("------------------------------------------");
					System.out.println("Enviando o email..");
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
					System.out.println("Email enviado com sucesso");
				}
			}
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getSimpleName());
		return properties;
	}
}
