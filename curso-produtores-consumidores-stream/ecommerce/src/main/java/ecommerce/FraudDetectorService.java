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
			
			//Faz a leitura e verifica se não existem novas mensagens a cada 100 milesegundos.
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.print("\n########## " + FraudDetectorService.class.getSimpleName());
				System.out.println(": recebidos " + records.count() + " registros\n");
				
				for (var record : records) {
					
					System.out.println("Processing new order, checking for fraud...\n");
					System.out.println("key: " + record.key());
					System.out.println("value: " + record.value());
					System.out.println("partition: " + record.partition());
					System.out.println("offset: " + record.offset());
					System.out.println(record.headers());
					System.out.println("timestamp: " + record.timestamp());
					
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// ignoring
						e.printStackTrace();
					}
					System.out.println("\nOrder processed");
					System.out.println("------------------------------------------");
				}
			}
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		
		/*
		 * Propriedades que definem qual o deserealizador para a key e value(mensagem), 
		 * O deserealizador fará a conversão das mensagens consumidas, transformando de bytes para string.
		 * */
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		/*
		 * Define o grupo ao qual o consumer fará parte. Para trabalhar com Publish and Subscribe, cada consumer deve possuir 
		 * seu próprio grupo. Assim todos os consumers receberão as mensagens. Por isso o uso do nome da classe como Group Id, 
		 * pois esse nome será único.
		 * */
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
		return properties;
	}
}
