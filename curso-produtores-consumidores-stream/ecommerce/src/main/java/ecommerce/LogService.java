package ecommerce;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Representa um consumidor para qualquer tópico do ECOMMERCE.
 * Será responsável por gerar o log da aplicação.
 * 
 * */
public class LogService {

	public static void main(String[] args) {
		//Instanciando o consumer com as propriedades declarada.
		var consumer = new KafkaConsumer<String, String>(properties());
		
		/*
		 * O Log consumirá qualquer mensagem postada postadas em tópicos que 
		 * deem match com a expressão regular ECOMMERCE.*
		 * */
		consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
		
		while (true) {
			
			//Faz a leitura e verifica se não existem novas mensagens a cada 100 milesegundos.
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.println("\nMensagem(s) recebida. Iniciando gravação do log...\n");
				System.out.println("Log - Quantidade de registros recebidos: " + records.count() + "\n");
				
				for (var record : records) {
					System.out.println("### Log Salvo");
					
					System.out.println("topic: " + record.topic());
					System.out.println("Key: " + record.key());
					System.out.println("value: "+ record.value());
					System.out.println("partição: " + record.partition());
					System.out.println("offset: " + record.offset());
					System.out.println(record.headers().toString());
					System.out.println("timestamp: " + record.timestamp());
					System.out.println("------------------------------------------");
				}
			}
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
		return properties;
	}
}
