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
		
		//Será um consumidor de qualquer tópico que comece com ECOMMERCE
		consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
		
		while (true) {
			
			//Faz a leitura e verifica se não existem novas mensagens.
			var records = consumer.poll(Duration.ofMillis(100));
			
			if (!records.isEmpty()) {
				System.out.println("Mensagem recebida. Iniciando gravação do log...");
				System.out.println("Log - " + records.count() + " registros");
				for (var record : records) {
					System.out.println("Log Salvo..");
					System.out.println(record.key());
					System.out.println(record.value());
					System.out.println(record.partition());
					System.out.println(record.offset());
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
