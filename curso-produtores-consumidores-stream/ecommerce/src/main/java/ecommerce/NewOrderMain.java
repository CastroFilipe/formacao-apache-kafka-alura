package ecommerce;

import java.sql.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Representa um novo pedido de compra.
 * 
 */
public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());
		
		//O valor da mensagem que será enviada(id do usuário, id do produto, valor)
		var value = "243,93456,39.99";
		
		/*Mensagem que será registrada(record). É necessário informar o tópico, a chave e o valor.
		 * Como essa classse trata de novas ordens de pedido, foi criado no kafka o tópico "ECOMMERCE_NEW_ORDER"
		 * */
		var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);

		/*
		 * Enviando o registro para o kafka. 
		 * O método Send é assincrono, o .get() fará com que ele espere a conclusão do envio da mensagem e execute a função de callback.
		 * Na função de calback iremos apenas informar o sucesso do envio da mensagem.
		 * 
		 * para enviar a mensagem sem utilizar o callback: producer.send(record);
		*/
		producer.send(record, (dadosSucesso, exceptionDeFalha) -> {
			if(exceptionDeFalha != null) {
				System.out.println("Erro no envio..");
				exceptionDeFalha.printStackTrace();
			} else {
				System.out.println("Sucesso no envio..");
				System.out.println(
					"Tópico: " + dadosSucesso.topic() 
					+ " Partição: "+ dadosSucesso.partition() 
					+ " offset: " + dadosSucesso.offset() + " timestamp: " +dadosSucesso.timestamp() 
					+ " date: "+ new Date(dadosSucesso.timestamp()));
			}
		}).get();
	}

	/*
	 * Método que retorna as propriedades necessárias para a conexão e utilização do
	 * kafka por parte do produtor(producer)
	 */
	private static Properties properties() {
		var properties = new Properties();

		// propriedade de conexão com o apache kafka
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

		/*
		 * Propriedades necessário para serializar as Strings que serão utilizadas para o envio da mensagem. 
		 * A utilização de Strings foi definido em new KafkaProducer<String, String>
		 */
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}
}
