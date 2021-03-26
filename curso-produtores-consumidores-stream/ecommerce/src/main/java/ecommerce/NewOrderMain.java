package ecommerce;

import java.sql.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
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
		/*
		 * É o objeto responsável pelo envio da mensagem ao kafka. 
		 * Os parâmetros de Tipagem representam o tipo da Chave e da Mensagem (nesse caso, ambos são do tipo String).
		 * O parâmetro no construtor representa as propriedades de configuração do producer.
		 * */
		var producer = new KafkaProducer<String, String>(properties());
		
		//Uma mensagem repressentando um pedido de compra.
		var value = "Teclado Gamer ReaDragon, 229.99";
		var key = "1";
		
		/*Mensagem(record) que será enviada ao kafka. É necessário informar o tópico, a key e o value.
		 * Como a mensagem trata de novas ordens de pedido, foi criado no kafka o tópico "ECOMMERCE_NEW_ORDER"
		 * */
		var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);

		/*
		 * O método Send, que fará o envio da mensagem ao kafka, é assincrono, não aguardando o resultado da postagem (sucesso ou falha). 
		 * O método get() permite aguardar o resultado do envio da mensagem, quando o resultado retornar será executada a função de callback.
		 * Na função de calback iremos apenas informar o sucesso ou falha de envio da mensagem.
		 * 
		 * para enviar a mensagem sem utilizar o callback: producer.send(record);
		*/
		
		Callback callback = (dadosSucesso, exceptionDeFalha) -> {
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
		};
		
		producer.send(record, callback).get();
		
		/*Novo producer para o envio de um email confirmando o pedido. O producer enviará uma mensagem para o tópico ECOMMERCE_SEND_EMAIL*/
//		var emailRecord =  new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", "meuemaul@fakemail.com", "Seu pedido será processado, em breve você receberá a confirmação");
//	
//		producer.send(emailRecord, callback).get();
	}

	/*
	 * Método que retorna as propriedades necessárias para a conexão e utilização do
	 * kafka por parte do produtor(producer)
	 */
	private static Properties properties() {
		var properties = new Properties();

		// propriedade de conexão com o apache kafka. Indica o servidor onde o kafka está sendo executado
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

		/*
		 * Propriedades que definem qual o Serializer utilizado no processo de serealização da chave(Key) e da Mensagem(value) que será enviada.
		 * Será utilizado StringSerializer para key e value pois foram definidos tipo string no kafkaProducer.
		 * 
		 */
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}
}
