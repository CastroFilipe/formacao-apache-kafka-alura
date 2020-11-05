# Curso Kafka: Produtores, Consumidores e Streams
[Acessar primeiro curso da formação Kafka](https://www.alura.com.br/curso-online-kafka-introducao-a-streams-em-microservicos).  

## Instruções 
1- Instalar o kafka(extrair o arquivo bin na raiz do disco C:)  

2- Usando windows: No terminal acessar o diretório raiz do kafka e Rodar o zookeeper com o comando: bin\windows\zookeeper-server-start.bat config\zookeeper.properties  

3- Rodar o kafka, em outro terminal, com o comando: bin\windows\kafka-server-start.bat config\server.properties  

4- Em outro terminal, criar um tópico com o comando: bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ECOMMERCE_NEW_ORDER  

5- Para listar os tópicos existentes: bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092