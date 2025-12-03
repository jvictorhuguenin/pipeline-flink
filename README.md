# 📌 **Fraud Detection Streaming Pipeline (Apache Flink + Kafka + ONNX + Java 17)**

Este projeto implementa uma pipeline de detecção de fraude em tempo real utilizando:

- **Apache Kafka** — ingestão contínua de dados
- **Apache Flink 1.17+** — processamento em tempo real
- **ONNX Runtime** — inferência do modelo treinado
- **Java 17** — linguagem principal
- **MySQL** — persistência das predições

A pipeline consome registros JSON do Kafka, executa inferência diretamente no Flink usando um modelo ONNX exportado do Python, classifica a transação como fraude ou não fraude e grava o resultado em um banco MySQL.

---

# 📦 1. Pré-requisitos

### ✔ Java 17 (JDK 17)
### ✔ Maven 3.8+
### ✔ Kafka local (ou remoto) em execução
### ✔ MySQL local (opcional para testar o sink)
### ✔ Flink executado via MiniCluster (embutido no projeto)

---

# 🛠 2. Instalando o Java 17

## **Windows**

1. Baixe o JDK 17 (Temurin):  
   https://adoptium.net/temurin/releases/?version=17

2. Instale normalmente.

3. Configure o JAVA_HOME:

```powershell
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17*"
setx PATH "%PATH%;%JAVA_HOME%\bin"
```

4. Verifique:

```powershell
java -version
```

Saída esperada:

```
openjdk version "17.0.x"
```

---

# 🔧 3. Instalando o Maven

Baixe em:  
https://maven.apache.org/download.cgi

Descompacte e configure no PATH:

```powershell
setx MAVEN_HOME "C:\tools\apache-maven-3.9.6"
setx PATH "%PATH%;%MAVEN_HOME%\bin"
```

Verifique:

```powershell
mvn -version
```

---

# 📂 4. Estrutura do Projeto

```
/src
  /main
    /java/org/example/FraudPipeline.java
    /resources/model.onnx
pom.xml
README.md
```

O arquivo `model.onnx` contém o modelo treinado exportado do Python e carregado diretamente no Flink.

---

# 📥 5. Baixar dependências

Na raiz do projeto:

```bash
mvn clean install
```

Isso irá:

- Baixar o Apache Flink
- Baixar ONNX Runtime
- Baixar conectores Kafka
- Compilar todas as classes

---

# ⚙ 6. Compilando o projeto

```bash
mvn clean package -DskipTests
```

O JAR final será gerado em:

```
target/fraud-pipeline-1.0.jar
```

---

# ▶️ 7. Executando a pipeline

Abra os SSH tunnels necessarios com a senha '123456':
```bash
ssh -f -L 3306:localhost:3306 -p 2203 labalto@200.159.243.250 -N
ssh -f -L 9092:localhost:9092 -p 2203 labalto@200.159.243.250 -N
```

Como o projeto usa o **Flink MiniCluster**, basta rodar o jar:

```bash
java -jar target/fraud-pipeline-1.0.jar
```

Nenhum cluster Flink externo é necessário.

---

# 📡 8. Executando com parâmetros (opcional)

```bash
java -jar target/fraud-pipeline-1.0.jar \
  --kafka.bootstrap=localhost:9092 \
  --kafka.topic=input-topic \
  --mysql.url=jdbc:mysql://localhost:3306/pipeline
```

---

# 🧪 9. Testando a pipeline com Kafka

Certifique-se de que o Kafka está rodando e envie mensagens:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic input-topic
```

Envie um JSON no formato:

```json
{"V1":0.1, "V2":-0.22, "V3":1.5, ..., "Amount":149.62}
```

A pipeline irá:

1. Ler o JSON do Kafka
2. Converter os valores para vetor `float[]`
3. Rodar inferência dentro do Flink usando o modelo ONNX
4. Classificar: `"FRAUD"` ou `"OK"`
5. Persistir no MySQL

---

# 💾 10. Consultando o MySQL
Entre na seguinte maquina, com a senha '123456':
```bash
ssh -p 2203 labalto@200.159.243.250
```
Entre no container do mysql:

```bash
docker exec -it 70ee5a7fd94a mysql -uroot -p
```

Execute o seguinte comando para ver as entidades classificadas:
```sql
SELECT * FROM classified_entries ORDER BY id DESC;
```
---

# ⚙ 11. Arquitetura da pipeline

```
Kafka  →  Flink (ONNX Inference)  →  MySQL
```

### Flink executa:

- KafkaSource nativo
- Pré-processamento
- Inferência ONNX (`OrtSession`)
- Sink transacional para MySQL

Tudo rodando dentro do JVM, sem microserviços externos, garantindo:

- Baixa latência
- Alta vazão
- Menos pontos de falha
- Simplicidade operacional

---

