from confluent_kafka import Consumer

TOPIC = "input-topic"
BROKER = "localhost:9092"

def main():
    consumer_config = {
        "bootstrap.servers": BROKER,
        "group.id": "grupo-teste",
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([TOPIC])

    print("Aguardando mensagens...")

    try:
        while True:
            mensagem = consumer.poll(1.0)

            if mensagem is None:
                continue

            if mensagem.error():
                print(f"Erro: {mensagem.error()}")
                continue

            # Decodifica key e value (podem ser None)
            raw_key = mensagem.key()
            raw_value = mensagem.value()

            chave = raw_key.decode("utf-8") if raw_key else None
            valor = raw_value.decode("utf-8") if raw_value else None

            print("Mensagem recebida:")
            print(f"  Key:   {chave}")
            print(f"  Value: {valor}")
            print("-" * 40)

    except KeyboardInterrupt:
        print("Saindo...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
