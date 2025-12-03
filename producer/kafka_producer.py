from kafka import KafkaProducer
from sklearn.preprocessing import RobustScaler
import hashlib
import pandas as pd
import time

def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: str(v).encode('utf-8'),
        key_serializer=lambda k: k.encode("utf-8")
    )

    csv_path = "../creditcard.csv"
    df = pd.read_csv(csv_path)

    rob_scaler = RobustScaler()

    df['scaled_amount'] = rob_scaler.fit_transform(df['Amount'].values.reshape(-1, 1))
    df['scaled_time'] = rob_scaler.fit_transform(df['Time'].values.reshape(-1, 1))

    df.drop(['Time', 'Amount'], axis=1, inplace=True)
    scaled_amount = df['scaled_amount']
    scaled_time = df['scaled_time']

    df.drop(['scaled_amount', 'scaled_time'], axis=1, inplace=True)
    df.insert(0, 'scaled_amount', scaled_amount)
    df.insert(1, 'scaled_time', scaled_time)

    df.drop('Class', inplace=True, axis=1)

    for _, row in df.iterrows():
        payload = row.to_json()

        # Gera hash da mensagem para usar como key
        key_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        producer.send(
            "input-topic",
            key=key_hash,
            value=payload
        )
        producer.flush()
        time.sleep(0.1)

    print("Envio finalizado.")

if __name__ == "__main__":
    main()