from kafka import KafkaConsumer

c = KafkaConsumer(
    bootstrap_servers="localhost:9051",
    group_id="0",
    auto_offset_reset="earliest"
)

c.subscribe(["sf.crime.report"])

while True:
    message = c.poll(1.0)
    if message:
        print(message)