import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ['localhost:9092', 'localhost:9092']
})

const producer = kafka.producer();

export const run = async (res) => {
  await producer.connect();

  await producer.send({
    topic: 'demo-kafka-topic',
    messages: [{ value: JSON.stringify(res) }]
  });

  console.log('Message sent successfuly', res);
}