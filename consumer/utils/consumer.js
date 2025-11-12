import { Kafka } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { File } from '../models/File.js';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'kafka' });

export const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'demo-kafka-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      console.log("****************** Arrived in Consumer ******************");

      const rawValue = message.value.toString();
      let obj;

      try {
        obj = JSON.parse(rawValue);
      } catch {
        console.warn("Message is not JSON, using as plain text");
        obj = { username: rawValue };
      }

      console.log("our object:", obj);

      try {
        const newFile = new File({
          userRef: uuidv4(),
          username: obj.username,
        });
        const response = await newFile.save();
        if (response) {
          console.log("File created successfully");
        }
      } catch (e) {
        console.error("Error saving file:", e);
      }
    },
  });
};



// await consumer.run({
//   eachMessage: async ({ partition, message }) => {
//     console.log({
//       partition,
//       offset: message.offset,
//       value: message.value.toString(),
//     });
//   }
// })