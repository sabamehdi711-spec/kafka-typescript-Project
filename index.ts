import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

async function run() {
  await producer.connect();
  await consumer.connect();

  // Subscribe to topic
  await consumer.subscribe({ topic: 'user-clicks', fromBeginning: true });

  // Listen for messages
  consumer.run({
    eachMessage: async ({ message }) => {
      console.log(`Received Event: ${message.value?.toString()}`);
    },
  });

  // Produce a sample event
  await producer.send({
    topic: 'user-clicks',
    messages: [{ value: JSON.stringify({ userId: 123, action: 'BUTTON_CLICK' }) }],
  });
}

run().catch(console.error);