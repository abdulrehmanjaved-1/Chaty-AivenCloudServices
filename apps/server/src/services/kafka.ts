import { Kafka, Producer } from "kafkajs";
import path from "path";
import fs from "fs";
const dotenv = require('dotenv');
dotenv.config();
import prismaClient from "./prisma";
const kafka = new Kafka({
  brokers: ["kafka-1a1df383-abdulrehmanjaveds12-1130.a.aivencloud.com:13326"],
  ssl: {
    ca: [fs.readFileSync(path.resolve("./ca.pem"), "utf-8")],
  },
  sasl: {
    username: process.env.KAFKA_USERNAME as string,
    password: process.env.KAFKA_PASSWORD as string,
    mechanism: "plain",
  },
});

let producer: null | Producer = null;

export async function createProducer() {
  if (producer) return producer; //so that we don't have to create producer again and again
  const _producer = kafka.producer();
  await _producer.connect();
  producer = _producer; //assigning producer to global variable
  return producer;
}

export async function produceMessage(message: string) {
  const producer = await createProducer();
  await producer.send({
    messages: [{ key: `message-${Date.now()}`, value: message }],
    topic: "MESSAGES",
  });
}

export async function startMessageConsumer(){
    console.log('Consumer is running...');
    const consumer=kafka.consumer({groupId:'default'});
    await consumer.connect();
    await consumer.subscribe({topic:'MESSAGES',fromBeginning:true});
    await consumer.run({
        autoCommit:true,
        eachMessage:async ({message,pause})=>{
            console.log(`New Received message ${message.value}}`);
            if(!message.value) return;
            try {
                await prismaClient.message.create({
                    data:{
                        text:message.value.toString(),
                    }
                })
            } catch (error) {
                console.log('Something is wrong with db');
                //if error it means db goes down
                //pause the consumer
                pause();
                setTimeout(() => {
                    consumer.resume([{topic:'MESSAGES'}]);
                }, 60 * 1000);
            }
        }
    })
}

export default kafka;
