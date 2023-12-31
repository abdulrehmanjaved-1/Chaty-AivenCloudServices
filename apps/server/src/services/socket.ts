import Redis from "ioredis";
import { Server } from "socket.io";
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";

const pub=new Redis({
    host:process.env.REDIS_HOST,
    port:process.env.REDIS_PORT as unknown as number,
    password:process.env.REDIS_PASSWORD,
    username:process.env.REDIS_USERNAME
});
const sub=new Redis({
  host:process.env.REDIS_HOST,
  port:process.env.REDIS_PORT as unknown as number,
  password:process.env.REDIS_PASSWORD,
  username:process.env.REDIS_USERNAME
});

class SocketService {
    private _io: Server;
  
    constructor() {
      console.log("Init Socket Service...");
      this._io = new Server({
        cors: {
          allowedHeaders: ["*"],
          origin: "*",
        },
      });
      sub.subscribe("MESSAGES");
    }
  
    public initListeners() {
      const io = this.io;
      console.log("Init Socket Listeners...");
  
      io.on("connect", (socket) => {
        console.log(`New Socket Connected`, socket.id);
        socket.on("event:message", async ({ message }: { message: string }) => {
          console.log("New Message Rec.", message);
          // publish this message to redis
          await pub.publish("MESSAGES", JSON.stringify({ message }));
        });
      });
  
      sub.on("message",async (channel, message) => {
        if (channel === "MESSAGES") {
          console.log("new message from redis", message);
          io.emit("message", message);
          //store this message in db(wrong approach)
          // await prismaClient.message.create({
          //   data: {
          //     text: message.value,
          //   },
          // })
          await produceMessage(message);
          console.log("Message produced to kafka broker"); //what is broker?ans:
        }
      });
    }
  
    get io() {
      return this._io;
    }
  }
  
  export default SocketService;