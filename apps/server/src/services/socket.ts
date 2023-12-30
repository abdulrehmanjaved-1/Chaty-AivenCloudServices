import Redis from "ioredis";
import { Server } from "socket.io";

const pub=new Redis({
    host:'redis-1b7efc0c-abdulrehmanjaveds12-1130.a.aivencloud.com',
    port:13313,
    password:'AVNS_sXKsPqhW3Xt7128XB79',
    username:'default'
});
const sub=new Redis({
    host:'redis-1b7efc0c-abdulrehmanjaveds12-1130.a.aivencloud.com',
    port:13313,
    password:'AVNS_sXKsPqhW3Xt7128XB79',
    username:'default'
});

class SocketService{
    private _io:Server;
    constructor(){ 
        console.log("Init SocketService");
        this._io=new Server({
            cors:{
                allowedHeaders:['*'],
                origin:'*'
            }
        });
        sub.subscribe("MESSAGE");
    }
    get io(){
        return this._io;
    }
    public initListeners(){
        const io=this._io;
        console.log("Init Listeners");
        io.on('connect', (socket) => {
            console.log('new socket connected',socket.id);
            socket.on("event:message",async ({message}:{message:string})=>{
                console.log("new message received",message);
                //publish this message to redis
                try {
                    const numRecipients = await pub.publish("MESSAGE", message);
                    if (numRecipients > 0) {
                        console.log('Message was successfully published');
                    } else {
                        console.log('No clients received the message');
                    }
                } catch (error) {
                    console.error('An error occurred while publishing the message:', error);
                }               
            });
            sub.on('message',(channel,message)=>{
                if(channel==="MESSAGE"){
                    console.log("Receiving events from redis directly",message);
                    io.emit("message",message);    
                }
            })
    })
 
    }
}
export default SocketService;