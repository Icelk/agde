import { WebSocketServer } from "ws"

const wss = new WebSocketServer({ port: 8081 })

wss.on('connection', (ws) => {
    ws.on('message', data => {
        console.log(`Got message: '${data}'`);
    })
    ws.send("Nice to meet you!")
})
