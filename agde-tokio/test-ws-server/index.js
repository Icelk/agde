import { WebSocketServer } from "ws"

const wss = new WebSocketServer({ port: 8081 })

/**
 * @type WebSocket.WebSocket[]
 */
const clients = []

wss.on("connection", (ws) => {
    clients.push(ws)
    ws.on("message", (data, isBinary) => {
        console.log(`Got message: '${data}'`)
        if (isBinary) {
            clients.forEach((client) => {
                if (client === ws) {
                    return
                }
                client.send(data)
            })
        }
    })
    ws.send("Nice to meet you!")
})
