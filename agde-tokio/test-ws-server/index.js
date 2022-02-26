import { WebSocketServer } from "ws"

const wss = new WebSocketServer({ port: 8081 })

/**
 * @type WebSocket.WebSocket[]
 */
const clients = []

wss.on("connection", (ws) => {
    clients.push(ws)
    ws.on("message", (data, isBinary) => {
        if (isBinary) {
            console.log(`Got binary message.`)

            clients.forEach((client) => {
                if (client === ws) {
                    return
                }
                client.send(data)
            })
        } else {
            console.log(`Got text message: '${data}'`)
        }
    })
    ws.send("Nice to meet you!")
})
