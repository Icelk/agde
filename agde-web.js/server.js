const http = require("http")
const fs = require("fs")

const server = http.createServer((req, res) => {
    let path = `./${req.url}`
    if (!fs.existsSync(path)) {
        res.writeHead(404)
        res.end("")
        return
    }
    if (fs.statSync(path).isDirectory()) {
        path = `${path}/index.html`
    }
    if (!fs.existsSync(path)) {
        res.writeHead(404)
        res.end("")
        return
    }
    const data = fs.readFileSync(path)
    if (path.endsWith(".js")) {
        res.setHeader("content-type", "application/javascript")
    } else if (path.endsWith(".html")) {
        res.setHeader("content-type", "text/html")
    } else if (path.endsWith(".wasm")) {
        res.setHeader("content-type", "application/wasm")
    }
    res.writeHead(200)
    res.end(data)
})

server.listen(8082)
