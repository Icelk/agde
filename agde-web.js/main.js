let worker = new Worker("worker.js")
worker.addEventListener("message", (msg) => {
    console.log(`Received message from worker: ${msg}`)
})
function get_docuemnt(worker, document) {
    worker.postMessage({ action: "get", document })
    new Promise((resolve, _reject) => {
        let listener = (msg) => {
            if (msg.data.document === document) {
                resolve(msg.data.data)
                worker.removeEventListener(listener)
            }
        }
        worker.addEventListener("message", listener)
    })
}

window.addEventListener("beforeunload", (_) => {
    worker.postMessage({ action: "shutdown" })
})
