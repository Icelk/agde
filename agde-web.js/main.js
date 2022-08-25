export let worker = new Worker("worker.js")

/**
 * @param url{string}
 */
export function init(url) {
    worker.postMessage({ action: "init", url })
}
worker.addEventListener("message", (msg) => {
    console.log(`Received message from worker: ${msg}`)
})
/**
 * @param document{string}
 * @returns {Promise<Uint8Array>}
 */
export function get_document(document) {
    worker.postMessage({ action: "get", document })
    return new Promise((resolve, _reject) => {
        let listener = (msg) => {
            if (msg.data.document === document) {
                resolve(msg.data.data)
                worker.removeEventListener(listener)
            }
        }
        worker.addEventListener("message", listener)
    })
}
/**
 * @param document {string}
 * @param data {Uint8Array}
 */
export function put_document(document, data) {
    worker.postMessage({ action: "put", document, data })
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
