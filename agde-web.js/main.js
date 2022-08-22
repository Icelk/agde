export let worker = new Worker("worker.js")

async function delay(duration) {
    await new Promise((resolve, _) => {
        setTimeout(() => resolve(), duration * 1000)
    })
}

/**
 * @param url{string}
 */
export async function init(url) {
    worker.postMessage({ action: "init", url })
    await delay(2)
            let te = new TextEncoder()
            put_docuemnt("testing", te.encode("this is a very nice file!"))
}
worker.addEventListener("message", (msg) => {
    console.log(`Received message from worker: ${msg}`)
})
/**
 * @param document{string}
 * @returns {Promise<Uint8Array>}
 */
export function get_docuemnt(document) {
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
export function put_docuemnt(document, data) {
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
