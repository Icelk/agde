let worker = new Worker("worker.js")

/**
 * @type {string|null}
 */
export let uuid = null
export let disconnected = false
/**
 * @type {(pier: string) => void}
 */
let pier_disconnect_callback = (_pier) => {}
/**
 * @type {(data: Uint8Array) => void}
 */
let user_data_callback = (_data) => {}

/**
 * @param cb{(pier: string) => void}
 */
export function setPierDisconnectCallback(cb) {pier_disconnect_callback = cb}
/**
 * @param cb{(data: Uint8Array) => void}
 */
export function setUserDataCallback(cb) {user_data_callback = cb}

worker.addEventListener("message", (msg) => {
    if (msg.data.uuid !== undefined) {
        if (msg.data.uuid === null) {
            console.warn("Tried to get the UUID after disconnection.")
        } else {
            uuid = msg.data.uuid
        }
    }
    if (msg.data.action === "user_data") {
        let d = msg.data.user_data
        user_data_callback(d)
    }
    if (msg.data.action === "pier_disconnect") {
        let pier = msg.data.pier
        pier_disconnect_callback(pier)
    }
    if (msg.data.action === "disconnect") {
        disconnected = true
    }
})

/**
 * @param url{string}
 * @param log_level{string|undefined}
 */
export function init(url, log_level) {
    worker.postMessage({ action: "init", url, log_level })
    worker.postMessage({ action: "uuid" })
}
const lf = localforage.createInstance({ name: "agde" })
/**
 * @param document{string}
 * @returns {Promise<Uint8Array | null>}
 */
export async function get_document(document) {
    let doc = await lf.getItem(`current/${document}`)
    return doc?.data ?? new Uint8Array()
}
/**
 * @param document {string}
 * @param data {Uint8Array}
 */
export async function put_document(document, data) {
    await lf.setItem(`current/${document}`, {
        data,
        compression: "none",
        size: data.length,
        mtime: new Date() / 1000,
    })
}
/**
 * Commit the changes and send them to others. This also pulls in changes from others.
 * Remember to call {@link put_document} before calling this.
 * You can then retrieve the new version using {@link get_document}.
 *
 * @param cursors{{resource: string, index: number}[]}
 * @returns {Promise<{resource: string, index: number}[]>}
 */
export async function commit_and_send(cursors) {
    worker.postMessage({
        action: "commit",
        cursors,
    })
    let c = await new Promise((resolve, _reject) => {
        let listener = (msg) => {
            if (msg.data.committed === true) {
                resolve(msg.data.cursors)
                worker.removeEventListener("message", listener)
            }
        }
        worker.addEventListener("message", listener)
    })
    return c
}
/**
 * Send `s` as a `Message::User` to all other piers. Use the
 * @param s{string}
 */
export function send(s) {
    let te = new TextEncoder()
    let data = te.encode(s)
    worker.postMessage({ action: "send", data }, [data.buffer])
}
/**
 * Make sure the worker gets shut down before page unload.
 */
export function watchUnload() {
    window.addEventListener("beforeunload", (_) => {
        worker.postMessage({ action: "shutdown" })
    })
}
