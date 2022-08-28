importScripts("node_modules/agde-web/agde_web.js")
importScripts("node_modules/localforage/dist/localforage.js")

async function delay(duration) {
    await new Promise((resolve, _) => {
        setTimeout(() => resolve(), duration * 1000)
    })
}

let handle
let lf
/**
 * @param url{string}
 * @param user_data_callback{ (user_data: Uint8Array) => any }
 * @param disconnect_callback{ () => any }
 * @param pier_disconnect_callback{ (pier: string) => any }
 * @param log_level{string|undefined}
 */
async function init(url, user_data_callback, disconnect_callback, pier_disconnect_callback, log_level) {
    lf = localforage.createInstance({ name: "agde" })

    await delay(0.5)

    await wasm_bindgen("node_modules/agde-web/agde_web_bg.wasm")
    wasm_bindgen.init_agde(log_level)

    handle = await wasm_bindgen.run(
        true,
        "none",
        async (path) => {
            let item = await lf.getItem(path)
            return item
        },
        async (path, data, compression, size) => {
            let obj = { data, compression, size, mtime: (new Date() * 1) / 1000 }
            await lf.setItem(path, obj)
        },
        async (path) => {
            await lf.removeItem(path)
        },
        async (path) => {
            let item = await lf.getItem(path)
            if (item === null) {
                return null
            }
            return item.mtime
        },
        async () => {
            let keys = {}
            await lf.iterate((value, key) => {
                keys[key] = { size: value.size }
            })
            return keys
        },
        url,
        0,
        user_data_callback,
        disconnect_callback,
        pier_disconnect_callback
    )
}

async function flush() {
    console.log("Flushing.")
    if (handle !== undefined) {
        await handle.flush()
    }
    console.log("Flush complete.")
}
async function shutdown() {
    console.log("Shutting down.")
    if (handle !== undefined) {
        await handle.shutdown()
    }
    console.log("We are shut down.")
}

onmessage = async (msg) => {
    switch (msg.data.action) {
        case "init":
            init(
                msg.data.url,
                (user_data) => postMessage({ action: "user_data", user_data }, [user_data.buffer]),
                () => postMessage({ action: "disconnect" }),
                (pier) => postMessage({ action: "pier_disconnect", pier }),
                msg.data.log_level
            )
            break
        case "commit":
            let cursors = await handle.commit_and_send(msg.data.cursors ?? [])

            await handle.flush()

            postMessage({ committed: true, cursors })

            // send back change event
            break

        case "shutdown":
            shutdown()
            break

        case "flush":
            await flush()
            break

        case "get":
            let resource = msg.data.document
            while (handle === undefined) {
                await delay(0.2)
            }
            await handle.flush()
            let data = lf.getItem(`current/${resource}`).data
            postMessage({ document: resource, data })
            break

        case "put":
            await lf.setItem(`current/${msg.data.document}`, {
                data: msg.data.data,
                compression: "none",
                size: msg.data.data.length,
                mtime: new Date() / 1000,
            })
            break

        case "uuid":
            while (handle === undefined) {
                await delay(0.2)
            }
            let uuid = await handle.uuid()
            postMessage({ uuid })
            break

        case "send":
            while (handle === undefined) {
                await delay(0.5)
            }
            await handle.send(msg.data.data)
            break

        default:
            console.error("Received an unexpected message from host: " + msg)
            break
    }
}
