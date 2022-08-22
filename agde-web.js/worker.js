importScripts("node_modules/agde-web/agde_web.js")
importScripts("node_modules/localforage/dist/localforage.js")

async function delay(duration) {
    await new Promise((resolve, _) => {
        setTimeout(() => resolve(), duration * 1000)
    })
}

let handle
let lf
async function init(url) {
    lf = localforage.createInstance({ name: "agde" })

    await delay(0.5)

    await wasm_bindgen("node_modules/agde-web/agde_web_bg.wasm")
    wasm_bindgen.init_agde()

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
        0
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
            init(msg.data.url)
            break
        case "commit":
            let _cursors = await handle.commit_and_send([])

            await handle.flush()

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

        default:
            console.error("Received an unexpected message from host: " + msg)
            break
    }
}
