importScripts("node_modules/agde-web/agde_web.js")
importScripts("node_modules/localforage/dist/localforage.js")

async function delay(duration) {
    await new Promise((resolve, _) => {
        setTimeout(() => resolve(), duration * 1000)
    })
}

let handle
;(async () => {
    await delay(1)

    await wasm_bindgen("node_modules/agde-web/agde_web_bg.wasm")
    wasm_bindgen.init_agde()

    handle = await wasm_bindgen.run(
        true,
        "none",
        async (path) => {
            let item = await localforage.getItem(path)
            return item
        },
        async (path, data, compression, size) => {
            let obj = { data, compression, size, mtime: new Date() * 1 / 1000 }
            await localforage.setItem(path, obj)
        },
        async (path) => {
            await localforage.removeItem(path)
        },
        async (path) => {
            let item = await localforage.getItem(path)
            if (item === null) {
                return null
            }
            return item.mtime
        },
        async () => {
            let keys = {}
            await localforage.iterate((value, key) => {
                keys[key] = { size: value.size }
            })
            return keys
        },
        "ws://localhost:8081",
        0
    )

    console.log("Handle after running agde: " + handle.toString())
})()

async function flush() {
    console.log("Flushing.")
    await handle.flush()
    console.log("returned: " + JSON.stringify(handle))
    console.log("Flush complete.")
}
async function shutdown() {
    console.log("Shutting down.")
    await handle.shutdown()
    console.log("We are shut down.")
}

onmessage = async (msg) => {
    switch (msg.data.action) {
        case "commit":
            let _cursors = await handle.commit_and_send([])

            // send back change event
            break

        case "shutdown":
            shutdown()
            break

        case "flush":
            await flush()
            break

        case "get":
            let document = msg.data.document
            await handle.flush()
            let data = localforage.getItem(document).data
            data = atob(data)
            postMessage({ document, data })
            break

        default:
            console.error("Received an unexpected message from host: " + msg)
            break
    }
}
