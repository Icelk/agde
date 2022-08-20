# agde-web

A web-first implementation of agde-io.

Before shutdown, make sure to call the shutdown method.
I suggest using a persistent web Worker and messaging it to shutdown when the
window event `beforeunload` is fired (using `window.addEventListener("beforeunload", () => {...})`).
