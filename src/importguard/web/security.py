"""Small pure-ASGI middlewares for a local, single-operator tool.

* ``BodySizeLimit`` stops oversized uploads while they stream in (Starlette's
  multipart parser has no file size limit of its own).
* ``SameOriginPosts`` rejects cross-site form posts (CSRF from another site
  open in the operator's browser).
* ``SecurityHeaders`` adds a strict Content-Security-Policy and friends.
"""

from __future__ import annotations

from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

MULTIPART_OVERHEAD = 64 * 1024
UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

CONTENT_SECURITY_POLICY = (
    "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; "
    "form-action 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
)


class _TooLarge(Exception):
    pass


class BodySizeLimit:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes + MULTIPART_OVERHEAD

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] not in UNSAFE_METHODS:
            await self.app(scope, receive, send)
            return
        declared = Headers(scope=scope).get("content-length")
        if declared and declared.isdigit() and int(declared) > self.max_body_bytes:
            await self._reject(scope, receive, send)
            return

        received = 0
        too_large = False
        rejected = False

        async def counting_receive() -> Message:
            nonlocal received, too_large
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    too_large = True
                    raise _TooLarge
            return message

        async def guarded_send(message: Message) -> None:
            # The framework may turn the aborted body read into its own 400; answer 413 instead.
            nonlocal rejected
            if not too_large:
                await send(message)
            elif not rejected:
                rejected = True
                await self._reject(scope, receive, send)

        try:
            await self.app(scope, counting_receive, guarded_send)
        except _TooLarge:
            if not rejected:
                await self._reject(scope, receive, send)

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        limit_mib = (self.max_body_bytes - MULTIPART_OVERHEAD) // (1024 * 1024)
        response = PlainTextResponse(
            f"The upload is larger than the {limit_mib} MiB limit.", status_code=413, headers={"Connection": "close"}
        )
        await response(scope, receive, send)


class SameOriginPosts:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["method"] in UNSAFE_METHODS and not _same_origin(Headers(scope=scope)):
            response = PlainTextResponse("Cross-site request rejected.", status_code=403)
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)


def _same_origin(headers: Headers) -> bool:
    origin = headers.get("origin")
    if origin is not None:
        return origin != "null" and urlsplit(origin).netloc == headers.get("host")
    # No Origin header (old browser or a non-browser client such as curl): fall back to Fetch Metadata.
    return headers.get("sec-fetch-site", "same-origin") in ("same-origin", "none")


class SecurityHeaders:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                headers.extend(
                    [
                        (b"content-security-policy", CONTENT_SECURITY_POLICY.encode()),
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                        (b"referrer-policy", b"same-origin"),
                    ]
                )
            await send(message)

        await self.app(scope, receive, send_with_headers)
