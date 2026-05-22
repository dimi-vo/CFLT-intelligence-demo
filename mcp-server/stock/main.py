import base64
import json
import logging
import os
import secrets

import uvicorn
import yfinance as yf
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("stock-mcp")

TICKERS = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "GOOG", "META", "SPY"]

MCP_USERNAME = os.environ.get("MCP_USERNAME", "bob")
MCP_PASSWORD = os.environ.get("MCP_PASSWORD", "whoisthat")

mcp = FastMCP("stock", host=os.environ.get("FASTMCP_HOST", "127.0.0.1"))


class BasicAuthMiddleware:
    """Simple HTTP Basic Auth middleware for Starlette/ASGI apps."""

    def __init__(self, app: ASGIApp, username: str, password: str) -> None:
        self.app = app
        self.username = username
        self.password = password

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode()

        if auth_header.startswith("Basic "):
            decoded = base64.b64decode(auth_header[6:]).decode()
            provided_user, _, provided_pass = decoded.partition(":")
            if (
                secrets.compare_digest(provided_user, self.username)
                and secrets.compare_digest(provided_pass, self.password)
            ):
                await self.app(scope, receive, send)
                return

        response = Response("Unauthorized", status_code=401, headers={"WWW-Authenticate": 'Basic realm="MCP"'})
        await response(scope, receive, send)


def compute_vwap(df) -> float:
    """Compute VWAP from a DataFrame with High, Low, Close, Volume columns."""
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    return (typical_price * df["Volume"]).sum() / df["Volume"].sum()


@mcp.tool()
def check_vwap_confirmation(ticker: str, period: str = "1mo") -> list[TextContent]:
    """Check whether a ticker's current price is above its VWAP, confirming a
    bullish signal.

    Use this tool when a bullish sentiment flag has been received for a ticker
    and you want to validate it against price-volume action.

    Args:
        ticker: Stock ticker symbol (e.g. "NVDA", "AAPL").
        period: Look-back period for VWAP calculation (default "1mo").
                Accepts yfinance period strings: 1d, 5d, 1mo, 3mo, 6mo, 1y.
    """
    logger.info("check_vwap_confirmation called: ticker=%s period=%s", ticker, period)

    ticker = ticker.upper()
    if ticker not in TICKERS:
        logger.warning("Unsupported ticker: %s", ticker)
        return [TextContent(type="text", text=json.dumps({"error": f"Unsupported ticker: {ticker}. Supported: {TICKERS}"}))]

    df = yf.Ticker(ticker).history(period=period)
    if df.empty:
        logger.warning("No data returned for %s", ticker)
        return [TextContent(type="text", text=json.dumps({"error": f"No data returned for {ticker}"}))]

    vwap = compute_vwap(df)
    latest_close = df["Close"].iloc[-1]
    bullish_confirmed = bool(latest_close > vwap)

    result = {
        "ticker": ticker,
        "period": period,
        "vwap": round(float(vwap), 4),
        "latest_close": round(float(latest_close), 4),
        "bullish_confirmed": bullish_confirmed,
        "summary": (
            f"{ticker} closed at {latest_close:.2f}, "
            f"{'above' if bullish_confirmed else 'below'} "
            f"VWAP of {vwap:.2f} — "
            f"{'confirms' if bullish_confirmed else 'does not confirm'} bullish signal."
        ),
    }

    logger.info("Returning result: %s", json.dumps(result, indent=2))
    return [TextContent(type="text", text=json.dumps(result))]


if __name__ == "__main__":
    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    print(f"Starting the server (transport={transport})")

    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        app = mcp.sse_app() if transport == "sse" else mcp.streamable_http_app()
        app = BasicAuthMiddleware(app, MCP_USERNAME, MCP_PASSWORD)
        config = uvicorn.Config(
            app,
            host=mcp.settings.host,
            port=mcp.settings.port,
            log_level=mcp.settings.log_level.lower(),
        )
        uvicorn.Server(config).run()
