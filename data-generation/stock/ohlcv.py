import logging

import yfinance as yf

from tickers import TICKERS

logger = logging.getLogger(__name__)


def fetch_ohlcv(period: str = "1y") -> dict[str, list[dict]]:
    """Fetch historical daily OHLCV data for all tickers.

    Returns a dict mapping ticker -> list of OHLCV records (oldest first).
    """
    data = {}

    for ticker in TICKERS:
        logger.info("Fetching %s OHLCV data for %s...", period, ticker)
        df = yf.Ticker(ticker).history(period=period)

        records = []
        for date, row in df.iterrows():
            records.append(
                {
                    "ticker": ticker,
                    "ts": int(date.timestamp() * 1000),
                    "t_open": round(row["Open"], 4),
                    "t_high": round(row["High"], 4),
                    "t_low": round(row["Low"], 4),
                    "t_close": round(row["Close"], 4),
                    "t_volume": int(row["Volume"]),
                }
            )

        data[ticker] = records
        logger.info("Fetched %d days for %s", len(records), ticker)

    return data
