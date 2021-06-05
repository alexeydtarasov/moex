import datetime
import pytest
import pandas as pd

import moex

test_tickers = pd.read_csv("tickers_to_test.csv", header=None)[0].values


@pytest.mark.parametrize("ticker", test_tickers)
def test_capitalization(ticker):
    cap = moex.capitalization(ticker)
    assert type(cap) is int or float
    assert cap > 0


@pytest.mark.parametrize("ticker", test_tickers)
def test_history_price_range(ticker):
    date1 = datetime.date.today() - datetime.timedelta(days=110)
    date2 = datetime.date.today()
    history = moex.history_price_range(ticker, date2, date1)
    assert len(history) > 0


@pytest.mark.parametrize("ticker", test_tickers)
def test_orderbook(ticker):
    DEPTH = 10
    orderbook = moex.orderbook(ticker, depth=DEPTH)
    assert orderbook is not None
    assert len(orderbook) > 0
    assert len(orderbook) == 2 * DEPTH


@pytest.mark.parametrize("ticker", test_tickers)
def test_realtime_price(ticker):
    quotes = moex.realtime_price(ticker)
    assert quotes is not None
    assert quotes['LAST'].values[0] > 0
    assert quotes['VOLTODAY'].values[0] > 0
