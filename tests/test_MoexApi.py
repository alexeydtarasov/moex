from moex import MoexApi
import moex
import datetime


with open('login_password.txt') as fin:
    login, password = tuple(map(lambda x: x.strip(), fin.readlines()))
    moex_api = MoexApi(login, password)
with open('./tests/tickers_to_test.csv') as fin:
    tickers = list(map(lambda x: x.strip(), fin.readlines()))


def test_auth():
    moex_api._auth()
    assert moex_api.authentification
    assert type(moex_api.token) == str
    assert len(moex_api.token) > 10
    flogin = 'login'
    fpassword = 'password'
    false_api = MoexApi(flogin, fpassword)
    false_api._auth()
    assert not false_api.authentification
    assert false_api.token is None


def test_url_loader():
    url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities/MOEX.json'
    resp = moex_api._load_url(url)
    assert resp is not None


def test_token_validator():
    url = 'https://iss.moex.com/iss/engines/stock/markets/shares/securities/MOEX.json'
    resp = moex_api._load_url(url)
    resp_test = moex._is_token_valid(response_headers=resp.headers)
    assert resp_test is not None
    assert type(resp_test) == bool
    # if login and password correct
    assert resp_test


def test_cap():
    for ticker in tickers:
        cap = moex_api.capitalization(ticker)
        assert cap is not None
        assert cap > 1_000_000


def test_realtime_price():
    for ticker in tickers:
        df = moex_api.realtime(ticker)
        assert df is not None
        assert len(df) > 0
        price = df.loc[:, 'LAST'].values[0] > 0
        assert price > 0


def test_trades():
    for ticker in tickers:
        df = moex_api.trades(ticker)
        assert df is not None
        assert len(df) > 1_000
        assert len(df.columns) > 5


def test_orderbook():
    # only for trade days
    now_dt = datetime.datetime.now()
    if now_dt.weekday() > 0 and now_dt.weekday() < 5 and now_dt.hour > 10 and now_dt.hour < 23:
        depth = 10
        for ticker in tickers:
            for depth in [10, 5]:
                df = moex_api.orderbook(ticker, depth)
                assert df is not None
                assert len(df) == depth * 2
    assert True