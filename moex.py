import datetime
import pandas as pd
import requests

from traceback import format_exc
from config import logger, MOEX_TOKEN_PATH


def _fmt_date(date):
    return date.strftime('%Y-%m-%d')


def _history_price_range(ticker, date_to, date_from, columns, engine, market,
                  boardid, trading_session, headers, timeout: int=10) -> pd.DataFrame:
    """Supporting internal function for history_price_range func"""
    url = f'https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/'
    url += f'{ticker}.json?from={_fmt_date(date_from)}&tradingsession={trading_session}'
    url += f'&till={_fmt_date(date_to)}'
    resp = _safe_query(url, headers)

    if resp is None:
        logger.error("Safe query returned None while parsing quotes for %s from %s to %s" %
                     (ticker, _fmt_date(date_from), _fmt_date(date_to)))
        return None
    elif resp.status_code != 200:
        logger.error("Error %i occurred while parsing quotes for %s from %s to %s" %
                     (resp.status_code, ticker, _fmt_date(date_from), _fmt_date(date_to)))
        return None

    resp = resp.json()['history']

    df = pd.DataFrame(resp['data'], columns=resp['columns'])
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'], format='%Y-%m-%d')
    df = df.loc[
            (df['BOARDID'] == boardid) &
            (df['TRADEDATE'].dt.date.between(date_from, date_to)),
            columns
        ].reset_index(drop=True)
    logger.debug("Intermediate dataframe for %s from %s to %s: \n%s" %
                 (ticker, _fmt_date(date_from), _fmt_date(date_to), df))

    return df


def _is_token_granted(response_headers: dict) -> bool:
    response_headers = dict(response_headers)
    if response_headers.get('X-MicexPassport-Marker', 'denied') != 'granted':
        return False
    return True


def _read_token(path: str='../common_data/moex_token.txt'):
    try:
        with open(path) as fin:
            return fin.read().strip()
    except:
        logger.error("Failed to load token from %s:\n %s" % (path, format_exc()))


def _safe_query(url: str, moex_token: str=None, timeout: int=10):
    if moex_token is None:
        moex_token = _read_token(MOEX_TOKEN_PATH)
    headers = {"Cookie": f'MicexPassportCert={moex_token}'}

    for _ in range(2):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            return resp
        except:
            logger.debug("Safe query error:\n%s" % format_exc())
            logger.debug("Request URL: %s" % url)
            logger.debug("Request headers: %s" % headers)
            continue


def _upd_token(prev_token: str) -> str:
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER.json"
    headers = {"Cookie": f'MicexPassportCert={prev_token}'}
    for _ in range(5):
        response = requests.get(url, headers=headers)
        text = dict(response.headers).get('Set-Cookie')
        if text is None or 'MicexPassportCert=' not in text:
            continue
        idx1 = text.find("MicexPassportCert=") + len("MicexPassportCert=")
        idx2 = text[idx1:].find('; ')
        new_token = text[idx1:(idx1+idx2)]
        _write_token(new_token, path=MOEX_TOKEN_PATH)
        return new_token
    logger.warning("Failed to get updated MOEX token")
    return None


def _write_token(token: str,
                 path: str='../common_data/moex_token.txt'):
    try:
        with open(path, 'w') as fout:
            fout.write(token)
    except:
        logger.error("Failed to write token to %s: \n%s" % (path, format_exc()))


def capitalization(ticker: str,
                  engine: str='stock',
                  market: str='shares',
                  boardid: str='TQBR'):
    url = f'https://iss.moex.com/iss/engines/{engine}'
    url += f'/markets/{market}/securities/{ticker}.json'

    resp = _safe_query(url)

    if resp is None or resp.status_code != 200:
        logger.error("Safe query returned None while parsing capitalization for %s" % ticker)
        return None
    elif resp.status_code != 200:
        logger.error("Error %i occurred while parsing capitalization for %s" % (resp.status_code, ticker))
        return None

    resp = resp.json()['marketdata']
    df = pd.DataFrame(resp['data'], columns=resp['columns'])

    if len(df) > 0:
        cap = df.loc[df['BOARDID'] == boardid, 'ISSUECAPITALIZATION'].values[0]
        logger.debug("Parsed %s capitalization: %s" % (ticker, cap))
        return cap
    else:
        logger.error("Capitalization parser: parsed dataframe appears to be empty")
        return None


def history_price_range(ticker: str, date_to: datetime.date,
                 date_from: datetime.date=None,
                 columns=['TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'],
                 engine: str='stock',
                 market: str='shares',
                 boardid: str='TQBR',
                 headers: dict=None,
                 trading_session=3,
                 correct_from_date: bool=False
                 ) -> pd.DataFrame:
    """
    Returns DataFrame with daily quotes for all trading days for the {ticker}

    Parameters:
        ticker: str
        date_to: datetime.date
            Last date of parsing interval. Note that if date_to is not a trade day
            then function returns data for nearest trade day in the past
        date_from: datetime.date
            First date of parsing interval. If None, then function will return data
            for last 50 calendar days (not trading days!)
        columns: list from
            ['BOARDID', 'TRADEDATE', 'SHORTNAME', 'SECID', 'NUMTRADES', 'VALUE',
           'OPEN', 'LOW', 'HIGH', 'LEGALCLOSEPRICE', 'WAPRICE', 'CLOSE', 'VOLUME',
           'MARKETPRICE2', 'MARKETPRICE3', 'ADMITTEDQUOTE', 'MP2VALTRD',
           'MARKETPRICE3TRADESVALUE', 'ADMITTEDVALUE', 'WAVAL', 'TRADINGSESSION']
            order and names of columns to return
        engine: str from ['stock', ...]
            Use 'stock' for getting data about RTSI index and stocks
        market: str from ['shares', 'index']
            Use index if you want to get data for RTSI index as example
        boardid: str from ['TQBR', 'RTSI']
            TQBR - stock, RTSI - RTS index
        headers: dict
            for request adjustment
        trading_session: int from [1, 2, 3]
            1 - main daytime session, 2 - evening session, 3 - both
        correct_from_date: bool
            By default if date_from is not a trade day, function will return data from NEXT
            trade day up to date_to
            If the parameter equals {True}, then function will return data from PREVIOUS trade
            day as well to take into account, for example, correct close price for date_from
    Examples:
        history_price_range('SBER', datetime.date(2021, 5, 4));
        history_price_range('MOEX', datetime.date(2021, 5, 4), datetime.date(2021, 1, 4))
    """
    one_row = False
    if date_from is None:
        one_row = True
        current_date_from = date_to - datetime.timedelta(days=50)
    else:
        current_date_from = date_from

    n_days = (date_to - current_date_from).days
    parse_times = n_days // 50
    if n_days % 50 != 0: parse_times += 1

    result_df = pd.DataFrame(columns=columns)

    for _ in range(parse_times):
        date_to = current_date_from + datetime.timedelta(days=50)

        df = _history_price_range(ticker, date_to, current_date_from, columns,
                           engine, market, boardid, trading_session, headers)

        if df is not None:
            result_df = pd.concat([result_df, df], ignore_index=True)
        else:
            break

        current_date_from = current_date_from + datetime.timedelta(days=50)

    if one_row:
        return result_df.tail(1)

    if correct_from_date and date_from < result_df.loc[0, 'TRADEDATE']:
        date_to = date_from
        date_from = date_from - datetime.timedelta(days=50)
        df = _history_price_range(ticker, date_to, date_from, columns,
                           engine, market, boardid, trading_session, headers)
        df = df.tail(1)
        result_df = pd.concat([df, result_df], ignore_index=True)

    logger.debug("Final dataframe for %s from %s to %s: \n%s" %
                 (ticker, _fmt_date(date_from), _fmt_date(date_to), result_df))
    return result_df.reset_index(drop=True)


def orderbook(ticker,
              moex_token: str=None,
              depth: int=10,
              engine: str='stock',
              market: str='shares',
              boardid: str='TQBR',
              columns: list=['SECID', 'BUYSELL', 'PRICE', 'QUANTITY', 'UPDATETIME']
              ) -> pd.DataFrame:
    """
    Return DataFrame with orderbook

    Parameters:
        ticker: str
        moex_token: str
            MOEX token auth is required for orderbook data downloading
        depth: int from {1, ... 10}
            number of rows with buy/sell orders
        engine: str
        market: str
        boardid: str
        columns: list

    Examples:
        df = orderbook('MOEX', moex_token='your_token', depth=5)
    """
    url = f'https://iss.moex.com/iss/engines/{engine}'
    url += f'/markets/{market}/securities/{ticker}/orderbook.json'

    resp = _safe_query(url, moex_token=moex_token)

    if resp is None or resp.status_code != 200:
        logger.error("Safe query returned None while parsing orderbook for %s" % ticker)
        return None
    elif resp.status_code != 200:
        logger.error("Error %i occurred while parsing orderbook for %s" % (resp.status_code, ticker))
        return None

    if not _is_token_granted(resp.headers):
        logger.warning("Orderbook parser: token appears to be invalid")

    resp = resp.json()['orderbook']

    try:
        df = pd.DataFrame(resp['data'], columns=resp['columns'])
        df = df.loc[df['BOARDID'] == boardid, columns]
        df['UPDATETIME'] = datetime.datetime.now()
        df.reset_index(inplace=True, drop=True)
        df = df.iloc[
            int((df.shape[0] / 2) - depth):
            int(df.shape[0] / 2 + depth)
        ]
        if df.shape[0] == 0:
            logger.warning("%s orderbook appears to be empty" % ticker)
            return None
        logger.debug("%s orderbook:\n%s" % (ticker, df))
        return df
    except:
        logger.error("Failed to parse orderbook for %s:\n%s" % (ticker, format_exc()))
        return None


def realtime_price(ticker: str, moex_token: str=None,
                   engine: str='stock',
                   market: str='shares',
                   boardid: str='TQBR',
                   columns: list=["UPDATETIME", "SECID", "LAST",
                                  "VOLTODAY", 'ISSUECAPITALIZATION'],
                   ) -> pd.DataFrame:
    """
    Returns: DataFrame with single row - latest price and some other data
    specified in {colunms} parameter
    """
    url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}'
    url += f'/securities/{ticker}.json'
    resp = _safe_query(url, moex_token)

    if resp is None or resp.status_code != 200:
        logger.error("Safe query returned None while parsing realtime price for %s" % ticker)
        return None
    elif resp.status_code != 200:
        logger.error("Error %i occurred while parsing realtime price for %s" % (resp.status_code, ticker))
        return None

    if not _is_token_granted(resp.headers):
        logger.warning("Realtime price parser: token appears to be invalid")

    resp = resp.json()['marketdata']
    df = pd.DataFrame(resp['data'], columns=resp['columns'])
    logger.debug("%s realtime quotes:\n%s" % (ticker, df))
    return df.loc[df['BOARDID'] == boardid, columns].reset_index(drop=True)


def trades(ticker: str,
           moex_token: str,
           engine: str='stock',
           market: str='shares',
           columns: list=[
                   'TRADENO', 'TRADETIME', 'SECID',
                   'PRICE', 'QUANTITY', 'VALUE', 'BUYSELL',
                   'TRADINGSESSION']):
    """Returns all trades made during previous trade day"""
    base_url = f'https://iss.moex.com/iss/engines/{engine}'
    base_url += f'/markets/{market}/securities/{ticker}/trades.json'
    additional = ''
    first_trade_num = None
    result_df = None
    while True:
        try:
            resp = _safe_query(base_url + additional, moex_token)
            if resp is None:
                return None
            if not _is_token_granted(resp.headers):
                logger.warning("Trades parser: token appears to be invalid")
            resp = resp.json()['trades']
            if result_df is None:
                result_df = pd.DataFrame(resp['data'], columns=resp['columns'])
            else:
                cur_df = pd.DataFrame(resp['data'], columns=resp['columns'])
                if len(cur_df) == 0 or cur_df.loc[0, 'TRADENO'] == first_trade_num:
                    return result_df.loc[:, columns]
                result_df = pd.concat([result_df, cur_df], ignore_index=True)
                first_trade_num = result_df.loc[0, 'TRADENO']
            if len(result_df) == 0:
                break
            last_trade_no = result_df.loc[len(result_df) - 1, 'TRADENO']
            additional = f'?tradeno={last_trade_no}&next_trade=1'
        except:
            logger.warning("Error while parsing trades for %s:\n%s" % (ticker, format_exc()))
