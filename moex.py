import datetime
import logging
import pandas as pd
import requests
import traceback

from requests import (
        ReadTimeout,
        ConnectTimeout,
        HTTPError,
        Timeout,
        ConnectionError
        )


TIMEOUT_ERRORS = (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError)
LOG_LEVEL = logging.WARNING


def _fmt_date(date):
    return date.strftime('%Y-%m-%d')


def _is_token_valid(token: str=None,
                    response_headers: dict=None) -> bool:
    if token:
        url = "https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER.json"
        headers = {"Cookie": f'MicexPassportCert={token}'}
        for _ in range(5):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
            except TIMEOUT_ERRORS:
                continue
        response_headers = resp.headers
    return response_headers.get('X-MicexPassport-Marker', 'denied') == 'granted'


class MoexApi:
    def __init__(self, login: str='', password: str=''):

        logging.basicConfig(
                format="[%(levelname)s] %(asctime)s | %(message)s",
                datefmt='%Y-%m-%d %H:%M:%S',
                level=LOG_LEVEL
                )
        self.logger = logging.getLogger()
        self.login = login
        self.password = password
        self._auth()

    def _auth(self):
        if self.login != '' and self.password != '':

            try:
                resp = requests.get(
                        'https://passport.moex.com/authenticate',
                        auth=(self.login, self.password)
                        )
                assert resp.status_code == 200 and len(resp.text) > 10
                self.authentification, self.token = True, resp.text
                self.logger.info('Authentication was successful')
                return None
            except AssertionError:
                self.logger.error(
                        f"Authentication failed: invalid login={self.login} or password={self.password}"
                        )
        self.logger.info("Using API without token")
        self.authentification, self.token = False, None

    def _history_price_range(self, ticker, date_to, date_from, columns, engine, market,
                      boardid, trading_session, headers, timeout: int=10) -> pd.DataFrame:
        """Supporting internal function for history_price_range func"""
        url = f'https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/'
        url += f'{ticker}.json?from={_fmt_date(date_from)}&tradingsession={trading_session}'
        url += f'&till={_fmt_date(date_to)}'
        resp = self._load_url(url, use_token=False)

        if resp is None:
            self.logger.error(
                    f"History price range for {ticker} {_fmt_date(date_from)}-{_fmt_date(date_to)} parsing failed"
                    )
            return None

        resp = resp.json()['history']

        df = pd.DataFrame(resp['data'], columns=resp['columns'])
        df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'], format='%Y-%m-%d')
        df = df.loc[
                (df['BOARDID'] == boardid) &
                (df['TRADEDATE'].dt.date.between(date_from, date_to)),
                columns
            ].reset_index(drop=True)
        self.logger.info(
                "Intermediate dataframe for %s from %s to %s: \n%s" %
                     (ticker, _fmt_date(date_from), _fmt_date(date_to), df))
        return df

    def _load_url(self, url: str,
                  use_token: bool=True,
                  n_tries: int=5,
                  timeout: int=10) -> requests.Response:
        headers = dict()
        if use_token:
            headers = {"Cookie": f'MicexPassportCert={self.token}'}

        for _ in range(n_tries):
            try:
                self.logger.info(f'Loading url: {url}')
                resp = requests.get(url, headers=headers, timeout=timeout)
                if use_token and ( \
                                  resp.status_code == 403 or \
                                  not _is_token_valid(response_headers=resp.headers) \
                                  ):
                    self.logger.warning(
                            f"Token={self.token} is invalid otherwise you don't have rights for request the url"
                            )
                    self._auth()
                    continue
                assert resp is not None and resp.status_code == 200
                self.logger.info('Url was loaded correctly')
                return resp

            except AssertionError:
                err_msg = f"Incorrect response from ISS server\nurl: {url}\nheaders: {headers}"
                if resp is not None:
                    err_msg += f'\nresponse status code: {resp.status_code}'
                self.logger.error(err_msg)
            except TIMEOUT_ERRORS:
                self.logger.warning('Time limit exceed, retrying')
        err_msg = f"No response from ISS server\nurl: {url}\nheaders: {headers}"
        if resp is not None:
            err_msg += f'\ncode: {resp.status_code}'
        self.logger.error(err_msg)

    ## ====== Methods can be called without any token and any time delay ========

    def candles(self,
                ticker: str,
                tf: str,
                engine: str='stock',
                market: str='shares',
                boardid: str='TQBR') -> pd.DataFrame:
        """
        tf: str from ['1m', '3m', '5m', '10m', '15m', '30m', '45m', '1h',
                      '2h', '3h', '4h', 'D', 'W', 'M']
        '"""
        pass

    def capitalization(self,
                       ticker: str,
                       engine: str='stock',
                       market: str='shares',
                       boardid: str='TQBR') -> float:
        url = f'https://iss.moex.com/iss/engines/{engine}'
        url += f'/markets/{market}/securities/{ticker}.json'

        resp = self._load_url(url)
        if resp is None:
            self.logger.debug(f'Failed parsing capitalization for ticker={ticker}')
            return None

        resp = resp.json()['marketdata']
        df = pd.DataFrame(resp['data'], columns=resp['columns'])

        if len(df) > 0:
            cap = df.loc[df['BOARDID'] == boardid, 'ISSUECAPITALIZATION'].values[0]
            self.logger.info("Parsed %s capitalization: %s. Success!" % (ticker, cap))
            return cap
        else:
            self.logger.warning(f'Capitalization DF for ticker {ticker} is empty')
            return None

    ## ====== Methods can be called without any token but with a time delay ========
    def history_price_range(self,
                    ticker: str, date_to: datetime.date,
                    date_from: datetime.date=None,
                    columns=['TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'],
                    engine: str='stock',
                    market: str='shares',
                    boardid: str='TQBR',
                    headers: dict=None,
                    trading_session=3,
                    previous_from_date: bool=False
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
            previous_from_date: bool
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

            df = self._history_price_range(ticker, date_to, current_date_from, columns,
                               engine, market, boardid, trading_session, headers)

            if df is not None:
                result_df = pd.concat([result_df, df], ignore_index=True)
            else:
                break

            current_date_from = current_date_from + datetime.timedelta(days=50)

        if len(result_df) < 1:
            return None

        if one_row:
            return result_df.tail(1)

        if previous_from_date and date_from < result_df.loc[0, 'TRADEDATE']:
            date_to = date_from
            date_from = date_from - datetime.timedelta(days=50)
            df = self._history_price_range(ticker, date_to, date_from, columns,
                               engine, market, boardid, trading_session, headers)
            df = df.tail(1)
            result_df = pd.concat([df, result_df], ignore_index=True)

        self.logger.info("Final dataframe for %s from %s to %s: \n%s" %
                     (ticker, _fmt_date(date_from), _fmt_date(date_to), result_df))

        return result_df.reset_index(drop=True)

    def orderbook(self,
                  ticker,
                  depth: int=10,
                  engine: str='stock',
                  market: str='shares',
                  boardid: str='TQBR',
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
            df = orderbook('MOEX', depth=5)
        """
        url = f'https://iss.moex.com/iss/engines/{engine}'
        url += f'/markets/{market}/securities/{ticker}/orderbook.json'

        resp = self._load_url(url)

        if resp is None:
            self.logger.error(f"Failed parsing orderbook for ticker={ticker}")
            return None

        resp = resp.json()['orderbook']

        try:
            df = pd.DataFrame(resp['data'], columns=resp['columns'])
            df = df.loc[df['BOARDID'] == boardid]
            df['UPDATETIME'] = datetime.datetime.now()
            df.reset_index(inplace=True, drop=True)
            df = df.iloc[
                int((df.shape[0] / 2) - depth):
                int(df.shape[0] / 2 + depth)
            ]
            if df.shape[0] == 0:
                self.logger.warning(f'Parsed orderbook for ticker={ticker} is empty')
                return None
            self.logger.debug("%s orderbook:\n%s" % (ticker, df))
            return df
        except:
            self.logger.error("Failed to parse orderbook for %s:\n%s" % (ticker, traceback.format_exc()))
            return None

    def realtime_quotes(self,
            ticker: str,
            engine: str='stock',
            market: str='shares',
            boardid: str='TQBR',
            ) -> pd.DataFrame:
        """
        Returns: DataFrame with single row - latest price and some other data
        specified in {colunms} parameter
        """
        url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}'
        url += f'/securities/{ticker}.json'
        resp = self._load_url(url)

        if resp is None:
            self.logger.error(f"Failed parsing realtime data for ticker={ticker}")
            return None

        resp = resp.json()['marketdata']
        df = pd.DataFrame(resp['data'], columns=resp['columns'])
        self.logger.info("%s realtime quotes:\n%s" % (ticker, df))

        return df.loc[df['BOARDID'] == boardid].reset_index(drop=True)

    def trades(self,
               ticker: str,
               engine: str='stock',
               market: str='shares') -> pd.DataFrame:
        """Returns all trades made during previous trade day"""
        base_url = f'https://iss.moex.com/iss/engines/{engine}'
        base_url += f'/markets/{market}/securities/{ticker}/trades.json'
        additional = ''
        first_trade_num = None
        result_df = None
        while True:
            resp = self._load_url(base_url + additional)
            if resp is None:
                self.logger.error(
                        f'Failed parsing trades for ticker={ticker}' \
                        '\n' + traceback.format_exc())
                return None
            resp = resp.json()['trades']

            if result_df is None:
                result_df = pd.DataFrame(resp['data'], columns=resp['columns'])
            else:
                cur_df = pd.DataFrame(resp['data'], columns=resp['columns'])
                if len(cur_df) == 0 or cur_df.loc[0, 'TRADENO'] == first_trade_num:
                    self.logger.info(f'Parsed trades DF:\n{result_df.head(5)}')
                    return result_df

                result_df = pd.concat([result_df, cur_df], ignore_index=True)
                first_trade_num = result_df.loc[0, 'TRADENO']

            last_trade_no = result_df.loc[len(result_df) - 1, 'TRADENO']
            additional = f'?tradeno={last_trade_no}&next_trade=1'