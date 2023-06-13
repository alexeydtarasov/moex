import pandas as pd
import numpy as np
import datetime

from typing import Union
from sqlalchemy import create_engine
from sqlalchemy.future import Engine


def get_engine(
        host: str = 'localhost',
        database: str = 'tg',
        user: str = 'tg',
        password: str = 'tg',
        port: int = 5432
    ) -> Engine:

    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')


def insert(
        table: str,
        data: Union[pd.DataFrame, pd.Series, dict],
        if_exists: str = 'append',
        index: bool = False
    ):
    logger.info(
        f'Received data to insert into {table}, data=\n{data}'
    )
    
    if isinstance(data, dict):
        data = pd.Series(data)

    if isinstance(data, pd.Series):
        data = pd.DataFrame(data).T

    engine = get_engine()

    data.replace({'Прочерк': np.nan, None: np.nan, '-': np.nan}, inplace=True)
    
    if 'created_at' not in data.columns:
        data['created_at'] = datetime.datetime.now()
        
    target_columns = pd.read_sql(
        f'select * from {table} limit 1;', engine
    ).columns.tolist()

    # if isinstance(data, pd.DataFrame):
    #     data_columns = data.columns.tolist()
    # else:
    #     data_columns = data.index.tolist()

    # for col in target_columns:
    #     if col != 'created_at' and not col.endswith('_id') and col not in data_columns:
    #         data[col] = np.nan

    for col in data.columns.tolist():
        if col not in target_columns:
            data.drop(columns=[col, ], inplace=True)

    try:
        data.to_sql(
            table, engine,
            if_exists=if_exists, index=index
        )
    except:
        logger.error(
            f'Failed to load into table={table} data=\n{data}',
            exc_info=True
        )


def select(query: str) -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql(query, engine).replace({None: np.nan})


def update(query: str):
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(query)
    except:
        logger.error(
            f'Failed to execute query=\n{query}',
            exc_info=True
        )

