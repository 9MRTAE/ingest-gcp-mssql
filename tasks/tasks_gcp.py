from __future__ import annotations

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from tasks.main_components_gcp import ExtractSourceData, LoadSourceData, TransformData


# CHANGE POINT: module-level class instantiation (_extractor, _transformer, _loader)
# has been removed. Instantiating at import time triggers DB connections (via config
# imports) before the Prefect task context exists, causing silent failures and making
# it impossible to run deploy.py with PREFECT_DEPLOY_MODE=1.
# Each task now creates its own instance — lightweight, no side effects at import time.


@task(name='extract', retries=2, retry_delay_seconds=30)
def extract(
    p_database: str,
    p_tablename: str,
    p_createdate: str = 'ftCreateDate',
    p_updatedate: str = 'ftUpdateDate',
    p_backdate: int = -1,
    p_timezone: int = 7,
    p_startdate: str = '',
    p_enddate: str = '',
    p_schema: str = 'dbo',
    p_query_join: str = '',
    p_tablename_join: str = '',
    p_where_extra: str = '',
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger = get_run_logger()
    logger.info('[extract] %s.[%s]', p_database, p_tablename)
    extractor = ExtractSourceData()
    return extractor.fn_ingest_mssql_gcp(
        p_database=p_database,
        p_tablename=p_tablename,
        p_createdate=p_createdate,
        p_updatedate=p_updatedate,
        p_backdate=p_backdate,
        p_timezone=p_timezone,
        p_startdate=p_startdate,
        p_enddate=p_enddate,
        p_schema=p_schema,
        p_query_join=p_query_join,
        p_tablename_join=p_tablename_join,
        p_where_extra=p_where_extra,
    )


@task(name='transform', retries=0)
def transform(
    p_dataframe: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info('[transform] start')
    transformer = TransformData()
    df = transformer.fn_transform_to_string(p_dataframe)
    if not df.empty:
        logger.info('Rows: %d  Cols: %d', len(df), df.shape[1])
        logger.info('\n%s', pd.concat([df.iloc[:1], df.tail(1)]))
    return df


@task(name='load', retries=0)
def load(
    p_tablename: str,
    p_dataframe: pd.DataFrame,
    p_application: str,
) -> str:
    """Load transformed DataFrame to GCS.

    Parameters
    ----------
    p_tablename   : source table name
    p_dataframe   : output of transform()
    p_application : GCS_APPLICATION from the calling flow (e.g. 'livingmart')
    """
    logger = get_run_logger()
    logger.info('[load] %s → GCS application: %s', p_tablename, p_application)
    if not p_dataframe.empty:
        logger.info('Rows: %d  Cols: %d', len(p_dataframe), p_dataframe.shape[1])
        logger.info('\n%s', pd.concat([p_dataframe.iloc[:1], p_dataframe.tail(1)]))
    loader = LoadSourceData()
    result = loader.fn_load_to_datalake_gcp(
        p_tablename=p_tablename,
        p_dataframe=p_dataframe,
        p_application=p_application,
    )
    logger.info('[load] result: %s', result)
    return result
