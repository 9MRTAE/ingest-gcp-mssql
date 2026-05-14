from __future__ import annotations

import uuid
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import sqlalchemy as SA
from sqlalchemy.engine import URL
from prefect.logging import get_run_logger

from config import (
    DB_DRIVER,
    DB_HOST,
    DB_PASSWORD,
    DB_PORT,
    DB_TYPE,
    DB_USER,
    DB_HOST_NAME,
    GCP_BUCKET,
    GCP_BUCKET_PARQUET,
    GCSFS,
)


# ── DB connection ─────────────────────────────────────────────────────────────

class ConnectorDB:

    def fn_connect_mssql(self, p_database: str) -> SA.engine.Connection:
        # CHANGE POINT: was fn_connect_mysql() using pymysql + MySQL connection string.
        # Now uses pyodbc + MSSQL (T-SQL) connection via SQLAlchemy URL.create()
        # to safely handle special characters in passwords without manual quoting.
        logger = get_run_logger()
        logger.info('Connect MSSQL source=%s db=%s', DB_HOST_NAME, p_database)
        url = URL.create(
            drivername=f'{DB_TYPE}+{DB_DRIVER}',
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=int(DB_PORT),
            database=p_database,
            query={
                'driver': 'ODBC Driver 17 for SQL Server',
                'TrustServerCertificate': 'yes',
            },
        )
        engine = SA.create_engine(url, fast_executemany=True)
        conn = engine.connect()
        logger.info('Connect MSSQL: Pass')
        return conn


# ── Extract ───────────────────────────────────────────────────────────────────

class ExtractSourceData:

    def __init__(self) -> None:
        self._connector = ConnectorDB()

    def fn_ingest_mssql_gcp(
        self,
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
        # CHANGE POINT: was fn_ingest_mysql_gcp().
        # All SQL rewritten from MySQL dialect → T-SQL:
        #   NOW()                          → GETDATE()
        #   DATE_ADD(x, INTERVAL n HOUR)   → DATEADD(hour, n, x)
        #   DATE_ADD(x, INTERVAL n DAY)    → DATEADD(day, n, x)
        #   IFNULL(a, b)                   → ISNULL(a, b)
        #   FROM_UNIXTIME(col)             → DATEADD(s, col, '19700101')
        #   DATE_FORMAT(x, '%Y-%m-%d')     → CAST(x AS DATE)
        #   YEAR/MONTH/DAY(x)              → same (T-SQL supports these)
        #   INFORMATION_SCHEMA: TABLE_SCHEMA (db) → TABLE_CATALOG (db) + TABLE_SCHEMA (schema)
        #   Integer types: only 'int' → 'int','bigint','smallint','tinyint'
        #   Table quoting: `table` → [schema].[table]
        logger = get_run_logger()
        conn = self._connector.fn_connect_mssql(p_database)
        p_backdate = int(p_backdate)

        try:
            # ── Schema inspection ──────────────────────────────────────────
            # CHANGE POINT: MSSQL INFORMATION_SCHEMA uses TABLE_CATALOG for the
            # database name and TABLE_SCHEMA for the schema (e.g. 'dbo').
            # MySQL uses TABLE_SCHEMA for the database name instead.
            sql_check_datatype = """
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = '{database}'
                  AND TABLE_SCHEMA  = '{schema}'
                  AND TABLE_NAME    = '{tablename}'
            """.format(database=p_database, schema=p_schema, tablename=p_tablename)

            df_check_datatype = pd.read_sql(sql_check_datatype, conn)

            # CHANGE POINT: MSSQL integer types include tinyint (no UNSIGNED equivalent).
            df_datatype_int = df_check_datatype[
                df_check_datatype['DATA_TYPE'].isin(['int', 'bigint', 'smallint', 'tinyint'])
            ]
            df_datatype_bit = df_check_datatype[
                df_check_datatype['DATA_TYPE'].isin(['bit'])
            ]

            # ── Build SQL ──────────────────────────────────────────────────
            # CHANGE POINT: synthetic timestamps use GETDATE() + DATEADD for TZ offset,
            # replacing MySQL's DATE_ADD(NOW(), INTERVAL 7 HOUR).
            _now_local = f'DATEADD(hour, {p_timezone}, GETDATE())'

            if p_query_join:
                sql_select = 'SELECT A.*, B.{createdate}, B.{updatedate} '
                sql_from   = p_query_join
            else:
                if p_createdate and p_updatedate:
                    sql_select = 'SELECT * '
                    sql_from   = 'FROM [{schema}].[{tablename}] AS B '
                else:
                    sql_select = (
                        f'SELECT *'
                        f', {_now_local} AS ftCreateDate'
                        f', {_now_local} AS ftUpdateDate'
                        f', YEAR({_now_local})  AS calendar_year'
                        f', MONTH({_now_local}) AS month_no'
                        f', DAY({_now_local})   AS day_of_month '
                    )
                    sql_from = 'FROM [{schema}].[{tablename}] AS B '

            sql_where = ''
            sql_daily = ''

            if p_createdate and p_updatedate:
                if p_query_join:
                    effective_table = p_tablename_join
                    sql_check_date = """
                        SELECT DATA_TYPE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_CATALOG = '{database}'
                          AND TABLE_SCHEMA  = '{schema}'
                          AND TABLE_NAME    = '{tablename}'
                          AND COLUMN_NAME   = '{createdate}'
                    """.format(
                        database=p_database,
                        schema=p_schema,
                        tablename=effective_table,
                        createdate=p_createdate,
                    )
                    df_check_date = pd.read_sql(sql_check_date, conn)
                    if df_check_date.empty:
                        logger.warning(
                            "Column '%s' not found in [%s].[%s]; falling back to synthetic timestamps",
                            p_createdate, p_schema, effective_table,
                        )
                        p_createdate = ''
                        p_updatedate = ''
                        datetype = None
                    else:
                        datetype = df_check_date['DATA_TYPE'].iloc[0]
                else:
                    df_check_date = df_check_datatype[
                        df_check_datatype['COLUMN_NAME'] == p_createdate
                    ].reset_index(drop=True)
                    if df_check_date.empty:
                        logger.warning(
                            "Column '%s' not found in [%s].[%s]; falling back to synthetic timestamps",
                            p_createdate, p_schema, p_tablename,
                        )
                        p_createdate = ''
                        p_updatedate = ''
                        datetype = None
                    else:
                        datetype = df_check_date['DATA_TYPE'].iloc[0]

                if not (p_createdate and p_updatedate):
                    # Missing partition columns — generate synthetic timestamps via GETDATE().
                    sql_select = (
                        f'SELECT *'
                        f', {_now_local} AS ftCreateDate'
                        f', {_now_local} AS ftUpdateDate'
                        f', YEAR({_now_local})  AS calendar_year'
                        f', MONTH({_now_local}) AS month_no'
                        f', DAY({_now_local})   AS day_of_month '
                    )
                    sql_from = ('FROM [{schema}].[{tablename}] AS B ' if not p_query_join else p_query_join)

                # CHANGE POINT: MSSQL datetime-like types vs int (unix timestamp).
                # MySQL: datetime/timestamp/date/varchar → T-SQL: datetime/datetime2/date/smalldatetime/char/varchar/nvarchar/nchar
                # MySQL: FROM_UNIXTIME(col)              → T-SQL: DATEADD(s, col, '19700101')
                elif datetype in ('datetime', 'datetime2', 'date', 'smalldatetime',
                                  'varchar', 'nvarchar', 'char', 'nchar'):
                    sql_select += ', YEAR(ISNULL(B.{updatedate}, B.{createdate}))  AS calendar_year'
                    sql_select += ', MONTH(ISNULL(B.{updatedate}, B.{createdate})) AS month_no'
                    sql_select += ', DAY(ISNULL(B.{updatedate}, B.{createdate}))   AS day_of_month '
                    sql_where_createdate = 'B.{createdate}'
                    sql_where_updatedate = 'B.{updatedate}'

                elif datetype in ('int', 'bigint', 'smallint', 'tinyint'):
                    # CHANGE POINT: MySQL FROM_UNIXTIME → T-SQL DATEADD(s, col, '19700101')
                    sql_select += ", YEAR(DATEADD(s, ISNULL(B.{updatedate}, B.{createdate}), '19700101'))  AS calendar_year"
                    sql_select += ", MONTH(DATEADD(s, ISNULL(B.{updatedate}, B.{createdate}), '19700101')) AS month_no"
                    sql_select += ", DAY(DATEADD(s, ISNULL(B.{updatedate}, B.{createdate}), '19700101'))   AS day_of_month "
                    sql_where_createdate = "DATEADD(s, B.{createdate}, '19700101')"
                    sql_where_updatedate = "DATEADD(s, B.{updatedate}, '19700101')"

                else:
                    raise TypeError(f'Unsupported datetime datatype for partitioning: {datetype!r}')

                if p_createdate and p_updatedate:
                    # CHANGE POINT: MySQL DATE_FORMAT(..., '%Y-%m-%d') BETWEEN → CAST(... AS DATE) BETWEEN
                    # CHANGE POINT: MySQL DATE_ADD(x, INTERVAL n DAY) → T-SQL DATEADD(day, n, x)
                    _coalesce = f'ISNULL({sql_where_updatedate}, {sql_where_createdate})'
                    _today_local = f'CAST(DATEADD(hour, {p_timezone}, GETDATE()) AS DATE)'

                    if p_startdate and p_enddate:
                        sql_where = 'WHERE '
                        sql_daily = (
                            f"(CAST(DATEADD(hour, {{timezone}}, {_coalesce}) AS DATE)"
                            f" BETWEEN '{{startdate}}' AND '{{enddate}}')"
                        )
                    elif p_backdate < 0:
                        operator = '>' if p_backdate < -1 else '='
                        sql_where = 'WHERE '
                        sql_daily = (
                            f"(CAST(DATEADD(hour, {{timezone}}, {_coalesce}) AS DATE)"
                            f" {operator} DATEADD(day, {{backdate}}, {_today_local}))"
                        )

            # p_where_extra: append an extra AND condition
            # Works for both date-filtered runs (appends after date clause)
            # and full-extract runs (becomes the sole WHERE condition).
            if p_where_extra:
                if sql_where:
                    sql_daily += f' AND ({p_where_extra})'
                else:
                    sql_where = 'WHERE '
                    sql_daily = f'({p_where_extra})'

            sql_full = (sql_select + sql_from + sql_where + sql_daily).format(
                schema=p_schema,
                tablename=p_tablename,
                createdate=p_createdate,
                updatedate=p_updatedate,
                timezone=p_timezone,
                backdate=p_backdate,
                startdate=p_startdate,
                enddate=p_enddate,
            )
            logger.info('Source SQL: %s', sql_full)
            df = pd.read_sql(sql_full, conn)

        finally:
            conn.close()

        logger.info('Extracted %d rows × %d cols from [%s].[%s]', len(df), df.shape[1], p_schema, p_tablename)
        return df, df_datatype_int, df_datatype_bit


# ── Transform ─────────────────────────────────────────────────────────────────

class TransformData:

    def fn_transform_to_string(
        self,
        p_dataframe: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
    ) -> pd.DataFrame:
        logger = get_run_logger()
        logger.info('Start: fn_transform_to_string')

        df_source, df_datatype_int, df_datatype_bit = p_dataframe

        # CHANGE POINT: MSSQL bit columns arrive as Python bool or int (0/1),
        # not as bytes. No ord() or int.from_bytes() needed.
        # Replaced deprecated applymap() with apply() per pandas ≥ 2.1.
        bit_columns = df_datatype_bit['COLUMN_NAME'].tolist() if not df_datatype_bit.empty else []
        int_columns = df_datatype_int['COLUMN_NAME'].tolist() if not df_datatype_int.empty else []

        for col in bit_columns:
            df_source[col] = df_source[col].apply(
                lambda v: np.nan if pd.isna(v) else ('true' if int(v) else 'false')
            )

        if int_columns:
            df_source[int_columns] = df_source[int_columns].astype('Int64')

        partition_cols = {'calendar_year', 'month_no', 'day_of_month'}
        string_cols = [c for c in df_source.columns if c not in partition_cols]
        if string_cols:
            df_source[string_cols] = df_source[string_cols].astype(str)

        df = df_source.copy()
        for null_repr in ('NaT', 'None', 'NaN', 'nan', '<NA>'):
            df = df.replace(null_repr, np.nan)

        if string_cols:
            df[string_cols] = df[string_cols].astype(str)

        df.columns = df.columns.str.lower()
        logger.info('Transform complete: %d rows × %d cols', len(df), df.shape[1])
        return df


# ── Load ──────────────────────────────────────────────────────────────────────

class LoadSourceData:

    def fn_load_to_datalake_gcp(
        self,
        p_tablename: str,
        p_dataframe: pd.DataFrame,
        p_application: str,
    ) -> str:
        """Write partitioned Parquet to GCS.

        GCS path:
            gs://{bucket}/gcp-storage-parquet/{p_application}/{table}/
            calendar_year=YYYY/month_no=M/day_of_month=D/

        Parameters
        ----------
        p_tablename   : source table name (used as GCS folder)
        p_dataframe   : transformed DataFrame
        p_application : GCS_APPLICATION constant from the calling flow (e.g. 'livingmart')
        """
        logger = get_run_logger()
        logger.info('Start: fn_load_to_datalake_gcp')

        target_path = f'gs://{GCP_BUCKET}/{GCP_BUCKET_PARQUET}/{p_application}/{p_tablename}'
        logger.info('Load target: %s', target_path)

        if not p_dataframe.empty:
            logger.info('Rows: %d  Cols: %d', len(p_dataframe), p_dataframe.shape[1])
            ds.write_dataset(
                pa.Table.from_pandas(p_dataframe),
                target_path,
                format='parquet',
                filesystem=GCSFS,
                partitioning_flavor='hive',
                basename_template=f'{uuid.uuid4()}-{{i}}.parquet',
                partitioning=['calendar_year', 'month_no', 'day_of_month'],
                existing_data_behavior='overwrite_or_ignore',
                max_partitions=100_000,
            )
        else:
            logger.warning('Empty DataFrame — skipping GCS write for %s', p_tablename)

        return target_path
