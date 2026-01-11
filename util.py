import pandas as pd
import oracledb
from time import time
from functools import lru_cache
from typing import List, Iterable, Tuple, Callable, Optional, Generator

oracledb.init_oracle_client()  # enable thick mode


def timer_func(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        t0 = time()
        res = func(*args, **kwargs)
        t1 = time()
        print(f'Function {func.__name__!r} executed in {round(t1 - t0, 1)} sec')
        return res

    return wrapper


def get_price_peak_map() -> pd.DataFrame:
    file_name = r'K:\Valuation\_Analysts\JordanK\Price Peak Map.xlsx'
    df = pd.read_excel(file_name, sheet_name='Price Map', header=[0, 1])
    return df


class EmtdbConnection:
    def __init__(self, user: str, pw: str):
        print('connecting to EMTDB...')
        host = "emtdbdb_aws.neeaws.local"
        port = 1721
        sid = 'EMTDB'
        self._con = oracledb.connect(user=user, password=pw, dsn=oracledb.makedsn(host=host, port=port, sid=sid))
        print('connected.')

    def __del__(self):
        self._con.close()

    @timer_func
    def execute(self, qry: str, params: dict, array_size: int = 100000) -> pd.DataFrame:
        with self._con.cursor() as crsr:
            crsr.arraysize = array_size
            crsr.prefetchrows = array_size + 1
            crsr.execute(statement=qry, parameters=params)
            columns = [x[0] for x in crsr.description]
            records = crsr.fetchall()
        df = pd.DataFrame.from_records(records, columns=columns)
        return df


def hourly_index(start_dt: str, end_dt: str) -> pd.MultiIndex:
    return pd.MultiIndex.from_product(
        [
            pd.date_range(start_dt, end_dt, freq="D", name="Date"),
            pd.Series(range(1, 25), name="Hour"),
        ]
    )


def parameterize_sql_list(items: Iterable) -> Tuple[str]:
    return tuple(f'{x}' for x in items)


def chunker(seq: List, size: int) -> Generator:
    # chunks a list "seq" into smaller lists of length at most "size"
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def list_peak_blocks(iso: str) -> List[str]:
    if iso == 'CAISO':
        return ['6x16-Weekday', '6x16-Saturday', 'Off-Sunday', 'Off-Night']
    if iso in ('PJM', 'ISONE', 'MISO', 'ERCOT', 'SPP'):
        return ['5x16', '2x16', '7x8']
    else:
        raise Exception(f'ISO not recognized: {iso}')


def date_hour_to_peak_block(date: pd.Timestamp, hour: int, iso: str) -> str:
    assert iso in ('PJM', 'ISONE', 'NYISO', 'MISO', 'ERCOT', 'SPP', 'CAISO')
    """
    LMPs from EMTDB come in the following time-zones:
        PJM / ISONE / NYISO      - EPT
        MISO                     - EST (** no DST adjustment)
        ERCOT / SPP              - CPT
        CAISO                    - PPT

    Traded contracts are defined in the following time-zones:
        PJM / ISONE / MISO      - EPT (HE 8-23 for On-Peak 5x16)
        ERCOT / SPP             - CPT (HE 7-22 for On-Peak 5x16)
        CAISO                   - PPT (HE 7-22 for On-Peak 6x16)
    """
    is_holiday = date in get_holidays(date.year)
    is_saturday = date.dayofweek == 5
    is_sunday = date.dayofweek == 6

    if iso in ('PJM', 'ISONE', 'NYISO', 'MISO'):
        is_night = not (8 <= hour <= 23)
        is_off_peak = is_holiday or is_saturday or is_sunday or is_night
        return '5x16' if not is_off_peak else ('7x8' if is_night else '2x16')
    elif iso in ('ERCOT', 'SPP'):
        is_night = not (7 <= hour <= 22)
        is_off_peak = is_holiday or is_saturday or is_sunday or is_night
        return '5x16' if not is_off_peak else ('7x8' if is_night else '2x16')
    elif iso == 'CAISO':
        is_night = not (7 <= hour <= 22)
        is_off_peak = is_holiday or is_sunday or is_night
        if is_off_peak:
            if is_night:
                return 'Off-Night'
            else:
                return 'Off-Sunday'
        else:
            if is_saturday:
                return '6x16-Saturday'
            else:
                return '6x16-Weekday'
    else:
        raise Exception(f'ISO not recognized: {iso}')


def date_hour_to_time_block(date: pd.Timestamp, hour: int, iso: str) -> str:
    assert iso in ('PJM', 'ISONE', 'NYISO', 'MISO', 'ERCOT', 'SPP', 'CAISO')

    is_holiday = date in get_holidays(date.year)
    is_saturday = date.dayofweek == 5
    is_sunday = date.dayofweek == 6

    if iso in ('PJM', 'ISONE', 'NYISO', 'MISO'):

        if is_holiday or is_saturday or is_sunday:
            if hour in list(range(8, 12)):
                return 'WE_1'
            elif hour in list(range(12, 16)):
                return 'WE_2'
            elif hour in list(range(16, 20)):
                return 'WE_3'
            elif hour in list(range(20, 24)):
                return 'WE_4'
            else:
                return 'WE_N'

        else:
            if hour in list(range(8, 12)):
                return 'WD_1'
            elif hour in list(range(12, 16)):
                return 'WD_2'
            elif hour in list(range(16, 20)):
                return 'WD_3'
            elif hour in list(range(20, 24)):
                return 'WD_4'
            else:
                return 'WD_N'

    elif iso in ('ERCOT'):

        if is_holiday or is_saturday or is_sunday:
            if hour in list(range(7, 11)):
                return 'WE_1'
            elif hour in list(range(11, 15)):
                return 'WE_2'
            elif hour in list(range(15, 19)):
                return 'WE_3'
            elif hour in list(range(19, 23)):
                return 'WE_4'
            else:
                return 'WE_N'

        else:
            if hour in list(range(7, 11)):
                return 'WD_1'
            elif hour in list(range(11, 15)):
                return 'WD_2'
            elif hour in list(range(15, 19)):
                return 'WD_3'
            elif hour in list(range(19, 23)):
                return 'WD_4'
            else:
                return 'WD_N'
    else:
        raise Exception(f'ISO not recognized: {iso}')


def peak_block_to_traded_peak(peak_block: str, iso: str) -> str:
    # takes as input the output of "date_hour_to_peak_block"
    if iso in ('PJM', 'ISONE', 'MISO', 'ERCOT', 'SPP'):
        lookup = {'5x16': '5x16', '2x16': 'Off', '7x8': 'Off'}
        return lookup[peak_block]
    elif iso == 'CAISO':
        return peak_block.split('-')[0]
    else:
        raise Exception(f'ISO not recognized: {iso}')


def peak_block_to_complement(peak_block: str) -> Optional[str]:
    # takes as input the output of "date_hour_to_peak_block"
    if peak_block == '5x16':
        return None
    if peak_block == '7x8':
        return '2x16'
    if peak_block == '2x16':
        return '7x8'
    if peak_block == 'Off-Night':
        return 'Off-Sunday'
    if peak_block == 'Off-Sunday':
        return 'Off-Night'
    if peak_block == '6x16-Saturday':
        return '6x16-Weekday'
    if peak_block == '6x16-Weekday':
        return '6x16-Saturday'
    else:
        raise Exception(f'Peak Block not recognized: {peak_block}')


def convert_lmps_tz(df_lmp: pd.DataFrame, convert_from: str, convert_to: str):
    """
    Converts df_lmp in EST to CPT

    Args:
        df_lmp: pd.DataFrame containing df_lmp with integer indices and columns 'Date', 'Hour', 'Price'. Naive dates and Hours are assigned the convert_from timezone.
        convert_from: Timezone to convert from e.g. 'EST'
        convert_to: Timezone to convert to e.g. 'EPT' or 'CPT'

    Returns: pd.Dataframe containing df_lmp in EPT in the same format as df_lmp. Note that this does not handle duplicates or missing hours.
    """

    # Decreasing hours by 1 to convert from 1 through 24 format to 0 through 23 that Pandas works in
    df_lmp.loc[:, 'Hour'] -= 1

    # Combining Date and Hour to get a timestamp
    df_lmp['DateTime'] = df_lmp['Date'] + pd.to_timedelta(df_lmp['Hour'], unit='h')

    # Discarding other columns
    df_lmp = df_lmp[['DateTime', 'Price']]

    df_lmp.set_index('DateTime', inplace=True)

    # Localizing naive timestamps to the convert_from timezone
    df_lmp.index = df_lmp.index.tz_localize(convert_from)

    # Converting to the convert_to
    if convert_to == 'CPT':
        df_lmp.index = df_lmp.index.tz_convert('US/Central')
    elif convert_to == 'EPT':
        df_lmp.index = df_lmp.index.tz_convert('US/Eastern')

    df_lmp.reset_index(inplace=True)

    df_lmp['Date'] = pd.to_datetime(df_lmp['DateTime'].dt.date)
    df_lmp['Hour'] = df_lmp['DateTime'].dt.hour

    # Increasing hours by 1 to convert from 0 through 23 format to 1 through 24 that we're used to
    df_lmp.loc[:, 'Hour'] += 1

    # Discarding 'DateTime' column
    df_lmp = df_lmp[['Date', 'Hour', 'Price']]

    return df_lmp


def get_holidays(year: int) -> List[pd.Timestamp]:
    # source: https://www.naesb.org//pdf/weq_iiptf050504w6.pdf
    return [
        _labor_day(year),
        _memorial_day(year),
        _independence_day(year),
        _new_years_day(year),
        _thanksgiving_day(year),
        _christmas_day(year),
    ]


@lru_cache()
def spring_dst(year: int) -> pd.Timestamp:
    # Spring DST = 2nd Sun of Mar
    dt = pd.Timestamp(year=year, month=3, day=1)
    while dt.weekday() != 6:
        dt += pd.Timedelta(days=1)
    dt += pd.Timedelta(days=7)
    return dt


@lru_cache()
def fall_dst(year: int) -> pd.Timestamp:
    # Fall DST = 1st Sun of Nov
    dt = pd.Timestamp(year=year, month=11, day=1)
    while dt.weekday() != 6:
        dt += pd.Timedelta(days=1)
    return dt


@lru_cache()
def _memorial_day(year: int) -> pd.Timestamp:
    # Last Mon of May
    dt = pd.Timestamp(year=year, month=5, day=31)
    while dt.weekday() != 0:
        dt -= pd.Timedelta(days=1)
    return dt


@lru_cache()
def _labor_day(year: int) -> pd.Timestamp:
    # 1st Mon of Sept
    dt = pd.Timestamp(year=year, month=9, day=1)
    while dt.weekday() != 0:
        dt += pd.Timedelta(days=1)
    return dt


@lru_cache()
def _thanksgiving_day(year: int) -> pd.Timestamp:
    # 4th Thur of Nov
    count = 0
    dt = pd.Timestamp(year=year, month=11, day=1)
    while count < 4:
        if dt.weekday() == 3:
            count += 1
        dt += pd.Timedelta(days=1)
    return dt - pd.Timedelta(days=1)


@lru_cache()
def _new_years_day(year: int) -> pd.Timestamp:
    dt = pd.Timestamp(year=year, month=1, day=1)
    if dt.weekday() == 6:  # if sunday, observe following day
        dt += pd.Timedelta(days=1)
    return dt


@lru_cache()
def _independence_day(year: int) -> pd.Timestamp:
    dt = pd.Timestamp(year=year, month=7, day=4)
    if dt.weekday() == 6:  # if sunday, observe following day
        dt += pd.Timedelta(days=1)
    return dt


@lru_cache()
def _christmas_day(year: int) -> pd.Timestamp:
    dt = pd.Timestamp(year=year, month=12, day=25)
    if dt.weekday() == 6:  # if sunday, observe following day
        dt += pd.Timedelta(days=1)
    return dt
