import pandas as pd
import numpy as np

# project code
from util import EmtdbConnection, timer_func


@timer_func
def pull_lmp_data(emtdb: EmtdbConnection, pnode_id: str, da_or_rt: str, start_dt: str, end_dt: str,
                  price_data_type: str = 'PRICE') -> pd.DataFrame:
    """
    Pulls LMP data from RISKDB.MARKET_PRICE_DATA

    Args:
        emtdb: EMTDB connection
        pnode_id: Pricing node ID, e.g. '51288'
        da_or_rt: Day-Ahead (DA) or Real-Time (RT), eg. 'DA'
        start_dt: First LMP date, e.g. '2024-03-01'
        end_dt: Last LMP date, e.g. '2024-06-30'
        price_data_type: Component of LMP to pull, e.g. 'PRICE', 'CONGESTION', 'LOSS'

    Returns: pd.DataFrame
        columns = ('Price')
        index names = ('Date', 'Hour')
    """
    start_dt = pd.to_datetime(start_dt).date()
    end_dt = pd.to_datetime(end_dt).date()
    print(f"Pulling {da_or_rt} LMP: pnode={pnode_id}, start={start_dt}, end={end_dt}...")

    qry = f"""
        SELECT "PRICE_DATE" as "Date", "HOUR"/100 as "Hour", "PRICE" as "Price"
        FROM RISKDB.MARKET_PRICE_DATA
        WHERE "PRICE_DATE" >= :start_dt
        AND "PRICE_DATE" <= :end_dt
        AND "LOCATION_4" = :pnode_id
        AND "PRICE_TYPE" = :da_or_rt
        AND "PRICE_DATA_TYPE" =: price_data_type
        ORDER BY "PRICE_DATE", "HOUR"
    """

    params = {
        'start_dt': start_dt,
        'end_dt': end_dt,
        'pnode_id': str(pnode_id),
        'da_or_rt': str(da_or_rt),
        'price_data_type': price_data_type
    }

    df = emtdb.execute(qry=qry, params=params)
    df = df.set_index(['Date', 'Hour'])

    return df


@timer_func
def pull_m2m_shaper_vw(emtdb: EmtdbConnection, pnode_id: str, eval_dt: str, is_hourly: bool) -> pd.DataFrame:
    """
    Pulls system shaper from RISKDB.M2M_SHAPERS_VW

    Args:
        emtdb: EMTDB connection
        pnode_id: Pricing node ID, e.g. '51288'
        eval_dt: Evaluation date, e.g. '2024-07-10'
        is_hourly: Hourly vs. time-block flag

    Returns: pd.DataFrame
        column names = ('Peak Block', 'Hour')
        index = Months 1-12
    """

    print(f"Pulling System Shaper: pnode={pnode_id}, eval_dt={eval_dt}, hourly={is_hourly}...")

    # EMTDB stores shapers according to a month start date
    eval_dt = pd.to_datetime(eval_dt)
    month_start_dt = eval_dt if eval_dt.is_month_start else eval_dt - pd.offsets.MonthBegin()

    qry = f"""
        SELECT PRICE_SHAPER, END_EFFECTIVE_DATE, HOUR_TYPE as "Peak Block", MONTH as "Month", BLOCK as "Hour"
        FROM RISKDB.M2M_SHAPERS_VW
        WHERE BASIS_NODEID = :pnode_id
        AND SHAPER_TYPE = :shaper_type
        AND START_EFFECTIVE_DATE = :effective_dt
        AND END_EFFECTIVE_DATE >= START_EFFECTIVE_DATE
        AND PRICE_TYPE = 'DA'
        AND HOUR_TYPE IN ('5x16','2x16','7x8','6x16','1x16')
    """

    params = {
        'effective_dt': month_start_dt.date(),
        'pnode_id': str(pnode_id),
        'shaper_type': 'HOURLY' if is_hourly else 'BLOCK',
    }

    df = emtdb.execute(qry=qry, params=params)
    assert len(df['END_EFFECTIVE_DATE'].unique()) == 1  # extra check to make sure shaper is unique

    if is_hourly:
        df['Hour'] = df['Hour'].astype(int)  # convert hourly string to integer if using hours
    df = df.pivot(index='Month', columns=['Peak Block', 'Hour'], values='PRICE_SHAPER').sort_index(axis=1)
    return df


@timer_func
def pull_m2m_price_vol_multiplier(emtdb: EmtdbConnection, eval_dt: str) -> pd.DataFrame:
    """
    Pulls system PVMs directly from EMTDB

    Args:
        emtdb: EMTDB connection
        eval_dt: Evaluation date, e.g. '2024-07-10'

    Returns: pd.DataFrame
        columns = (ISO, Zone, Peak Block, Contract Month, Multiplier, START_EFFECTIVE_DATE, END_EFFECTIVE_DATE, MODIFY_DATE)
    """
    qry = f"""
         SELECT
            ac.ISO_ID AS "ISO",
            ic.NAME AS "Zone",
            htd.HOUR_TYPE AS "Peak Block",
            mmap.CONTRACT_MONTH AS "Contract Month",
            mmap.MID_VALUE AS "Multiplier",
            mmap.START_EFFECTIVE_DATE,
            mmap.END_EFFECTIVE_DATE ,
            mmap.MODIFY_DATE
        FROM 
            RISKDB.M2M_ANCILLARY_PRICES mmap, 
            RAFR.ANCILLARIES a,
            RAFR.ANCILLARY_CURVES ac,
            RAFR.ISO_CLASSIFICATIONS ic,
            PHOENIX.HOUR_TYPE_DETAILS htd 
        WHERE
            a.NAME = 'Zonal Vol Multiplier'
            AND a.ID = ac.ANCILLARY_ID
            AND ac.ID = mmap.ANCILLARY_CURVE_ID
            AND ic.ID = ac.ISO_CLASSIFICATION_ID
            AND htd.ID = a.HOUR_TYPE_ID
            AND a.ISO_ID in (1, 2, 3, 5, 8, 11)
            AND mmap.START_EFFECTIVE_DATE <= :effective_dt
            AND mmap.END_EFFECTIVE_DATE >= :effective_dt
            AND mmap.CONTRACT_MONTH > :contract_month
        ORDER BY
            MMAP.CONTRACT_MONTH,
            ic.ISO_ID,
            ic.NAME,
            htd.HOUR_TYPE
    """
    params = {
        'effective_dt': pd.to_datetime(eval_dt).date(),
        'contract_month': pd.to_datetime(eval_dt).strftime('%Y%m')
    }
    df = emtdb.execute(qry=qry, params=params)
    return df


def pull_projection_curves(emtdb: EmtdbConnection, cd: str, bp: str, start_dt: str, end_dt: str,
                           first_contract_month: str, last_contract_month: str) -> pd.DataFrame:
    """
    Pulls data from RISKDB.PROJECTION_CURVES

    Args:
        emtdb: EMTDB connection
        cd: Commodity code, e.g. 'PJM-WESTERN HUB-5x16', 'PJM-WESTERN HUB-7x24'
        bp: Basis point code, e.g. 'PJM-PECO_RESID_AGG-5x16', 'PJM-BGE-7x24'
        start_dt: First trade date, e.g. '2024-03-01'
        end_dt: Last trade date, e.g. '2024-06-30'
        first_contract_month: First contract month, e.g. '202502'
        last_contract_month: Last contract month, e.g. '202503'

    Returns: pd.DataFrame
        columns = (EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH, PROJ_LOC_AMT, PROJ_BASIS_AMT)
    """
    qry = f"""
        SELECT EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH, PROJ_LOC_AMT, PROJ_BASIS_AMT
        FROM RISKDB.PROJECTION_CURVES
        WHERE BASIS_POINT = :bp
        AND COMMODITY = :cd
        AND EFFECTIVE_DATE BETWEEN :start_dt AND :end_dt
        AND CONTRACT_MONTH BETWEEN :first_contract_month AND :last_contract_month
        ORDER BY EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH
    """
    params = {
        'bp': bp, 'cd': cd, 'start_dt': pd.to_datetime(start_dt).date(), 'end_dt': pd.to_datetime(end_dt).date(),
        'first_contract_month': first_contract_month, 'last_contract_month': last_contract_month
    }
    df = emtdb.execute(qry=qry, params=params)
    return df


def pull_fwd_market_price(emtdb: EmtdbConnection, cd: str, bp: str, start_dt: str, end_dt: str,
                          first_contract_month: str, last_contract_month: str) -> pd.DataFrame:
    """
    Pulls data from RISKDB.FWD_MARKET_PRICE

    Args:
        emtdb: EMTDB connection
        cd: Commodity code, e.g. 'PJM-ON'
        bp: Basis point code, e.g. 'PJM-ON'
        start_dt: First trade date, e.g. '2024-03-01'
        end_dt: Last trade date, e.g. '2024-06-30'
        first_contract_month: First contract month, e.g. '202502'
        last_contract_month: Last contract month, e.g. '202503'

    Returns: pd.DataFrame
        columns = (EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH, FIXED_AMOUNT, BASIS_AMOUNT)

    """
    qry = f"""
        SELECT EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH, FIXED_AMOUNT, BASIS_AMOUNT
        FROM RISKDB.FWD_MARKET_PRICE
        WHERE BASIS_POINT = :bp
        AND COMMODITY = :cd
        AND EFFECTIVE_DATE BETWEEN :start_dt AND :end_dt
        AND CONTRACT_MONTH BETWEEN :first_contract_month AND :last_contract_month
        ORDER BY EFFECTIVE_DATE, COMMODITY, BASIS_POINT, CONTRACT_MONTH
    """
    params = {
        'bp': bp, 'cd': cd, 'start_dt': pd.to_datetime(start_dt).date(), 'end_dt': pd.to_datetime(end_dt).date(),
        'first_contract_month': first_contract_month, 'last_contract_month': last_contract_month
    }
    df = emtdb.execute(qry=qry, params=params)
    return df


def pull_discount_factors(
        emtdb: EmtdbConnection,
        effective_dt: str,
        first_contract_month: str,
        last_contract_month: str,
        credit_rating: str = "BBB+",
) -> pd.Series:
    """
    Pulls rate and credit spread data from RISKDB.YIELD_CURVE and PHOENIX.CREDIT_CURVES

    Args:
        emtdb: EMTDB connection
        effective_dt: Effective date, e.g. '2024-07-10'
        first_contract_month: First contract month, e.g. '202502'
        last_contract_month: Last contract month, e.g. '202503'
        credit_rating: Credit rating, e.g. 'BBB+'

    Returns: pd.DataFrame
        columns = (RfRate, CreditSpread, DiscountFactor)
        index names = (Year, Month)
    """
    qry = """\
        SELECT
            yc.EFFECTIVE_DATE as "Effective Date",
            yc.CONTRACT_MONTH as "Contract Month",
            yc.ZERO_COUPON_YIELD_RATE as "RF Rate",
            cc.BOND_SPREAD as "Credit Spread"
        FROM
            RISKDB.YIELD_CURVE yc
        LEFT JOIN
            PHOENIX.CREDIT_CURVES cc
        ON yc.EFFECTIVE_DATE = cc.EFFECTIVE_DATE AND yc.CONTRACT_MONTH = cc.CONTRACT_MONTH
        WHERE yc.EFFECTIVE_DATE = :effective_dt
        AND yc.CONTRACT_MONTH >= :first_contract_month
        AND yc.CONTRACT_MONTH <= :last_contract_month
        AND cc.RATING_SYSTEM = 'SNP18'
        AND cc.RATING = :credit_rating
        ORDER BY yc.CONTRACT_MONTH
    """
    params = {
        "effective_dt": pd.to_datetime(effective_dt).date(),
        "first_contract_month": first_contract_month,
        "last_contract_month": last_contract_month,
        "credit_rating": credit_rating,
    }

    df = emtdb.execute(qry=qry, params=params)

    df['Contract Start'] = df['Contract Month'].map(
        lambda x: pd.Timestamp(year=int(str(x)[:4]), month=int(str(x)[4:]), day=1))
    df['Contract End'] = df['Contract Start'] + pd.offsets.MonthEnd()
    df["r"] = df["RF Rate"] + df["Credit Spread"]
    df["T-t"] = (df['Contract End'] - df['Effective Date']).dt.days / 365
    df["Discount Factor"] = np.exp(-df["r"] * df["T-t"])
    return df[['Contract Month', 'RF Rate', 'Credit Spread', 'Discount Factor']]


@timer_func
def pull_2x16_splitter(emtdb: EmtdbConnection, hub_id: str, eval_dt: str) -> pd.DataFrame:
    """
    Pulls system splitters from RISKDB.BASIS_PROJ_BKBONE_MULTIPLIERS

    Args:
        emtdb: EMTDB connection
        hub_id: Hub ID, e.g INDRT, SPP_S, PJM, NIHUB, INDDA, NYJ, NYA, ARKRT, MAHUB, ADHUB, NYG, ARKDA
        eval_dt: Evaluation date, e.g. '2024-07-10'

    Returns: pd.DataFrame
        column names = ('Splitter')
        index = Months 1-12
    """

    print(f"Pulling System Splitter: hub_id={hub_id}, eval_dt={eval_dt}")

    eval_dt = pd.to_datetime(eval_dt)

    qry = f"""
        SELECT MONTH as "Month", HISTORIC_2x16_MULTIPLIER as "2x16"
        FROM RISKDB.BASIS_PROJ_BKBONE_MULTIPLIERS
        WHERE BEG_EFFECTIVE_DATE = :effective_dt
        AND END_EFFECTIVE_DATE >= BEG_EFFECTIVE_DATE
        AND NUCLEUS_POWER_POOL = :hub_id
    """

    params = {
        'effective_dt': eval_dt.date(),
        'hub_id': str(hub_id),
    }

    df = emtdb.execute(qry=qry, params=params)
    df.set_index('Month', inplace=True)

    return df


@timer_func
def pull_system_vols(emtdb: EmtdbConnection, cd: str, eval_dt: str, first_contract_month: str,
                     last_contract_month: str) -> pd.DataFrame:
    """
    Pulls system vols from RISKDB.FWD_MARKET_VOLATILITY

    Args:
        emtdb: EMTDB connection
        commodity: Commodity ID, e.g PJM-ON
        eval_dt: Evaluation date, e.g. '2024-07-10'
        first_contract_month: First contract month, e.g. '202502'
        last_contract_month: Last contract month, e.g. '202503'

    Returns: pd.DataFrame
        columns = (Monthly Volatility, Daily Volatility)
        index = Contract Month
    """

    print(f"Pulling System Vols: commodity={cd}, eval_dt={eval_dt}")

    eval_dt = pd.to_datetime(eval_dt)

    qry = f"""
        SELECT EFFECTIVE_DATE, CONTRACT_MONTH, MONTHLY_VOLATILITY, DAILY_VOLATILITY
        FROM RISKDB.FWD_MARKET_VOLATILITY
        WHERE "COMMODITY" =: cd
        AND CONTRACT_MONTH BETWEEN :first_contract_month AND :last_contract_month
        AND "EFFECTIVE_DATE" >= :effective_date
    """

    params = {
        'cd': cd,
        'first_contract_month': first_contract_month,
        'last_contract_month': last_contract_month,
        'effective_date': pd.to_datetime(eval_dt).date()
    }

    df = emtdb.execute(qry=qry, params=params)
    df.set_index('CONTRACT_MONTH', inplace=True)

    return df
