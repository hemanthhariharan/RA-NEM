import pandas as pd

# project code
from util import EmtdbConnection, date_hour_to_peak_block, spring_dst, fall_dst, hourly_index, peak_block_to_traded_peak, get_holidays, convert_lmps_tz
from emtdb_api import pull_lmp_data
import numpy as np
from scipy.stats import norm

SUPPORTED_ISOS = ('SPP', 'CAISO', 'MISO', 'ISONE', 'PJM')

# helper function to calculate the number of months a given observation's month is from the month for which the splitter is to be calculated

def months_away(kernel_months, splitter_month):
    months_away_abs = np.abs(kernel_months - splitter_month)
    return np.where(months_away_abs <= 6, months_away_abs, 12 - months_away_abs)

def pull_lmp_and_calc_splitter(emtdb: EmtdbConnection, iso: str, pnode_id: str, eval_dt: str,
                               lookback_yrs: int = 2, clip_quantile: float = 1):
    """    Computes historical day-ahead LMP shaper over the given period
    Args:
        emtdb: EMTDB connection
        iso: ISO, e.g. 'PJM'
        pnode_id: Pricing node ID, e.g. '51288'
        eval_dt: Evaluation date, e.g. '2024-07-10'
        lookback_yrs: Number of years of historical data (default methodology = 2). To be precise, this is exactly 2 years for shapers but between 24 and 25 months for splitters
        clip_quantile: Upper quantile of LMP values to clip (default methodology = 1, or no clipping)
    Returns: pd.DataFrame
    column names = ('2x16') splitters
    index = Months 1-12
    """
    assert iso in SUPPORTED_ISOS
    assert 0 < clip_quantile <= 1
    assert lookback_yrs >= 1
    # use the past 'lookback_years' years of data up to the most recent month-end
    end_dt = pd.to_datetime(eval_dt)
    previous_month_end = end_dt - pd.offsets.MonthEnd()
    start_dt = previous_month_end - pd.offsets.MonthBegin(12 * lookback_yrs)
    start_dt_one_day_before = start_dt - pd.Timedelta(days=1) # this is for MISO where we may need to fill a missing hour

    # pull data from EMTDB - splitters calculated using DA LMPs
    df_lmp = pull_lmp_data(emtdb=emtdb, pnode_id=pnode_id, da_or_rt='DA', start_dt=start_dt, end_dt=end_dt).reset_index()

    if iso == 'MISO':
        # convert MISO LMPs from EST to EPT. Sometimes for MISO, we convert LMPs to CPT and use hours 7-22 as the peak. If that is the case,
        # change the convert_to to 'CPT' and as a hack, change the iso in date_hour_to_peak_block and peak_block_traded_block to 'SPP'
        df_lmp = convert_lmps_tz(
            df_lmp = pull_lmp_data(
                emtdb=emtdb,
                pnode_id=pnode_id,
                da_or_rt='DA',
                start_dt=start_dt,
                end_dt=end_dt
            ).reset_index(),
            convert_from='EST',
            convert_to='EPT'
        ) #.reset_index()

    # post-processing
        df_lmp['Price'] = df_lmp['Price'].clip(upper=df_lmp['Price'].quantile(clip_quantile, interpolation='higher'))
        df_lmp['Month'] = df_lmp['Date'].dt.month
        df_lmp['Peak Block'] = df_lmp.apply(
            lambda x: date_hour_to_peak_block(date=x['Date'], hour=x['Hour'], iso=iso), axis=1
        )
        df_lmp['5x16 / Off'] = df_lmp.apply(
            lambda x: peak_block_to_traded_peak(peak_block=x['Peak Block'], iso=iso), axis=1
        )
    # Aggregating the LMPs at the daily level to calculate splitters
        df_daily = df_lmp[['Date','Month']].drop_duplicates()
    # merging with off prices
        df_daily = df_daily.merge(
            df_lmp.pivot_table(
                index='Date',
                columns='5x16 / Off',
                values='Price',
                aggfunc='mean'
            ),
            how='left',
            on='Date',
            validate='1:1'
        ).drop(['5x16'], axis=1)

    # merging with 2x16 prices
        df_daily = df_daily.merge(
            df_lmp.pivot_table(
                index='Date',
                columns='Peak Block',
                values='Price',
                aggfunc='mean'
            ),
            how='left',
            on='Date',
            validate='1:1'
        ).drop(['5x16', '7x8'], axis=1)

    # calculating decay factors
        df_daily['Decay Factor'] = df_daily.apply(
            lambda x: 0.5 ** ((np.abs(pd.to_datetime(eval_dt) - x['Date']) / pd.Timedelta(days=365)) + 1), axis=1
        )
    # Since off-peak days (weekends/holidays) have 24 off-peak hours while non-off peak days have only 8 off-peak hours, we weight them accordingly
        df_daily['Off peak day weight'] = df_daily.apply(
            lambda x: 1 if (x['Date'] in get_holidays(x['Date'].year) or x['Date'].dayofweek in [5, 6]) else 1 / 3, axis=1
        )
    # months 1 through 12 to calculate kernel weights
        kernel_months = np.arange(1, 13)
    # months 1 through 12 to calculate splitters
        splitter_months = np.arange(1, 13)
    # Dictionary to store splitters
        splitter_dict = {}
    # Defined in risk methodology paper
        bm = 0.5
    # Looping through each month to calculate splitters
        for splitter_month in splitter_months:
            months_away_arr = months_away(kernel_months=kernel_months, splitter_month=splitter_month)
            kernel_weights = norm.pdf(months_away_arr / bm)
            kernel_weights_df = pd.DataFrame(
                {
                    'Month': kernel_months,
                    'Weights': kernel_weights
                }
            ).set_index('Month')

            # mapping from kernel weight dataframe

            df_daily['Kernel Weight'] = df_daily.apply(
                lambda row: kernel_weights_df.iloc[row['Month'] - 1, 0], axis=1
            )
            df_daily['2x16 weight'] = df_daily['Kernel Weight'] * df_daily['Decay Factor']
            df_daily['Off peak weight'] = df_daily['Kernel Weight'] * df_daily['Decay Factor'] * df_daily['Off peak day weight']
            df_daily['2x16 weighted price'] = df_daily['2x16'] * df_daily['2x16 weight']
            df_daily['Off weighted price'] = df_daily['Off'] * df_daily['Off peak weight']

            # Weighted-average 2x16 price
            avg_2x16 = df_daily['2x16 weighted price'].sum() / df_daily['2x16 weight'][df_daily['2x16'].notnull()].sum()

            # Weighted-average off price
            avg_off = df_daily['Off weighted price'].sum() / df_daily['Off peak weight'].sum()

            # By definition of splitter
            splitter_dict[int(splitter_month)] = avg_2x16 / avg_off

    # Converting splitter dictionary to dataframe and renaming index and columns
    calc_splitters = pd.DataFrame.from_dict(
        splitter_dict,
        orient='index')

    calc_splitters.index.rename('Month', inplace=True)
    calc_splitters.rename(columns={0: '2x16'}, inplace=True)

    return calc_splitters
