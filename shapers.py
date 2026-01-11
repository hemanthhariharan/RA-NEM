import pandas as pd

# project code
from util import EmtdbConnection, date_hour_to_peak_block, spring_dst, fall_dst, hourly_index, convert_lmps_tz
from emtdb_api import pull_lmp_data

SUPPORTED_ISOS = ('SPP', 'CAISO', 'MISO', 'ISONE', 'PJM')


def pull_lmp_and_calc_shaper(emtdb: EmtdbConnection, iso: str, pnode_id: str, eval_dt: str, is_hourly: bool,
                             lookback_yrs: int = 2, clip_quantile: float = 1):
    """    Computes historical day-ahead LMP shaper over the given period
    Args:        
        emtdb: EMTDB connection
        iso: ISO, e.g. 'PJM'
        pnode_id: Pricing node ID, e.g. '51288'
        eval_dt: Evaluation date, e.g. '2024-07-10'
        is_hourly: Hourly vs. time-block flag
        lookback_yrs: Number of years of historical data (default methodology = 2)
        clip_quantile: Upper quantile of LMP values to clip (default methodology = 1, or no clipping)
    Returns: pd.DataFrame
    column names = ('Peak Block', 'Hour')
    index = Months 1-12
    """
    assert iso in SUPPORTED_ISOS
    assert 0 < clip_quantile <= 1
    assert lookback_yrs >= 1

    if not is_hourly:
        assert iso != 'CAISO'  # not currently supported
    # use the past 'lookback_years' years of data up to the most recent month-end
    end_dt = pd.to_datetime(eval_dt)
    if not end_dt.is_month_end:
        end_dt -= pd.offsets.MonthEnd()
    start_dt = end_dt - pd.offsets.MonthBegin(12 * lookback_yrs)
    if iso == 'MISO':

        # convert MISO LMPs from EST to EPT. Sometimes for MISO, we convert LMPs to CPT and use hours 7-22 as the peak.
        # If that is the case,change the convert_to to 'CPT' and as a hack, change the iso in date_hour_to_peak_block
        # to 'SPP'
        df_lmp = convert_lmps_tz(pull_lmp_data(
            emtdb=emtdb,
            pnode_id=pnode_id,
            da_or_rt='DA',
            start_dt=start_dt,
            end_dt=end_dt
        ).reset_index(),
                                 convert_from='EST',
                                 convert_to='EPT'
                                 )
    else:

        # pull data from EMTDB - shapers calculated using DA LMPs
        df_lmp = pull_lmp_data(emtdb=emtdb, pnode_id=pnode_id, da_or_rt='DA', start_dt=start_dt,
                               end_dt=end_dt).reset_index()

    # post-processing
    df_lmp['Price'] = df_lmp['Price'].clip(upper=df_lmp['Price'].quantile(clip_quantile, interpolation='higher'))
    df_lmp['Month'] = df_lmp['Date'].dt.month
    df_lmp['Peak Block'] = df_lmp.apply(
        lambda x: date_hour_to_peak_block(date=x['Date'], hour=x['Hour'], iso=iso), axis=1)

    # calculate shaper
    avg_hourly = df_lmp.groupby(['Month', 'Peak Block', 'Hour'])['Price'].mean()
    avg_peak_block = df_lmp.groupby(['Month', 'Peak Block'])['Price'].mean()

    shaper = avg_hourly / avg_peak_block

    shaper = shaper.unstack(['Peak Block', 'Hour']).sort_index(axis=1)

    # calculate time-block shaper if flagged

    if not is_hourly:
        data = {('5x16', 'WD_1'): shaper['5x16'].iloc[:, 0:4].mean(axis=1),
                ('5x16', 'WD_2'): shaper['5x16'].iloc[:, 4:8].mean(axis=1),
                ('5x16', 'WD_3'): shaper['5x16'].iloc[:, 8:12].mean(axis=1),
                ('5x16', 'WD_4'): shaper['5x16'].iloc[:, 12:16].mean(axis=1),
                ('2x16', 'WE_1'): shaper['2x16'].iloc[:, 0:4].mean(axis=1),
                ('2x16', 'WE_2'): shaper['2x16'].iloc[:, 4:8].mean(axis=1),
                ('2x16', 'WE_3'): shaper['2x16'].iloc[:, 8:12].mean(axis=1),
                ('2x16', 'WE_4'): shaper['2x16'].iloc[:, 12:16].mean(axis=1),
                ('7x8', 'WN_1'): shaper['7x8'].mean(axis=1)}

        shaper = pd.DataFrame(data).sort_index(axis=1)

    return shaper
