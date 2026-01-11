import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple

# project code
from util import EmtdbConnection, date_hour_to_peak_block, list_peak_blocks, get_price_peak_map, spring_dst, fall_dst, hourly_index, convert_lmps_tz
from emtdb_api import pull_lmp_data, pull_fwd_market_price

SUPPORTED_ISO_PNODES = {
    'SPP': 'SPPNORTH_HUB', 'ERCOT': 'HB_NORTH', 'MISO': 'INDIANA.HUB', 'ISONE': '4000', 'PJM': '51288'
}

# the first contract in the list is the on-peak backbone
ISO_TO_FWD_MARKET_PRICE_BACKBONE = {
    'PJM': ['PJM-ON', 'PJM-OFF'],
    'ISONE': ['NEPOOLMAHUB-ON', 'NEPOOLMAHUB-OFF'],
    'MISO': ['MISO-INDIANA.HUB-DA-5X16', 'MISO-INDIANA.HUB-DA-OFF'],
    'CAISO': ['SP15-ON', 'SP15-OFF'],
    'SPP': ['SPP-SPPNORTH_HUB-DA-5X16', 'SPP-SPPNORTH_HUB-DA-OFF'],
    'ERCOT': ['ERCOT-ON', 'ERCOT-OFF', 'ERCOT-2X16', 'ERCOT-7X8', 'ERCOT-7X24'],
}

def _get_cash_vol(emtdb: EmtdbConnection, iso: str, pnode_id: str, start_dt: str, end_dt: str,
                  zero_mean: bool) -> Optional[pd.DataFrame]:
    """
    Computes realized cash volatility for day-ahead LMPs, assuming an annual basis of 360 days

    Args:
        emtdb: EMTDB connection
        iso: ISO, e.g. 'ISONE'
        pnode_id: Pricing node ID, e.g. '4001'
        start_dt: Start date, e.g. '2023-10-01'
        end_dt: End date, e.g. '2024-06-30'
        zero_mean: Flag for the assumption E[log LMP returns]=0

    Returns: pd.DataFrame
        columns = Peak blocks
        rows = Month end dates over the time period
    """

    if iso == 'MISO':
        # convert MISO LMPs from EST to EPT
        df_lmp = convert_lmps_tz(
        emtdb=emtdb,
        pnode_id=pnode_id,
        df_lmp = pull_lmp_data(
        emtdb=emtdb,
        pnode_id=pnode_id,
        da_or_rt='DA',
        start_dt=start_dt,
        end_dt=end_dt
    ).reset_index(),
    convert_from='EST',
    convert_to='EPT'
    ).set_index(['Date', 'Hour'])
    else:
        df_lmp = pull_lmp_data(emtdb=emtdb, pnode_id=pnode_id, da_or_rt='DA', start_dt=start_dt, end_dt=end_dt)

    if len(df_lmp) == 0:
        print(f'missing LMPs: {pnode_id}')
        return

    df_lmp['Peak Block'] = df_lmp.index.map(lambda x: date_hour_to_peak_block(date=x[0], hour=x[1], iso=iso))
    df_lmp = df_lmp.groupby(['Date', 'Peak Block'])['Price'].mean().unstack()

    df_cash_vol = pd.DataFrame(columns=list_peak_blocks(iso=iso), index=pd.date_range(start_dt, end_dt, freq='ME')) # Only cash vols for complete months are calculated

    for peak_block in list_peak_blocks(iso=iso):
        daily_prices = df_lmp[peak_block].dropna() # Dropping NaNs can lead to significantly different results
        dt = (daily_prices.index[1:] - daily_prices.index[:-1]).days / 360  # annualized
        daily_returns = np.log(daily_prices).diff().iloc[1:] / np.sqrt(dt)
        if zero_mean:
            monthly_vols = np.sqrt((daily_returns ** 2).groupby(pd.Grouper(freq='ME')).mean())
        else:
            monthly_vols = daily_returns.groupby(pd.Grouper(freq='ME')).std()
        df_cash_vol[peak_block] = monthly_vols

    return df_cash_vol

def get_cash_pvm(emtdb: EmtdbConnection, iso: str, pnode_id: str, start_dt: str, end_dt: str, zero_mean: bool,
                 q_upper: float) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Computes cash PVMs for a given pnode

    Args:
        emtdb: EMTDB connection
        iso: ISO, e.g. 'ISONE'
        pnode_id: Pricing node ID, e.g. '4001'
        start_dt: Start date, e.g. '2023-10-01'
        end_dt: End date, e.g. '2024-06-30'
        zero_mean: Flag for the assumption E[LMP returns]=0
        q_upper: Upper quantile of PVMs to clip (between 0 and 1, methodology default = 1)

    Returns: dictionary of "Node" or "Hub" to pd.DataFrame
        columns = Peak blocks
        rows = Months 1-12 and "Avg"
    """
    assert 0 < q_upper <= 1
    if iso not in SUPPORTED_ISO_PNODES.keys():
        print(f'unsupported ISO: {iso}')
        return

    # calculate cash vol for each historical month
    node_cash_vol = _get_cash_vol(emtdb, iso, pnode_id, start_dt, end_dt, zero_mean)
    if node_cash_vol is None:
        return None

    hub_pnode_id = SUPPORTED_ISO_PNODES[iso]
    hub_cash_vol = _get_cash_vol(emtdb, iso, hub_pnode_id, start_dt, end_dt, zero_mean)

    # calculate price vol multiplier for each historical month
    node_pvm = node_cash_vol.div(hub_cash_vol, axis=0)  # nodal pvm = node cash vol / hub cash vol
    hub_pvm = hub_cash_vol.div(hub_cash_vol['5x16'], axis=0)  # hub pvm = hub cash vol / hub 5x16 cash vol

    # clip upper quantile of multipliers across columns (peaks)
    node_pvm = node_pvm.clip(upper=node_pvm.quantile(q=q_upper), axis=1)
    hub_pvm = hub_pvm.clip(upper=hub_pvm.quantile(q=q_upper), axis=1)

    # take average for each month 1-12
    node_pvm_averages = node_pvm.groupby(node_pvm.index.month).mean()
    hub_pvm_averages = hub_pvm.groupby(hub_pvm.index.month).mean()

    # insert average across all months
    node_pvm_averages.loc['Avg', :] = node_pvm_averages.mean(axis=0)
    hub_pvm_averages.loc['Avg', :] = hub_pvm_averages.mean(axis=0)

    return {'Node': node_pvm_averages, 'Hub': hub_pvm_averages}

def get_all_zone_and_hub_cash_pvm(emtdb: EmtdbConnection, start_dt: str, end_dt: str, zero_mean: bool,
                                  q_upper: float) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Computes cash PVMs all major zones and hubs (as defined in "get_price_peak_map")

    Args:
        emtdb: EMTDB connection
        start_dt: Start date, e.g. '2023-10-01'
        end_dt: End date, e.g. '2024-06-30'
        zero_mean: Flag for the assumption E[LMP returns]=0
        q_upper: Upper quantile of PVMs to clip (between 0 and 1, methodology default = 1)

    Returns: dictionary of ISO to dictionary of zone name to pd.DataFrame
        columns = Peak blocks
        rows = Months 1-12 and "Avg"
    """
    pvm = {}
    price_peak_map = get_price_peak_map()

    for _, row in price_peak_map.iterrows():
        iso = row['General']['ISO']
        if iso not in SUPPORTED_ISO_PNODES.keys():
            print(f'unsupported ISO: {iso}')
            continue
        if iso not in pvm.keys():
            pvm[iso] = {}

        name = row['General']['Name']
        pnode_id = row['RISKDB.MARKET_PRICE_DATA']['Node ID']
        vol_backbone = row['RISKDB.FWD_MARKET_PRICE']['Vol Backbone']

        cash_pvm = get_cash_pvm(emtdb, iso, pnode_id, start_dt, end_dt, zero_mean, q_upper)
        pvm[iso][name] = cash_pvm['Hub'] if vol_backbone else cash_pvm['Node']

    return pvm
