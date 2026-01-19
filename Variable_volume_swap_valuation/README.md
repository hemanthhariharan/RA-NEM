The file 'BGS.ipynb' is illustrative of the typical data wrangling and analysis/visualization process followed for the valuation of deals on the FR desk.

The files 'emtdb_api.py' and 'util.py' are standard interfacing and helper tools developed by the desk to allow analysts to interact with the Energy Marketing and Trading Database (EMTDB).

Forward Prices:

  Since only ON (5x16) and OFF (non-5x16) electricity futures are liquid, the team uses an actuarial approach to convert monthly futures prices to hourly prices.

  Splitters (splitters.py) are used to convert the OFF futures to 2x16 and 7x8 prices, and shapers (Shapers.py) to convert the split prices to a more granular time-block level.

  The splitters use a combination of time decay and Gaussian weighting to eliminate outliers, while the shapers are straight averages of history.

Volatilites:

  The team needs the volatilities at the delivery point for calculating the covariance costs. However, since ON-peak options are liquid only at the trading hub, the team uses an actuarial approach to convert the market-implied ON volatilities to delivery point volatilities using price volatility multipliers (PVMs - pvm.py).

Weather Normalization:

Volumes of electricity consumed are highly correlated with temperature. The weather-normalization model (Weather_Normalization.ipynb) is a multivariate regression model that captures the relationship between temperature and related variables (CDD, HDD, etc.) and normalizes them to forecast volume.

ARR:

In addition, the team also receives credits based on the results of FTR (Financial Transmission Rights) auctions, which are valued using the ARR (Auction Revenue Right - ARR.ipynb) model. The FTRs are essentially an exotic derivative of congestion, and take the form of swaps and options.
