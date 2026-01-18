The Risk Analytics desk on the Full Requirements team at NextEra Energy Marketing values and hedges short positions in variable-volume swaps. Since there is optionality embedded into an exotic swap of this nature, the team works with the futures and the options desk for the forward prices and the volatilities, respectively.

Forward Prices:

  Since only ON (5x16) and OFF (non-5x16) electricity futures are liquid, the team uses an actuarial approach to convert monthly futures prices to hourly prices.

  Splitters are used to convert the OFF futures to 2x16 and 7x8 prices, and shapers to convert the split prices to a more granular time-block level.

  The splitters use a combination of time decay and Gaussian weighting to eliminate outliers, while the shapers are straight averages of history.

Volatilites:

  The team needs the volatilities at the delivery point for calculating the covariance costs. However, since ON-peak options are liquid only at the trading hub, the team uses an actuarial approach to convert the market-implied ON volatilities to delivery point volatilities using price volatility multipliers (PVMs).

In addition, the team also receives credits based on the results of FTR (Financial Transmission Rights) auctions, which are valued using the ARR (Auction Revenue Right) model. The FTRs are essentially an exotic derivative of congestion, and take the form of swaps and options.
