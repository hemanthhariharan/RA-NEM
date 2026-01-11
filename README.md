# RA-NEM
Risk Analytics - NextEra Energy Marketing

The Risk Analytics desk on the Full Requirements team at NextEra Energy Marketing values and hedges short positions in variable-volume swaps.

Since only ON (5x16) and OFF (non-5x16) electricity futures are liquid, the team uses an actuarial approach to convert monthly futures prices to hourly prices.

Splitters are used to convert the OFF futures to 2x16 and 7x8 prices, and shapers to convert the split prices to a more granular time-block level.

The splitters use a combination of time decay and Gaussian weighting to eliminate outliers, while the shapers are straight averages of history.
