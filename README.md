# RA-NEM
Risk Analytics - NextEra Energy Marketing

The Risk Analytics desk on the Full Requirements team at NextEra Energy Marketing values and hedges short positions in variable-volume swaps. Since there is optionality embedded into an exotic swap of this nature, the team works with the futures and the options desk for the forward prices and the volatilities, respectively.

The folder 'Variable_volume_valuation' contains the code used for pricing the swap, with more details on the use of various statistical methods.

'Geometric_brownian_motion.ipynb' contains implementations of correlated Geometric Brownian motion, empirical calculations of volatility and correlations, functions to value exotic options (Asian and lookback), and the development of a systematic methodology to price variable-volume swaps by expressing volume as a function of price.

'Options_valuation.ipynb' contains implementations of functions for modeling European and American options (binomial tree), variable-volume swaps Greeks, Geometric Brownian Motion, VaR etc.


