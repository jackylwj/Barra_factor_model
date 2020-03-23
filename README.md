# Barra_factor_model
This rep is a simple attempt of Application of Barra Risk Model on China A share Market.
##1.Data preparation
The buildbd.py can automatically download style factor data, including market cap, according to Barra CNE5 model from JoinQuant API and then build four databases. The first three databases are for style factors, market data and industry data respectively. The last database is a merged database of the above order by date.

## 1.Introduction to Barra Model
According to CNE5, the factor excess returns over the period are obtained via a cross_sectional regression of asset excess returns on their associated factor exposures. Factor returns are estimated using weighted least-squares regression, assuming that the variance of specific returns is inversely proportional to the square root of total market capitalization. In order to avoid multicollinearity, Restriction is added that the industry cap-weighted sum of factor returns is zero.

In order to faciliate the verification of the solution, I solved the weight matrix of pure factor portfolios instead of the factor returns themselves. The solution can be checked by examining the exposure of each factor portfolios where style factors portfolio should have unit exposure on the respective factor and no exposure on the others.

## 2.Results
The factor_portfolio_exposure.csv is a single check of solution on a single day of 2019-12-25 on CSI500 constituents from which we can easily find the characteristics of pure factor portfolio exposure. The sw_l1.csv is actually Shenwan industry classification standard, emm... this file should be introduced in part2 but I forget, anyway.

The style_factors_cum.png is the daily cummulative returns of the 10 style factors from 2019-01-01 to 2019-12-31 with style_factors_cum.csv as the original dataset.

## 3.Further exploration
The value of Barra Factor Model lies on volatility forecast and portfolio risk attribution. There is a lot to talk such as risk adjustment of covariance of factor return. This rep is just a start and will be continually updated. 

## 4.How to use this rep
- Enter the database path and run builddb.py
- run the Barra_Model.py

