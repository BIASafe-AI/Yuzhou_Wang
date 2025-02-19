import pandas as pd
import numpy as np
import statsmodels.api as sm
from pandas_datareader import data as pdr
import logging
from datetime import datetime
import yfinance as yf

class MLPipeline:
    def __init__(self, df):
        self.df = df

    def top_performers(self, date, mtl_12, mtl_ret):
        """Select the top N monthly stocks and calculate next month's returns"""
        all_ = mtl_12.loc[date]
        valid_stocks = all_.dropna()  # Remove missing values
        
        # +++ Error handling: If fewer than 5 valid stocks exist, select available ones +++
        n = min(5, len(valid_stocks))
        if n == 0:
            return np.nan
        top = valid_stocks.nlargest(n)
        try:
            # Retrieve next month's returns (avoiding look-ahead bias)
            next_month = mtl_ret.loc[date + pd.DateOffset(months=1), top.index]
            return next_month.mean()
        except KeyError:
            logging.warning(f"No data for next month after {date}")
            return np.nan

    def prepare_training_data(self):
        """Generate training data (momentum strategy returns vs benchmark returns)"""
        # +++ Calculate returns and handle missing values +++
        ret_df = self.df.pct_change(fill_method=None)
        mtl_ret = (ret_df.fillna(0) + 1).resample('ME').prod()  # Set suspended stock returns to 0
        mtl_12 = mtl_ret.rolling(12, min_periods=12).apply(np.prod).dropna()  # Rolling 12-month returns

        # +++ Retrieve risk-free rate (FRED API) +++
        try:
            risk_free = pdr.get_data_fred('DGS1', mtl_12.index[0], mtl_12.index[-1])
            risk_free_monthly = (1 + risk_free / 100) ** (1 / 12) - 1  # Convert annualized to monthly compound rate
        except Exception as e:
            logging.error(f"Failed to fetch risk-free rate: {e}")
            risk_free_monthly = pd.Series(0, index=mtl_12.index)  # Fallback handling

        # +++ Generate strategy return series +++
        strategy_returns = []
        for date in mtl_12.index[:-1]:  # Avoid using future data for the last month
            ret = self.top_performers(date, mtl_12, mtl_ret)
            strategy_returns.append(ret)
        
        strategy_returns = pd.Series(strategy_returns, index=mtl_12.index[:-1])
        strategy_returns.name = 'Momentum'

        # +++ Retrieve benchmark returns (S&P 500) +++
        sp500 = yf.download('^GSPC', start=self.df.index[0], end=self.df.index[-1])['Close']
        sp500_returns = sp500.pct_change().resample('ME').last().dropna()
        sp500_returns.name = 'S&P_500'

        # +++ Merge data and align time index +++
        merged_data = pd.merge(
            strategy_returns.to_frame(),
            sp500_returns.to_frame(),
            left_index=True,
            right_index=True,
            how='inner'
        )
        merged_data['risk_free'] = risk_free_monthly.reindex(merged_data.index, method='ffill')
        return merged_data.dropna()

    def calculate_alpha_beta(self, merged_data):
        """Calculate Alpha and Beta using the CAPM model"""
        # +++ Calculate risk premium +++
        merged_data['rs_rf'] = merged_data['Momentum'] - merged_data['risk_free']
        merged_data['rm_rf'] = merged_data['S&P_500'] - merged_data['risk_free']

        # +++ OLS regression +++
        X = sm.add_constant(merged_data['rm_rf'])
        y = merged_data['rs_rf']
        model = sm.OLS(y, X)
        results = model.fit()
        
        # +++ Extract results +++
        alpha = results.params['const'] * 100  # Monthly Alpha (percentage)
        beta = results.params['rm_rf']
        return alpha, beta, results.summary()

    def train_model(self, merged_data):
        """Complete training process"""
        alpha, beta, summary = self.calculate_alpha_beta(merged_data)
        logging.info(f"Alpha: {alpha:.2f}% (monthly), Beta: {beta:.2f}")
        return alpha, beta, summary
