import quantstats as qs
import pandas as pd
import os
import logging

class BacktestEngine:
    def __init__(self, strategy_returns: pd.Series, benchmark_returns: pd.Series):
        self.strategy_returns = strategy_returns
        self.benchmark_returns = benchmark_returns

    def run_backtest(self, output_dir: str):
        """Execute backtest and save performance report"""
        try:
            # +++ 1. Align time series +++
            common_index = self.strategy_returns.index.intersection(self.benchmark_returns.index)
            if len(common_index) == 0:
                raise ValueError("No common time index between strategy and benchmark returns. Please check data alignment.")
            
            self.strategy_returns = self.strategy_returns.loc[common_index]
            self.benchmark_returns = self.benchmark_returns.loc[common_index]

            # +++ 2. Simulate trading costs (0.1% deduction per rebalancing, assuming monthly rebalancing) +++
            transaction_cost = 0.001  # One-way transaction fee
            self.strategy_returns -= transaction_cost  # Deduct transaction fee at each rebalance

            # +++ 3. Create output directory +++
            os.makedirs(output_dir, exist_ok=True)

            # +++ 4. Compute performance metrics +++
            metrics = qs.reports.metrics(
                returns=self.strategy_returns,
                benchmark=self.benchmark_returns,
                rf=0.02,  # Assume 2% risk-free rate
                display=False,
                compounded=True
            )

            # +++ 5. Save performance report +++
            metrics_file = os.path.join(output_dir, "metrics_report.txt")
            with open(metrics_file, "w", encoding="utf-8") as f:
                f.write("=== Momentum Strategy Performance Report ===\n")
                f.write(metrics.to_string())
            
            logging.info(f"Performance report saved to: {metrics_file}")

        except Exception as e:
            logging.error(f"Backtest execution failed: {str(e)}", exc_info=True)
            raise

    def plot_performance(self, output_dir: str):
        """Generate and save performance charts"""
        try:
            # +++ 1. Return comparison chart +++
            qs.plots.returns(
                self.strategy_returns,
                self.benchmark_returns,
                savefig=os.path.join(output_dir, "returns_comparison.png")
            )

            # +++ 2. Rolling Sharpe ratio +++
            qs.plots.rolling_sharpe(
                self.strategy_returns,
                self.benchmark_returns,
                period=6,  # 6-month rolling window
                savefig=os.path.join(output_dir, "rolling_sharpe.png")
            )

            # +++ 3. Drawdown chart +++
            qs.plots.drawdown(
                self.strategy_returns,
                savefig=os.path.join(output_dir, "drawdown.png")
            )

            logging.info(f"Performance charts saved to: {output_dir}")

        except Exception as e:
            logging.error(f"Failed to generate charts: {str(e)}", exc_info=True)
            raise
