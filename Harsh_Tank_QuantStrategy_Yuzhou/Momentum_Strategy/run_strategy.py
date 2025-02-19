import yaml
import logging
from datetime import datetime
import pandas as pd
from data_loader import DataLoader
from feature_engineering import FeatureEngine
from ml_pipe_line import MLPipeline
from backtesting import BacktestEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("strategy.log"), logging.StreamHandler()]
)

def main():
    """Main process: Data Loading -> Feature Engineering -> Model Training -> Backtesting"""
    try:
        # === 1. Load Configuration File ===
        config_path = r"C:\Users\Wyuzh\Desktop\Capstone\biasafe\Yuzhou_Wang\Harsh_Tank_QuantStrategy_Yuzhou\Momentum_Strategy\config.yaml"
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        # Parse date parameters
        start_date = datetime.strptime(config["data"]["start_date"], "%Y-%m-%d")
        end_date = datetime.strptime(config["data"]["end_date"], "%Y-%m-%d")
        
        # === 2. Data Loading and Survivorship Bias Correction ===
        logging.info("Step 1/4: Loading S&P 500 constituent data...")
        data_loader = DataLoader()
        sp500_symbols, added_stocks, removed_stocks = data_loader.get_SP_500_constituents_data(start_date, end_date)
        
        if len(sp500_symbols) == 0:
            raise ValueError("No valid S&P 500 constituent list retrieved.")
        
        # === 3. Feature Engineering ===
        logging.info("Step 2/4: Handling survivorship bias and downloading price data...")
        feature_engine = FeatureEngine(sp500_symbols, added_stocks, removed_stocks)
        price_data = feature_engine.remove_survivorship_bias(start_date, end_date)
        
        # === 4. Model Training and Strategy Returns Calculation ===
        logging.info("Step 3/4: Calculating momentum strategy returns and training CAPM model...")
        ml_pipeline = MLPipeline(price_data)
        merged_data = ml_pipeline.prepare_training_data()
        alpha, beta, model_summary = ml_pipeline.train_model(merged_data)
        logging.info(f"Model training results:\n{model_summary}")
        
        # === 5. Backtesting and Performance Evaluation ===
        logging.info("Step 4/4: Running backtest and generating reports...")
        strategy_returns = merged_data["Momentum"]
        benchmark_returns = merged_data["S&P_500"]
        
        backtester = BacktestEngine(strategy_returns, benchmark_returns)
        backtester.run_backtest("Output")
        backtester.plot_performance()
        logging.info("Backtest completed! Results saved in the Output/ directory.")
        
    except Exception as e:
        logging.error(f"Strategy execution failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
