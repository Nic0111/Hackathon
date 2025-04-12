import pandas as pd
import requests
import ccxt
from datetime import datetime, timezone
import time
import os
import json

# --- 1. Configuration ---
CONFIG_FILE = 'api_config.json'

def load_config(config_file):
    """Loads API keys and settings from a JSON file."""
    if not os.path.exists(config_file):
        print(f"Error: Config file '{config_file}' not found.")
        print("Please create it with your API keys, e.g.,")
        print("""
        {
            "binance": {"apiKey": "YOUR_BINANCE_KEY", "secret": "YOUR_BINANCE_SECRET"},
            "glassnode": {"apiKey": "YOUR_GLASSNODE_KEY"},
            "target_symbols": ["BTC/USDT", "ETH/USDT"],
            "target_timeframes": ["1h", "4h"],
            "start_date_str": "2022-01-01T00:00:00Z"
            // Add other settings as needed
        }
        """)
        return None
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        return None

config = load_config(CONFIG_FILE)
if config is None:
    exit() # Stop if config loading failed

# --- 2. Data Fetching Functions ---

# --- 2a. Exchange Market Data (using CCXT) ---
def fetch_exchange_ohlcv(exchange_id, symbol, timeframe, since_dt=None, end_dt=None, limit=1000):
    """Fetches OHLCV data from an exchange using CCXT between since_dt and end_dt."""
    exchange_config = config.get(exchange_id, {})
    all_ohlcv = [] # List to hold all fetched candles

    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class(exchange_config)

        if not exchange.has['fetchOHLCV']:
            print(f"Exchange {exchange_id} does not support fetchOHLCV.")
            return None

        # Convert start/end dates to milliseconds
        since_ms = int(since_dt.timestamp() * 1000) if since_dt else None
        end_ms = int(end_dt.timestamp() * 1000) if end_dt else None

        # CCXT helper to get timeframe duration in milliseconds
        timeframe_duration_ms = exchange.parse_timeframe(timeframe) * 1000

        current_since_ms = since_ms
        print(f"Fetching {symbol} {timeframe} from {exchange_id} since {since_dt} until {end_dt}")

        while True:
            # Be polite to the API
            try:
                # Use exchange.rateLimit property for sleep duration
                # Add a small buffer, e.g., 10%
                sleep_duration = (exchange.rateLimit / 1000) * 1.1
                # print(f"Sleeping for {sleep_duration:.2f} seconds...") # Optional debug print
                time.sleep(sleep_duration)
            except Exception as e:
                print(f"Rate limit info potentially unavailable for {exchange_id}, using default sleep: {e}")
                time.sleep(1) # Default sleep if rateLimit not available

            print(f" > Fetching chunk starting from {pd.to_datetime(current_since_ms, unit='ms', utc=True)}")
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=current_since_ms, limit=limit)

            if not ohlcv: # No more data returned
                print(" > No more data returned by exchange.")
                break

            first_candle_ts = ohlcv[0][0]
            last_candle_ts = ohlcv[-1][0]
            print(f" > Fetched {len(ohlcv)} records. First: {pd.to_datetime(first_candle_ts, unit='ms', utc=True)}, Last: {pd.to_datetime(last_candle_ts, unit='ms', utc=True)}")

            all_ohlcv.extend(ohlcv)

            # Prepare for the next iteration
            next_since_ms = last_candle_ts + timeframe_duration_ms
            current_since_ms = next_since_ms # Move to the next chunk

            # Check stopping conditions
            # 1. If the last candle fetched is already beyond our desired end time
            if end_ms is not None and last_candle_ts >= end_ms:
                print(" > Reached or passed end date.")
                break
            # 2. If the exchange returned fewer candles than the limit (likely hit the end of available data)
            if len(ohlcv) < limit:
                print(" > Fetched less than limit, assuming end of available data.")
                break
            # 3. Safety break: If the next fetch start time hasn't advanced (avoid infinite loop on strange data)
            if current_since_ms <= last_candle_ts:
                print(f"Warning: Next fetch time {current_since_ms} is not after last candle time {last_candle_ts}. Stopping fetch.")
                break


        if not all_ohlcv:
            print(f"No data fetched for {symbol} {timeframe} in the specified range.")
            return pd.DataFrame()

        # --- Process the combined data ---
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        # Remove duplicates just in case the API overlapped fetches slightly
        df.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        df = df.astype(float)

        # --- Filter results explicitly to the requested range ---
        if since_dt:
            df = df[df.index >= since_dt]
        if end_dt:
            df = df[df.index <= end_dt]

        # Add identifiers
        df['exchange'] = exchange_id
        df['symbol'] = symbol
        df['timeframe'] = timeframe

        print(f"Fetched total {len(df)} final records for {symbol} {timeframe} from {exchange_id}")
        return df

    except ccxt.AuthenticationError as e:
        print(f"Authentication Error with {exchange_id}: {e}")
        return None
    except ccxt.NetworkError as e:
        print(f"Network Error fetching from {exchange_id}: {e}")
        return None
    except ccxt.ExchangeError as e:
        print(f"Exchange Error fetching {symbol} from {exchange_id}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred fetching from {exchange_id}: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for unexpected errors
        return None

# --- 2b. Placeholder for Derivatives Data (e.g., Funding Rates) ---
def fetch_funding_rates(exchange_id, symbol, since_dt=None, limit=100):
    """Placeholder: Fetches funding rate data (implementation depends on exchange/ccxt support)."""
    # Example using CCXT if supported (check specific exchange docs in CCXT)
    exchange_config = config.get(exchange_id, {})
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class(exchange_config)

        if not exchange.has.get('fetchFundingRateHistory'): # Check specific capability
            print(f"Exchange {exchange_id} does not support fetchFundingRateHistory via CCXT (or requires specific params).")
            # Alternative: Use direct API call with 'requests' if needed
            return None

        since_ms = int(since_dt.timestamp() * 1000) if since_dt else None
        # Note: Params might differ per exchange (symbol format etc.)
        funding_data = exchange.fetch_funding_rate_history(symbol, since=since_ms, limit=limit)

        if not funding_data: return pd.DataFrame()

        df = pd.DataFrame(funding_data)
        # Standardize columns (CCXT aims for this, but double-check)
        # Expected columns often include 'symbol', 'timestamp', 'fundingRate'
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df.set_index('timestamp', inplace=True)
        # Select/rename relevant columns
        df = df[['fundingRate']] # Adjust column name if needed
        df = df.astype(float)
        df['exchange'] = exchange_id
        df['symbol'] = symbol # Original symbol used in call
        print(f"Fetched {len(df)} funding rate records for {symbol} from {exchange_id}")
        return df

    except Exception as e:
        print(f"Error fetching funding rates from {exchange_id}: {e}")
        return None

# --- 2c. Placeholder for On-Chain Data (e.g., Glassnode using requests) ---
def fetch_onchain_metric(provider, metric_path, asset, params={}):
    """Placeholder: Fetches a specific metric from an on-chain provider API."""
    if provider == 'glassnode':
        api_key = config.get('glassnode', {}).get('apiKey')
        if not api_key:
            print("Glassnode API key not found in config.")
            return None

        base_url = "https://api.glassnode.com/v1/metrics/"
        url = f"{base_url}{metric_path}"

        # Common parameters (check Glassnode docs)
        default_params = {'a': asset, 'api_key': api_key}
        all_params = {**default_params, **params} # Merge default and specific params

        try:
            response = requests.get(url, params=all_params)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()

            if not data: return pd.DataFrame()

            df = pd.DataFrame(data)
            # Glassnode often returns 't' (timestamp) and 'v' (value)
            df['timestamp'] = pd.to_datetime(df['t'], unit='s', utc=True) # Timestamps are often seconds
            df.set_index('timestamp', inplace=True)
            df = df[['v']] # Select the value column
            df.rename(columns={'v': f"{provider}_{asset}_{metric_path.replace('/', '_')}"}, inplace=True) # Rename for clarity
            df = df.astype(float)
            print(f"Fetched {len(df)} records for {metric_path} ({asset}) from {provider}")
            return df

        except requests.exceptions.RequestException as e:
            print(f"Error fetching from {provider}: {e}")
            return None
        except Exception as e:
            print(f"Error processing {provider} data: {e}")
            return None
    else:
        print(f"Provider {provider} not implemented.")
        return None

# --- 3. Data Processing and Combining ---

def combine_dataframes(data_dict, target_freq):
    """
    Combines multiple DataFrames based on timestamp.

    Args:
        data_dict (dict): Dictionary where keys are descriptive names and values are DataFrames.
                          All DataFrames MUST have a DatetimeIndex (UTC recommended).
        target_freq (str): The target frequency for the final DataFrame (e.g., '1h', '4h', '1d').
                           All data will be aligned to this frequency.

    Returns:
        pandas.DataFrame: A single DataFrame with combined data, or None if error.
    """
    if not data_dict:
        print("No dataframes provided to combine.")
        return None

    print(f"\nCombining dataframes to target frequency: {target_freq}")

    # Find a reference DataFrame (e.g., the first OHLCV data for the primary symbol/exchange)
    # Or explicitly define one based on your prediction target
    ref_df = next(iter(data_dict.values()), None)
    if ref_df is None or ref_df.empty:
        # Try finding the first non-empty df as reference
        ref_df = next((df for df in data_dict.values() if not df.empty), None)
        if ref_df is None:
             print("Cannot determine reference dataframe (all input dataframes might be empty).")
             return None


    # --- Alignment Strategy: Resample/Reindex + Forward Fill ---
    # Create the target index based on the reference df range and target frequency
    try:
        min_ts = min(df.index.min() for df in data_dict.values() if not df.empty)
        max_ts = max(df.index.max() for df in data_dict.values() if not df.empty)
        target_index = pd.date_range(start=min_ts, end=max_ts, freq=target_freq, tz='UTC')
    except ValueError:
        print("Error creating target index. Ensure input dataframes are not empty and have valid timestamps.")
        return None


    final_df = pd.DataFrame(index=target_index)

    for name, df in data_dict.items():
        if df.empty:
            print(f"Skipping empty dataframe: {name}")
            continue

        # --- CORRECTED TIMEZONE CHECK ---
        # Check if index is timezone-aware and if it is UTC
        if df.index.tz is None or df.index.tz != timezone.utc:
             print(f"Warning: DataFrame '{name}' index is naive or not UTC ({df.index.tz}). Attempting conversion to UTC.")
             try:
                 if df.index.tz is None:
                      # Localize naive index to UTC
                      df.index = df.index.tz_localize('UTC')
                 else:
                      # Convert existing timezone to UTC
                      df.index = df.index.tz_convert('UTC')
                 print(f"Successfully converted index for '{name}' to UTC.")
             except Exception as e:
                 print(f"Error converting index timezone for '{name}': {e}. Skipping this dataframe.")
                 continue
        # --- END CORRECTION ---

        # Reindex to the target frequency, forward filling missing values
        # This ensures that at each target timestamp, we have the most recent data point
        # from the source dataframe `df`.
        try:
            # Important: Ensure the index is sorted before reindexing/ffill
            if not df.index.is_monotonic_increasing:
                print(f"Warning: Index for '{name}' is not sorted. Sorting...")
                df = df.sort_index()

            df_resampled = df.reindex(target_index, method='ffill')

            # Add prefix/suffix to column names to avoid collisions
            df_resampled = df_resampled.add_prefix(f"{name}_")

            # Merge into the final dataframe
            final_df = final_df.join(df_resampled, how='left') # Use left join to keep all target timestamps
        except Exception as e:
            print(f"Error processing or joining dataframe '{name}': {e}")
            continue # Skip this dataframe if processing fails


    # Initial rows might still have NaNs if data sources start later than target_index.min()
    # Or if ffill couldn't fill them (e.g., target_index starts before the first data point in df)
    initial_len = len(final_df)
    final_df.dropna(axis=0, how='any', inplace=True) # Drop rows with ANY NaNs after merge
    print(f"Dropped {initial_len - len(final_df)} rows with NaNs after merging.")

    # Remove columns that might have been added during processing but are not features
    # (like 'exchange', 'symbol', 'timeframe' if they were prefixed)
    cols_to_drop = [c for c in final_df.columns if c.endswith(('_exchange', '_symbol', '_timeframe'))]
    final_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')


    if final_df.empty:
        print("Final combined dataframe is empty after processing and dropping NaNs.")
        return None

    print(f"Final combined dataframe shape: {final_df.shape}")
    return final_df

# --- 4. Main Execution Logic ---
if __name__ == "__main__":

    all_fetched_data = {} # Dictionary to store raw dataframes

    start_dt = pd.to_datetime(config['start_date'], utc=True)
    end_dt = pd.to_datetime(config['end_date'], utc=True) if config.get('end_date') else None

    # --- Fetch Market Data ---
    for symbol in config['target_symbols']:
        for tf in config['target_timeframes']:
            # Example: Fetch from Binance
            binance_key = f"binance_{symbol.replace('/', '')}_{tf}_ohlcv"
            df_binance = fetch_exchange_ohlcv('binance', symbol, tf, since_dt=start_dt, end_dt=end_dt)
            if df_binance is not None and not df_binance.empty:
                all_fetched_data[binance_key] = df_binance
            time.sleep(0.5) # Be polite to APIs

            # Example: Fetch from Coinbase (optional, if you want redundant sources)
            # coinbase_key = f"coinbase_{symbol.replace('/', '')}_{tf}_ohlcv"
            # df_coinbase = fetch_exchange_ohlcv('coinbase', symbol, tf, since_dt=start_dt)
            # if df_coinbase is not None and not df_coinbase.empty:
            #     all_fetched_data[coinbase_key] = df_coinbase
            # time.sleep(0.5)

    # --- Fetch Derivatives Data ---
    # Example: Funding Rate for BTC perpetual on Binance
    funding_symbol = 'BTC/USDT' # Check exact symbol format required by exchange/ccxt
    funding_key = f"binance_{funding_symbol.replace('/', '')}_funding"
    df_funding = fetch_funding_rates('binance', funding_symbol, since_dt=start_dt)
    if df_funding is not None and not df_funding.empty:
        all_fetched_data[funding_key] = df_funding
    time.sleep(0.5)

    # --- Fetch On-Chain Data ---
    # Example: Bitcoin Active Addresses from Glassnode (daily)
    onchain_asset = 'BTC'
    onchain_metric = 'addresses/active_count'
    onchain_params = {'i': '24h', 's': int(start_dt.timestamp())} # 'i' for interval, 's' for since timestamp (seconds)
    onchain_key = f"glassnode_{onchain_asset}_{onchain_metric.replace('/', '_')}_1d"
    df_onchain = fetch_onchain_metric('glassnode', onchain_metric, onchain_asset, params=onchain_params)
    if df_onchain is not None and not df_onchain.empty:
        all_fetched_data[onchain_key] = df_onchain
    time.sleep(0.5)

    # --- Combine Data ---
    # Choose the target frequency for your model (e.g., '1h')
    # This should match the frequency of your primary prediction target (e.g., 1h BTC price)
    target_prediction_frequency = '1min'
    final_combined_df = combine_dataframes(all_fetched_data, target_prediction_frequency)

    # --- Save or Use Data ---
    if final_combined_df is not None and not final_combined_df.empty:
        print("\n--- Final Combined DataFrame ---")
        print(final_combined_df.head())
        print(final_combined_df.info())

        # Save the combined data (Parquet is often more efficient than CSV for storage/loading)
        save_path_parquet = 'data/final_crypto_data.parquet'
        save_path_csv = 'data/final_crypto_data.csv'
        os.makedirs('data', exist_ok=True)
        final_combined_df.to_parquet(save_path_parquet)
        final_combined_df.to_csv(save_path_csv)
        print(f"Data saved to {save_path_parquet} and {save_path_csv}")

        # Now `final_combined_df` is ready for feature engineering and model training
        # Example: Calculate some features
        # final_combined_df['feature_SMA_close_20'] = final_combined_df['binance_BTCUSDT_1h_ohlcv_close'].rolling(20).mean()
        # print(final_combined_df.tail())

    else:
        print("\nNo final combined dataframe was generated.")