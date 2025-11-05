#!/usr/bin/env python3
"""
KuCoin Bitcoin Trading Bot - Phase 1: Balance Monitoring + Portfolio Rebalancing Calculations
Fetches coin-margined futures account balance and calculates portfolio rebalancing needs
"""
import sys
import os
import time
import logging
import hmac
import hashlib
import base64
import requests
import uuid
import json
from datetime import datetime
from dotenv import load_dotenv

# Configure advanced logging system
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all levels, filter in handlers

# Custom filter for file logging levels
class LogLevelFilter(logging.Filter):
    """Filter logs based on configured log level"""
    def __init__(self, log_level='TRADE'):
        super().__init__()
        self.log_level = log_level.upper()
        
        # Define what each level includes
        self.level_hierarchy = {
            'ERROR': ['ERROR', 'CRITICAL'],
            'WARNING': ['ERROR', 'CRITICAL', 'WARNING'],
            'TRADE': ['ERROR', 'CRITICAL', 'WARNING', 'TRADE', 'STARTUP', 'SHUTDOWN'],
            'INFO': ['ERROR', 'CRITICAL', 'WARNING', 'INFO', 'TRADE', 'STARTUP', 'SHUTDOWN', 'DEBUG']
        }
    
    def filter(self, record):
        # Allow ERROR and CRITICAL always
        if record.levelno >= logging.ERROR:
            return True
        
        # Check custom levels
        msg = record.getMessage()
        
        # Map message patterns to levels
        if '[ERROR]' in msg:
            return 'ERROR' in self.level_hierarchy.get(self.log_level, [])
        elif '[WARNING]' in msg:
            return 'WARNING' in self.level_hierarchy.get(self.log_level, [])
        elif '[TRADE]' in msg or '[GTC] Order fully filled' in msg:
            return 'TRADE' in self.level_hierarchy.get(self.log_level, [])
        elif '[OK] Bot started' in msg or '[OK] Bot stopped' in msg or '[OK] Time sync' in msg or '[OK] Trading Bot initialized' in msg:
            return 'STARTUP' in self.level_hierarchy.get(self.log_level, [])
        elif '[OK] Successfully fetched' in msg or '[GTC] Status:' in msg or 'Position mode' in msg or 'Limit price calculation' in msg or 'Best Bid' in msg or 'Best Ask' in msg:
            return 'INFO' in self.level_hierarchy.get(self.log_level, [])
        
        # Default: allow for INFO level
        return self.log_level == 'INFO'

# Console handler (always enabled, minimal output)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))


class RotatingFileHandler(logging.Handler):
    """Custom rotating file handler with time-based rotation"""
    
    def __init__(self, filename, rotation_hours=168, enabled=True):
        super().__init__()
        self.filename = filename
        self.rotation_hours = rotation_hours
        self.enabled = enabled
        self.last_rotation = time.time()
    
    def emit(self, record):
        if not self.enabled:
            return
        
        try:
            # Check if rotation needed
            if self.rotation_hours > 0:
                current_time = time.time()
                if current_time - self.last_rotation > (self.rotation_hours * 3600):
                    self._rotate_logs()
                    self.last_rotation = current_time
            
            # Write log entry
            msg = self.format(record)
            with open(self.filename, 'a', encoding='utf-8') as f:
                f.write(msg + '\n')
        except Exception:
            self.handleError(record)
    
    def _rotate_logs(self):
        """Remove log entries older than rotation_hours"""
        if not os.path.exists(self.filename):
            return
        
        try:
            cutoff_time = time.time() - (self.rotation_hours * 3600)
            
            # Read existing logs
            with open(self.filename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Filter logs newer than cutoff
            new_lines = []
            for line in lines:
                try:
                    # Parse timestamp (format: 2025-11-04 18:55:06,...)
                    if len(line) > 19:
                        timestamp_str = line[:19]
                        log_time = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S').timestamp()
                        if log_time >= cutoff_time:
                            new_lines.append(line)
                except:
                    # Keep lines we can't parse
                    new_lines.append(line)
            
            # Write back filtered logs
            with open(self.filename, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            
        except Exception as e:
            # Don't crash if rotation fails
            pass


logger.addHandler(console_handler)


class TradeLogger:
    """CSV-based trade logger for minimal logging"""
    
    def __init__(self, csv_filename: str = None, enabled: bool = True):
        """
        Initialize trade logger with monthly CSV filename
        
        Args:
            csv_filename: Optional custom filename
            enabled: If False, logging is disabled
        """
        self.enabled = enabled
        
        if not self.enabled:
            return
        
        if csv_filename is None:
            from datetime import datetime
            csv_filename = datetime.now().strftime("trades_%Y-%m.csv")
        
        self.csv_filename = csv_filename
        self.csv_headers = [
            'timestamp', 'action', 'symbol', 'contracts', 'limit_price', 
            'filled_price', 'order_id', 'slippage', 'status', 'error'
        ]
        
        # Create file with headers if it doesn't exist
        if not os.path.exists(self.csv_filename):
            self._create_csv_file()
    
    def _create_csv_file(self):
        """Create CSV file with headers"""
        try:
            import csv
            with open(self.csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(self.csv_headers)
        except Exception as e:
            logger.error(f"[ERROR] Failed to create CSV file: {e}")
    
    def log_trade(self, action: str, symbol: str, contracts: int, 
                  limit_price: float = None, filled_price: float = None,
                  order_id: str = None, slippage: float = None, 
                  status: str = 'PENDING', error: str = None):
        """Log a trade to CSV"""
        if not self.enabled:
            return
        
        from datetime import datetime
        import csv
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        row = [
            timestamp,
            action,
            symbol,
            contracts,
            f"{limit_price:.1f}" if limit_price else "",
            f"{filled_price:.1f}" if filled_price else "",
            order_id or "",
            f"{slippage:+.1f}%" if slippage is not None else "",
            status,
            error or ""
        ]
        
        try:
            with open(self.csv_filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except Exception as e:
            logger.error(f"[ERROR] Failed to log trade to CSV: {e}")



class KuCoinFuturesClient:
    """KuCoin Futures API Client with authentication and data fetching"""
    
    def __init__(self, api_key: str, api_secret: str, api_passphrase: str, endpoint: str):
        """
        Initialize KuCoin Futures client
        
        Args:
            api_key: KuCoin API Key
            api_secret: KuCoin API Secret
            api_passphrase: KuCoin API Passphrase
            endpoint: API endpoint URL
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = self._encrypt_passphrase(api_passphrase)
        self.endpoint = endpoint
        self.session = requests.Session()
        
        # Check time synchronization on initialization
        self._check_time_sync()
    
    def _check_time_sync(self):
        """
        Check if system time is synchronized with server time
        Warns if time drift exceeds acceptable threshold
        """
        try:
            # Get server time from public endpoint (no auth needed)
            response = self.session.get(
                f"{self.endpoint}/api/v1/timestamp",
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == '200000':
                    server_time = int(data.get('data', 0))
                    local_time = int(time.time() * 1000)
                    time_diff = abs(server_time - local_time)
                    
                    # Warn if time difference exceeds 5 seconds
                    if time_diff > 5000:
                        logger.warning(f"[WARNING] System time off by {time_diff / 1000:.1f}s - may cause auth errors")
                    else:
                        logger.info(f"[OK] Time sync OK (drift: {time_diff}ms)")
        except Exception as e:
            logger.warning(f"[WARNING] Could not verify time sync: {e}")
    
    def _encrypt_passphrase(self, passphrase: str) -> str:
        """Encrypt passphrase using HMAC SHA256"""
        return base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                passphrase.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
    
    def _generate_signature(self, timestamp: str, method: str, endpoint_path: str, body: str = '') -> str:
        """
        Generate signature for API request
        
        Args:
            timestamp: Unix timestamp in milliseconds
            method: HTTP method (GET, POST, etc.)
            endpoint_path: API endpoint path
            body: Request body (empty for GET requests)
            
        Returns:
            Base64 encoded signature
        """
        str_to_sign = timestamp + method + endpoint_path + body
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode()
        return signature
    
    def _get_headers(self, method: str, endpoint_path: str, body: str = '') -> dict:
        """
        Generate authentication headers for API request
        
        Args:
            method: HTTP method
            endpoint_path: API endpoint path
            body: Request body
            
        Returns:
            Dictionary of headers
        """
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, endpoint_path, body)
        
        headers = {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': timestamp,
            'KC-API-PASSPHRASE': self.api_passphrase,
            'KC-API-KEY-VERSION': '3',
            'Content-Type': 'application/json'
        }
        return headers
    
    def get_ticker_price(self, symbol: str = 'XBTUSDM') -> float:
        """
        Get current ticker price for a futures symbol
        
        Args:
            symbol: Futures symbol (e.g., XBTUSDM for BTC coin-margined)
            
        Returns:
            Current price as float, or 0 if error
        """
        method = 'GET'
        endpoint_path = f'/api/v1/ticker?symbol={symbol}'
        url = self.endpoint + endpoint_path
        
        # Note: Ticker is a public endpoint but we include auth headers for consistency
        headers = self._get_headers(method, endpoint_path)
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                price = float(data.get('data', {}).get('price', 0))
                return price
            else:
                logger.error(f"API Error fetching price: {data.get('msg', 'Unknown error')}")
                return 0.0
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching price: {e}")
            return 0.0
        except Exception as e:
            logger.error(f"Unexpected error fetching price: {e}")
            return 0.0
    
    def get_best_bid_ask(self, symbol: str = 'XBTUSDM') -> dict:
        """
        Get best bid and ask prices from ticker
        
        Args:
            symbol: Futures symbol (e.g., XBTUSDM for BTC coin-margined)
            
        Returns:
            Dictionary with 'best_bid' and 'best_ask' prices, or empty dict if error
        """
        method = 'GET'
        endpoint_path = f'/api/v1/ticker?symbol={symbol}'
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                ticker_data = data.get('data', {})
                
                # Get best bid and ask from ticker
                best_bid = float(ticker_data.get('bestBidPrice', 0))
                best_ask = float(ticker_data.get('bestAskPrice', 0))
                
                # Fallback to last price if bid/ask not available
                if best_bid == 0 or best_ask == 0:
                    last_price = float(ticker_data.get('price', 0))
                    if last_price > 0:
                        logger.warning("Best bid/ask not available, using last price as fallback")
                        best_bid = best_bid if best_bid > 0 else last_price
                        best_ask = best_ask if best_ask > 0 else last_price
                
                return {
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'last_price': float(ticker_data.get('price', 0)),
                    'timestamp': ticker_data.get('ts', 0)
                }
            else:
                logger.error(f"API Error fetching ticker: {data.get('msg', 'Unknown error')}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching ticker: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching ticker: {e}")
            return {}
    
    def get_futures_account(self, currency: str = 'XBT') -> dict:
        """
        Get futures account balance
        
        Args:
            currency: Currency to query (XBT for Bitcoin coin-margined, USDT for USDT-margined)
            
        Returns:
            Dictionary containing account information
        """
        # KuCoin may have changed their API - try multiple approaches
        # Order: most likely to work first
        attempts = [
            ('without parameters', '/api/v1/account-overview'),  # Current API doesn't need currency
            (f'with currency={currency}', f'/api/v1/account-overview?currency={currency}'),  # Old API format
        ]
        
        last_error = None
        
        for attempt_name, endpoint_path in attempts:
            method = 'GET'
            url = self.endpoint + endpoint_path
            
            headers = self._get_headers(method, endpoint_path)
            
            try:
                logger.debug(f"Attempting to fetch account: {attempt_name}")
                response = self.session.get(url, headers=headers, timeout=10)
                
                # Check for specific error codes
                if response.status_code == 400:
                    logger.debug(f"400 Bad Request with {attempt_name}, trying next method...")
                    last_error = f"400 Bad Request with {attempt_name}"
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == '200000':
                    logger.info(f"[OK] Successfully fetched account (method: {attempt_name})")
                    return data.get('data', {})
                else:
                    error_msg = data.get('msg', 'Unknown error')
                    logger.debug(f"API returned error with {attempt_name}: {error_msg}")
                    last_error = error_msg
                    continue
                    
            except requests.exceptions.HTTPError as e:
                logger.debug(f"HTTP error with {attempt_name}: {e}")
                last_error = str(e)
                continue
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request failed with {attempt_name}: {e}")
                last_error = str(e)
                continue
            except Exception as e:
                logger.debug(f"Unexpected error with {attempt_name}: {e}")
                last_error = str(e)
                continue
        
        # All attempts failed
        logger.error("=" * 80)
        logger.error("FAILED TO FETCH ACCOUNT - ALL METHODS TRIED")
        logger.error(f"Last error: {last_error}")
        logger.error("Possible causes:")
        logger.error("  1. KuCoin API changed (check https://www.kucoin.com/docs)")
        logger.error("  2. API key permissions issue")
        logger.error("  3. KuCoin service issue (check https://status.kucoin.com/)")
        logger.error("=" * 80)
        return {}
    
    def get_positions(self, currency: str = 'XBT') -> list:
        """
        Get all open positions
        
        Args:
            currency: Currency to filter positions (XBT for Bitcoin coin-margined)
            
        Returns:
            List of position dictionaries
        """
        # KuCoin may have changed their API - try multiple approaches
        attempts = [
            ('without parameters', '/api/v1/positions'),  # Current API doesn't need currency
            (f'with currency={currency}', f'/api/v1/positions?currency={currency}'),  # Old API format
        ]
        
        last_error = None
        
        for attempt_name, endpoint_path in attempts:
            method = 'GET'
            url = self.endpoint + endpoint_path
            
            headers = self._get_headers(method, endpoint_path)
            
            try:
                logger.debug(f"Attempting to fetch positions: {attempt_name}")
                response = self.session.get(url, headers=headers, timeout=10)
                
                # Check for specific error codes
                if response.status_code == 400:
                    logger.debug(f"400 Bad Request with {attempt_name}, trying next method...")
                    last_error = f"400 Bad Request with {attempt_name}"
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('code') == '200000':
                    positions = data.get('data', [])
                    logger.info(f"[OK] Successfully fetched positions (method: {attempt_name})")
                    # Filter only open positions
                    return [pos for pos in positions if pos.get('isOpen', False)]
                else:
                    error_msg = data.get('msg', 'Unknown error')
                    logger.debug(f"API returned error with {attempt_name}: {error_msg}")
                    last_error = error_msg
                    continue
                    
            except requests.exceptions.HTTPError as e:
                logger.debug(f"HTTP error with {attempt_name}: {e}")
                last_error = str(e)
                continue
            except requests.exceptions.RequestException as e:
                logger.debug(f"Request failed with {attempt_name}: {e}")
                last_error = str(e)
                continue
            except Exception as e:
                logger.debug(f"Unexpected error with {attempt_name}: {e}")
                last_error = str(e)
                continue
        
        # All attempts failed
        logger.error("=" * 80)
        logger.error("FAILED TO FETCH POSITIONS - ALL METHODS TRIED")
        logger.error(f"Last error: {last_error}")
        logger.error("Possible causes:")
        logger.error("  1. KuCoin API changed (check https://www.kucoin.com/docs)")
        logger.error("  2. API key permissions issue")
        logger.error("  3. KuCoin service issue (check https://status.kucoin.com/)")
        logger.error("=" * 80)
        return []
    
    def get_position_mode(self, symbol: str) -> str:
        """
        Get current position mode for a symbol
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            
        Returns:
            Position mode: 'ONE_WAY' or 'HEDGE_MODE', or empty string if error
        """
        method = 'GET'
        endpoint_path = f'/api/v2/position/getPositionMode?symbol={symbol}'
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                # API returns integer: 0 = ONE_WAY, 1 = HEDGE_MODE
                position_mode_int = data.get('data', {}).get('positionMode', -1)
                
                # Convert integer to string format
                if position_mode_int == 0:
                    position_mode = 'ONE_WAY'
                elif position_mode_int == 1:
                    position_mode = 'HEDGE_MODE'
                else:
                    logger.error(f"Unknown position mode value: {position_mode_int}")
                    return ''
                
                logger.info(f"Current position mode for {symbol}: {position_mode} (API value: {position_mode_int})")
                return position_mode
            else:
                logger.error(f"API Error fetching position mode: {data.get('msg', 'Unknown error')}")
                return ''
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching position mode: {e}")
            return ''
        except Exception as e:
            logger.error(f"Unexpected error fetching position mode: {e}")
            return ''
    
    def set_position_mode(self, symbol: str, position_mode: str) -> bool:
        """
        Set position mode for a symbol
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            position_mode: 'ONE_WAY' or 'HEDGE_MODE'
            
        Returns:
            True if successful, False otherwise
        """
        import json
        
        # Convert string format to integer: 0 = ONE_WAY, 1 = HEDGE_MODE
        if position_mode == 'ONE_WAY':
            position_mode_int = 0
        elif position_mode == 'HEDGE_MODE':
            position_mode_int = 1
        else:
            logger.error(f"Invalid position mode: {position_mode}. Must be ONE_WAY or HEDGE_MODE")
            return False
        
        method = 'POST'
        endpoint_path = '/api/v2/position/changePositionMode'
        url = self.endpoint + endpoint_path
        
        body_data = {
            'symbol': symbol,
            'positionMode': position_mode_int
        }
        body = json.dumps(body_data)
        
        headers = self._get_headers(method, endpoint_path, body)
        
        try:
            response = self.session.post(url, headers=headers, data=body, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                logger.info(f"Successfully set position mode for {symbol} to {position_mode} (API value: {position_mode_int})")
                return True
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"API Error setting position mode: {error_msg}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed setting position mode: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error setting position mode: {e}")
            return False
    
    def get_order_details(self, order_id: str) -> dict:
        """
        Get details of a specific order
        
        Args:
            order_id: Order ID to query
            
        Returns:
            Dictionary with order details, or empty dict if error
        """
        method = 'GET'
        endpoint_path = f'/api/v1/orders/{order_id}'
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                return data.get('data', {})
            else:
                logger.error(f"API Error fetching order: {data.get('msg', 'Unknown error')}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching order: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching order: {e}")
            return {}
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            True if successful, False otherwise
        """
        method = 'DELETE'
        endpoint_path = f'/api/v1/orders/{order_id}'
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            response = self.session.delete(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                logger.info(f"Successfully cancelled order: {order_id}")
                return True
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"API Error cancelling order: {error_msg}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed cancelling order: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error cancelling order: {e}")
            return False


class OrderExecutor:
    """Handle order execution with safety checks"""
    
    def __init__(self, client: 'KuCoinFuturesClient', dry_run: bool = True, 
                 max_order_usd: float = 10000, min_order_usd: float = 100,
                 margin_mode: str = 'ISOLATED', position_mode: str = 'ONE_WAY',
                 auto_set_position_mode: bool = False, order_type: str = 'market',
                 time_in_force: str = 'IOC', slippage_pct: float = 0.1,
                 gtc_timeout_seconds: int = 300, trade_logging_enabled: bool = True):
        """
        Initialize Order Executor
        
        Args:
            client: KuCoinFuturesClient instance
            dry_run: If True, simulate orders without executing
            max_order_usd: Maximum order size in USD
            min_order_usd: Minimum order size in USD
            margin_mode: Margin mode ('ISOLATED' or 'CROSS')
            position_mode: Position mode ('ONE_WAY' or 'HEDGE_MODE')
            auto_set_position_mode: If True, automatically set position mode if mismatch
            order_type: Order type ('market' or 'limit')
            time_in_force: Time in force for limit orders ('IOC', 'GTC', 'FOK')
            slippage_pct: Slippage percentage for limit orders (positive = aggressive, negative = conservative)
            gtc_timeout_seconds: Timeout for GTC orders before cancelling (default 300s = 5min)
        """
        self.client = client
        self.dry_run = dry_run
        self.max_order_usd = max_order_usd
        self.min_order_usd = min_order_usd
        self.margin_mode = margin_mode.upper()
        self.position_mode = position_mode.upper()
        self.auto_set_position_mode = auto_set_position_mode
        self.order_type = order_type.lower()
        self.time_in_force = time_in_force.upper()
        self.slippage_pct = slippage_pct
        self.gtc_timeout_seconds = gtc_timeout_seconds
        
        # Initialize trade logger
        self.trade_logger = TradeLogger(enabled=trade_logging_enabled)
        
        if dry_run:
            logger.info("[OK] DRY RUN MODE - No real orders will be placed")
    
    def verify_position_mode(self, symbol: str) -> bool:
        """
        Verify that the current position mode matches the configured mode
        
        Args:
            symbol: Futures symbol to check
            
        Returns:
            True if position mode matches or was successfully set, False otherwise
        """
        
        current_mode = self.client.get_position_mode(symbol)
        
        if not current_mode:
            logger.warning("[WARNING] Could not fetch current position mode")
            return False
        
        if current_mode == self.position_mode:
            logger.info(f"[OK] Position mode verified: {current_mode}")
            return True
        
        # Position mode mismatch
        logger.warning("=" * 80)
        logger.warning(f"[MISMATCH] Position mode does not match!")
        logger.warning(f"  Current on KuCoin:  {current_mode}")
        logger.warning(f"  Expected by bot:    {self.position_mode}")
        logger.warning("=" * 80)
        
        if self.auto_set_position_mode:
            logger.info(f"Attempting to set position mode to {self.position_mode}...")
            success = self.client.set_position_mode(symbol, self.position_mode)
            if success:
                logger.info(f"[OK] Successfully set position mode to {self.position_mode}")
                return True
            else:
                logger.error(f"âœ— Failed to set position mode to {self.position_mode}")
                logger.error("Please manually set position mode on KuCoin website")
                return False
        else:
            logger.warning("Auto-set position mode is DISABLED")
            logger.warning(f"Please manually change position mode to {self.position_mode} on KuCoin")
            logger.warning("Or enable AUTO_SET_POSITION_MODE=true in .env file")
            return False
    
    def calculate_limit_price(self, symbol: str, side: str) -> float:
        """
        Calculate limit price based on slippage and best bid/ask
        
        Args:
            symbol: Futures symbol
            side: 'buy' or 'sell'
            
        Returns:
            Limit price, or 0 if error
        """
        # Get best bid/ask from ticker
        ticker_data = self.client.get_best_bid_ask(symbol)
        if not ticker_data:
            logger.error("Failed to fetch ticker data for limit price calculation")
            return 0.0
        
        best_bid = ticker_data.get('best_bid', 0)
        best_ask = ticker_data.get('best_ask', 0)
        
        if best_bid == 0 or best_ask == 0:
            logger.error("Invalid bid/ask prices")
            return 0.0
        
        # Calculate limit price based on side and slippage
        if side.lower() == 'buy':
            # For CLOSE SHORT (BUY): Use best ask as reference
            # Positive slippage = pay more (aggressive)
            # Negative slippage = pay less (conservative, maker)
            reference_price = best_ask
            limit_price = reference_price * (1 + self.slippage_pct / 100)
        else:  # sell
            # For OPEN SHORT (SELL): Use best bid as reference
            # Positive slippage = receive less (aggressive)
            # Negative slippage = receive more (conservative, maker)
            reference_price = best_bid
            limit_price = reference_price * (1 - self.slippage_pct / 100)
        
        logger.info(f"Limit price calculation:")
        logger.info(f"  Side: {side.upper()}")
        logger.info(f"  Best Bid: ${best_bid:,.2f}")
        logger.info(f"  Best Ask: ${best_ask:,.2f}")
        logger.info(f"  Reference: ${reference_price:,.2f}")
        logger.info(f"  Slippage: {self.slippage_pct:+.3f}%")
        logger.info(f"  Limit Price: ${limit_price:,.2f}")
        
        return limit_price
    
    def monitor_gtc_order(self, order_id: str, symbol: str, side: str, 
                         contracts: int, leverage: int, reduce_only: bool,
                         action: str = None, limit_price: float = None) -> dict:
        """
        Monitor a GTC order until filled or timeout, then retry if needed
        
        Args:
            order_id: Order ID to monitor
            symbol: Futures symbol
            side: 'buy' or 'sell'
            contracts: Number of contracts
            leverage: Leverage
            reduce_only: If reduce only
            action: Trade action for CSV logging (OPEN SHORT / CLOSE SHORT)
            limit_price: Limit price for CSV logging
            
        Returns:
            Dictionary with final result
        """
        import time
        
        logger.info(f"[GTC] Monitoring order {order_id} for up to {self.gtc_timeout_seconds}s...")
        
        start_time = time.time()
        check_interval = 5  # Check every 5 seconds
        
        while True:
            elapsed = time.time() - start_time
            
            # Check if timeout reached
            if elapsed >= self.gtc_timeout_seconds:
                logger.warning(f"[GTC] Timeout reached ({self.gtc_timeout_seconds}s)")
                
                # Get order status
                order_details = self.client.get_order_details(order_id)
                if order_details:
                    filled_size = float(order_details.get('dealSize', 0))
                    total_size = float(order_details.get('size', contracts))
                    
                    logger.info(f"[GTC] Order status: {filled_size}/{total_size} filled")
                    
                    if filled_size < total_size:
                        logger.warning(f"[GTC] Order not fully filled, cancelling...")
                        
                        # Cancel the remaining
                        cancel_success = self.client.cancel_order(order_id)
                        if cancel_success:
                            logger.info(f"[GTC] Cancelled order {order_id}")
                        
                        # Calculate unfilled amount
                        unfilled = int(total_size - filled_size)
                        
                        # If completely unfilled (0 filled), log as CANCELLED
                        if filled_size == 0 and action and limit_price:
                            self.trade_logger.log_trade(
                                action=action.replace(" ", "_"),
                                symbol=symbol,
                                contracts=abs(contracts),
                                limit_price=limit_price,
                                order_id=order_id,
                                slippage=self.slippage_pct,
                                status='CANCELLED',
                                error='GTC timeout - no fill'
                            )
                        
                        if unfilled > 0:
                            logger.info(f"[GTC] Retrying with updated price for {unfilled} contracts...")
                            
                            # Retry with new price (this will create a new CSV entry when it fills/fails)
                            return self.place_order(symbol, side, unfilled, leverage, reduce_only)
                        else:
                            # Partial fill - log what was filled
                            if action and limit_price:
                                self.trade_logger.log_trade(
                                    action=action.replace(" ", "_"),
                                    symbol=symbol,
                                    contracts=int(filled_size),  # Log only what was filled
                                    limit_price=limit_price,
                                    order_id=order_id,
                                    slippage=self.slippage_pct,
                                    status='PARTIAL',
                                    error=f'Filled {filled_size}/{total_size}'
                                )
                            
                            return {
                                'success': True,
                                'order_id': order_id,
                                'partially_filled': True,
                                'filled_size': filled_size
                            }
                
                # Final timeout with no order details - log as failed
                if action and limit_price:
                    self.trade_logger.log_trade(
                        action=action.replace(" ", "_"),
                        symbol=symbol,
                        contracts=abs(contracts),
                        limit_price=limit_price,
                        order_id=order_id,
                        slippage=self.slippage_pct,
                        status='TIMEOUT',
                        error='GTC timeout - could not get order status'
                    )
                
                return {'success': False, 'error': 'GTC order timeout'}
            
            # Check order status
            order_details = self.client.get_order_details(order_id)
            if order_details:
                status = order_details.get('status', '')
                filled_size = float(order_details.get('dealSize', 0))
                total_size = float(order_details.get('size', contracts))
                
                logger.info(f"[GTC] Status: {status}, Filled: {filled_size}/{total_size} ({elapsed:.0f}s elapsed)")
                
                if status == 'done':
                    logger.info(f"[GTC] Order fully filled!")
                    
                    # Log successful fill to CSV
                    if action and limit_price:
                        self.trade_logger.log_trade(
                            action=action.replace(" ", "_"),
                            symbol=symbol,
                            contracts=abs(contracts),
                            limit_price=limit_price,
                            order_id=order_id,
                            slippage=self.slippage_pct,
                            status='FILLED'
                        )
                    
                    return {
                        'success': True,
                        'order_id': order_id,
                        'filled_size': filled_size
                    }
            
            # Wait before next check
            time.sleep(check_interval)
    
    def place_order(self, symbol: str, side: str, contracts: int, leverage: int = 1, 
                   reduce_only: bool = False) -> dict:
        """
        Place a market or limit order on KuCoin Futures
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            side: 'buy' or 'sell'
            contracts: Number of contracts
            leverage: Leverage to use
            reduce_only: If True, only reduce position size (for closing)
            
        Returns:
            Dictionary with order result
        """
        import uuid
        import json
        
        # Safety check: Validate order size
        order_usd = abs(contracts)
        if order_usd > self.max_order_usd:
            logger.error(f"Order size ${order_usd:,.0f} exceeds maximum ${self.max_order_usd:,.0f}")
            return {'success': False, 'error': 'Order size exceeds maximum'}
        
        if order_usd < self.min_order_usd:
            logger.warning(f"Order size ${order_usd:,.0f} below minimum ${self.min_order_usd:,.0f}")
            return {'success': False, 'error': 'Order size below minimum'}
        
        # Safety check: Verify position mode (only for real orders)
        if not self.dry_run:
            if not self.verify_position_mode(symbol):
                logger.error("[FAILED] Position mode verification failed")
                logger.error("[FAILED] Cannot place order - position mode mismatch")
                return {'success': False, 'error': 'Position mode mismatch'}
        
        # Generate client order ID
        client_oid = str(uuid.uuid4())
        
        # Determine order type and TIF
        order_type = self.order_type
        time_in_force = self.time_in_force
        
        # If using limit orders with negative slippage (conservative), must use GTC
        if order_type == 'limit' and self.slippage_pct < 0:
            time_in_force = 'GTC'
            logger.info(f"Using GTC for conservative limit order (slippage: {self.slippage_pct:+.3f}%)")
        
        # Calculate limit price if needed
        limit_price = None
        if order_type == 'limit':
            limit_price = self.calculate_limit_price(symbol, side)
            if limit_price == 0:
                logger.error("Failed to calculate limit price, falling back to market order")
                order_type = 'market'
        
        # Prepare order data
        order_data = {
            'clientOid': client_oid,
            'side': side.lower(),
            'symbol': symbol,
            'type': order_type,
            'leverage': str(leverage),
            'size': abs(contracts),
            'reduceOnly': reduce_only,
            'marginMode': self.margin_mode
        }
        
        # Add limit-specific fields
        if order_type == 'limit' and limit_price:
            # KuCoin requires price to be multiple of 0.1 (1 decimal place)
            order_data['price'] = str(round(limit_price, 1))
            order_data['timeInForce'] = time_in_force
        
        # Minimal order info (only if not dry run)
        if not self.dry_run:
            pass  # Logging moved to after order execution
        
        if self.dry_run:
            action = "OPEN SHORT" if side.lower() == 'sell' and not reduce_only else "CLOSE SHORT"
            logger.info(f"[DRY RUN] {action} {abs(contracts)} @ ${limit_price:,.1f}" if limit_price else f"[DRY RUN] {action} {abs(contracts)} MARKET")
            return {
                'success': True,
                'dry_run': True,
                'order_id': 'DRY_RUN_' + client_oid[:8],
                'client_oid': client_oid,
                'order_type': order_type
            }
        
        # Execute real order
        try:
            method = 'POST'
            endpoint_path = '/api/v1/orders'
            url = self.client.endpoint + endpoint_path
            body = json.dumps(order_data)
            
            headers = self.client._get_headers(method, endpoint_path, body)
            
            response = self.client.session.post(url, headers=headers, data=body, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                order_id = data.get('data', {}).get('orderId', '')
                
                # Console output
                action = "OPEN SHORT" if side.lower() == 'sell' and not reduce_only else "CLOSE SHORT"
                logger.info(f"[TRADE] {action} {abs(contracts)} @ ${limit_price:,.1f}" if limit_price else f"[TRADE] {action} {abs(contracts)} MARKET [FILLED]")
                
                # If GTC order, monitor it first before logging to CSV
                if order_type == 'limit' and time_in_force == 'GTC':
                    # Pass trade info for CSV logging after monitoring
                    return self.monitor_gtc_order(
                        order_id, symbol, side, contracts, leverage, reduce_only,
                        action=action, limit_price=limit_price
                    )
                
                # For non-GTC orders, log to CSV immediately
                self.trade_logger.log_trade(
                    action=action.replace(" ", "_"),
                    symbol=symbol,
                    contracts=abs(contracts),
                    limit_price=limit_price if order_type == 'limit' else None,
                    order_id=order_id,
                    slippage=self.slippage_pct if order_type == 'limit' else None,
                    status='FILLED'
                )
                
                return {
                    'success': True,
                    'order_id': order_id,
                    'client_oid': client_oid,
                    'order_type': order_type,
                    'time_in_force': time_in_force if order_type == 'limit' else None
                }
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"[FAILED] Order placement failed: {error_msg}")
                
                # For IOC/FOK failures, just log and continue (retry next cycle)
                if order_type == 'limit' and time_in_force in ['IOC', 'FOK']:
                    logger.warning(f"[{time_in_force}] Order failed to fill - will retry next cycle")
                
                return {'success': False, 'error': error_msg}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"[FAILED] Request failed: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"[FAILED] Unexpected error: {e}")
            return {'success': False, 'error': str(e)}
    
    def open_short(self, symbol: str, contracts: int, leverage: int = 1) -> dict:
        """
        Open a SHORT position (SELL)
        
        Args:
            symbol: Futures symbol
            contracts: Number of contracts to short
            leverage: Leverage to use
            
        Returns:
            Order result dictionary
        """
        logger.info(f"Opening SHORT position: {contracts:,} contracts")
        return self.place_order(symbol, 'sell', contracts, leverage, reduce_only=False)
    
    def close_short(self, symbol: str, contracts: int) -> dict:
        """
        Close a SHORT position (BUY with reduceOnly)
        
        Args:
            symbol: Futures symbol
            contracts: Number of contracts to close
            
        Returns:
            Order result dictionary
        """
        logger.info(f"Closing SHORT position: {contracts:,} contracts")
        return self.place_order(symbol, 'buy', contracts, leverage=1, reduce_only=True)
    
    def execute_rebalance(self, metrics: dict, symbol: str = 'XBTUSDM', 
                         leverage: int = 1) -> dict:
        """
        Execute rebalancing based on portfolio metrics
        
        Args:
            metrics: Portfolio metrics from PortfolioCalculator
            symbol: Futures symbol to trade
            leverage: Leverage to use for new positions
            
        Returns:
            Execution result dictionary
        """
        if not metrics['needs_rebalancing']:
            logger.info("Portfolio is balanced - no action needed")
            return {
                'success': True,
                'action': 'none',
                'message': 'Portfolio already balanced'
            }
        
        contracts = metrics['contracts_to_adjust']
        
        logger.info("=" * 80)
        logger.info("REBALANCING REQUIRED")
        logger.info("=" * 80)
        logger.info(f"Deviation:        {metrics['allocation_deviation']:+.2f}%")
        logger.info(f"Contracts needed: {contracts:,}")
        logger.info(f"USD Value:        ${abs(metrics['short_position_adjustment']):,.2f}")
        logger.info("=" * 80)
        
        # Determine action
        if metrics['allocation_deviation'] > 0:
            # Too much BTC - need to OPEN more shorts
            logger.info("Action: OPEN SHORT (reduce BTC exposure)")
            result = self.open_short(symbol, contracts, leverage)
            result['action'] = 'open_short'
        else:
            # Too little BTC - need to CLOSE shorts
            logger.info("Action: CLOSE SHORT (increase BTC exposure)")
            result = self.close_short(symbol, contracts)
            result['action'] = 'close_short'
        
        result['contracts'] = contracts
        result['usd_value'] = abs(metrics['short_position_adjustment'])
        
        return result


class PortfolioCalculator:
    """Calculate portfolio metrics and rebalancing needs"""
    
    @staticmethod
    def calculate_metrics(cold_storage_btc: float, futures_account: dict, 
                         positions: list, btc_price: float, target_allocation: float, 
                         rebalance_threshold: float) -> dict:
        """
        Calculate portfolio metrics and rebalancing needs
        
        Args:
            cold_storage_btc: Amount of BTC in cold storage
            futures_account: Futures account data from API
            positions: List of open positions
            btc_price: Current BTC price in USD
            target_allocation: Target BTC allocation percentage (e.g., 50.0)
            rebalance_threshold: Threshold to trigger rebalance (e.g., 1.0 for 1%)
            
        Returns:
            Dictionary with portfolio metrics and rebalancing info
        """
        # Get futures account BTC balance (accountEquity for coin-margined)
        futures_btc = float(futures_account.get('accountEquity', 0))
        
        # Calculate short position exposure
        # For coin-margined (inverse) contracts like XBTUSDM:
        # - Each contract = $1 USD
        # - Negative currentQty = short position
        # - Short position USD value = |currentQty|
        total_short_usd = 0
        position_details = []
        
        for pos in positions:
            symbol = pos.get('symbol', '')
            current_qty = float(pos.get('currentQty', 0))
            is_inverse = pos.get('isInverse', False)
            mark_price = float(pos.get('markPrice', btc_price))
            
            # For coin-margined shorts (XBTUSDM, XBTUSDM, etc.)
            if is_inverse and current_qty < 0:
                # Each contract is $1, so absolute value is USD exposure
                short_usd_value = abs(current_qty)
                total_short_usd += short_usd_value
                
                position_details.append({
                    'symbol': symbol,
                    'qty': current_qty,
                    'usd_value': short_usd_value,
                    'mark_price': mark_price,
                    'avg_entry_price': float(pos.get('avgEntryPrice', 0)),
                    'unrealised_pnl': float(pos.get('unrealisedPnl', 0))
                })
        
        # Calculate totals
        total_btc = cold_storage_btc + futures_btc
        
        # Calculate USD values
        cold_storage_usd = cold_storage_btc * btc_price
        futures_btc_usd = futures_btc * btc_price
        total_portfolio_usd = total_btc * btc_price
        
        # Calculate NET BTC exposure
        # BTC exposure = All BTC holdings - Short position USD exposure
        gross_btc_exposure_usd = cold_storage_usd + futures_btc_usd
        net_btc_exposure_usd = gross_btc_exposure_usd - total_short_usd
        
        # USD exposure = Short positions
        usd_exposure = total_short_usd
        
        # Calculate current allocation (based on NET exposure)
        if total_portfolio_usd > 0:
            current_btc_allocation = (net_btc_exposure_usd / total_portfolio_usd) * 100
            current_usd_allocation = (usd_exposure / total_portfolio_usd) * 100
        else:
            current_btc_allocation = 0
            current_usd_allocation = 0
        
        # Calculate targets
        target_btc_usd = total_portfolio_usd * (target_allocation / 100)
        target_usd_exposure = total_portfolio_usd * ((100 - target_allocation) / 100)
        
        # Calculate deviation
        allocation_deviation = current_btc_allocation - target_allocation
        
        # Calculate required adjustment
        # Positive deviation = too much BTC, need to short more (increase USD exposure)
        # Negative deviation = too little BTC, need to close shorts (decrease USD exposure)
        required_usd_adjustment = (allocation_deviation / 100) * total_portfolio_usd
        
        # Determine how much to adjust the short position
        current_short_position_usd = total_short_usd
        target_short_position_usd = target_usd_exposure
        short_position_adjustment = target_short_position_usd - current_short_position_usd
        
        # Calculate contract count for coin-margined (XBTUSDM)
        # Each contract = $1 USD, so contract count = USD amount
        contracts_to_adjust = int(abs(short_position_adjustment))
        
        # Check if rebalancing needed
        needs_rebalancing = abs(allocation_deviation) > rebalance_threshold
        
        return {
            'cold_storage_btc': cold_storage_btc,
            'cold_storage_usd': cold_storage_usd,
            'futures_btc': futures_btc,
            'futures_btc_usd': futures_btc_usd,
            'total_btc': total_btc,
            'total_portfolio_usd': total_portfolio_usd,
            'btc_price': btc_price,
            'gross_btc_exposure_usd': gross_btc_exposure_usd,
            'net_btc_exposure_usd': net_btc_exposure_usd,
            'usd_exposure': usd_exposure,
            'current_short_usd': total_short_usd,
            'current_btc_allocation': current_btc_allocation,
            'current_usd_allocation': current_usd_allocation,
            'target_btc_allocation': target_allocation,
            'target_usd_allocation': 100 - target_allocation,
            'allocation_deviation': allocation_deviation,
            'target_btc_usd': target_btc_usd,
            'target_usd_exposure': target_usd_exposure,
            'required_usd_adjustment': required_usd_adjustment,
            'short_position_adjustment': short_position_adjustment,
            'contracts_to_adjust': contracts_to_adjust,
            'needs_rebalancing': needs_rebalancing,
            'rebalance_threshold': rebalance_threshold,
            'positions': position_details,
            'position_count': len(position_details)
        }


class DisplayManager:
    """Handle display of account and portfolio information"""
    
    @staticmethod
    def display_account_info(account_data: dict):
        """Display futures account information"""
        if not account_data:
            logger.warning("No account data to display")
            return
        
        logger.info("=" * 80)
        logger.info(f"FUTURES ACCOUNT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        logger.info(f"Currency:              {account_data.get('currency', 'N/A')}")
        logger.info(f"Account Equity:        {account_data.get('accountEquity', 'N/A')}")
        logger.info(f"Available Balance:     {account_data.get('availableBalance', 'N/A')}")
        logger.info(f"Margin Balance:        {account_data.get('marginBalance', 'N/A')}")
        logger.info(f"Position Margin:       {account_data.get('positionMargin', 'N/A')}")
        logger.info(f"Order Margin:          {account_data.get('orderMargin', 'N/A')}")
        logger.info(f"Unrealized PNL:        {account_data.get('unrealisedPNL', 'N/A')}")
        logger.info(f"Risk Ratio:            {account_data.get('riskRatio', 'N/A')}")
        logger.info("=" * 80)
    
    @staticmethod
    def display_portfolio_metrics(metrics: dict):
        """Display portfolio metrics and rebalancing recommendations"""
        logger.info("=" * 80)
        logger.info("PORTFOLIO & REBALANCING ANALYSIS")
        logger.info("=" * 80)
        logger.info(f"BTC Price:             ${metrics['btc_price']:,.2f}")
        logger.info("")
        logger.info("BTC Holdings:")
        logger.info(f"  Cold Storage:        {metrics['cold_storage_btc']:.8f} BTC (${metrics['cold_storage_usd']:,.2f})")
        logger.info(f"  Futures Account:     {metrics['futures_btc']:.8f} BTC (${metrics['futures_btc_usd']:,.2f})")
        logger.info(f"  TOTAL:               {metrics['total_btc']:.8f} BTC (${metrics['total_portfolio_usd']:,.2f})")
        logger.info("")
        
        # Show position information
        if metrics['position_count'] > 0:
            logger.info(f"Open Positions ({metrics['position_count']}):")
            for pos in metrics['positions']:
                logger.info(f"  {pos['symbol']}:")
                logger.info(f"    Contracts:         {pos['qty']:,.0f} (SHORT)")
                logger.info(f"    USD Exposure:      ${pos['usd_value']:,.2f}")
                logger.info(f"    Entry Price:       ${pos['avg_entry_price']:,.2f}")
                logger.info(f"    Mark Price:        ${pos['mark_price']:,.2f}")
                logger.info(f"    Unrealized PnL:    {pos['unrealised_pnl']:.8f} BTC")
            logger.info("")
        else:
            logger.info("Open Positions: None")
            logger.info("")
        
        # Show exposure breakdown
        logger.info("Exposure Breakdown:")
        logger.info(f"  Gross BTC:           ${metrics['gross_btc_exposure_usd']:,.2f}")
        logger.info(f"  Short Positions:     ${metrics['current_short_usd']:,.2f}")
        logger.info(f"  NET BTC Exposure:    ${metrics['net_btc_exposure_usd']:,.2f}")
        logger.info(f"  USD Exposure:        ${metrics['usd_exposure']:,.2f}")
        logger.info("")
        
        logger.info("Allocation:")
        logger.info(f"  Current BTC:         {metrics['current_btc_allocation']:.2f}%")
        logger.info(f"  Current USD:         {metrics['current_usd_allocation']:.2f}%")
        logger.info(f"  Target BTC:          {metrics['target_btc_allocation']:.2f}%")
        logger.info(f"  Target USD:          {metrics['target_usd_allocation']:.2f}%")
        logger.info(f"  Deviation:           {metrics['allocation_deviation']:+.2f}%")
        logger.info("")
        
        if metrics['needs_rebalancing']:
            logger.info("[WARNING] REBALANCING NEEDED")
            logger.info(f"  Threshold:           +/-{metrics['rebalance_threshold']:.2f}%")
            
            if metrics['allocation_deviation'] > 0:
                # Too much BTC - need to SHORT more
                logger.info(f"  Action Required:     OPEN SHORT position")
                logger.info(f"  Contracts to SHORT:  {metrics['contracts_to_adjust']:,} contracts (XBTUSDM)")
                logger.info(f"  USD Value:           ${abs(metrics['short_position_adjustment']):,.2f}")
                logger.info(f"  Current Short:       ${metrics['current_short_usd']:,.2f}")
                logger.info(f"  Target Short:        ${metrics['target_usd_exposure']:,.2f}")
                logger.info(f"  Reason:              Too much BTC exposure ({metrics['allocation_deviation']:+.2f}%)")
            else:
                # Too little BTC - need to CLOSE shorts
                logger.info(f"  Action Required:     CLOSE SHORT position")
                logger.info(f"  Contracts to CLOSE:  {metrics['contracts_to_adjust']:,} contracts (XBTUSDM)")
                logger.info(f"  USD Value:           ${abs(metrics['short_position_adjustment']):,.2f}")
                logger.info(f"  Current Short:       ${metrics['current_short_usd']:,.2f}")
                logger.info(f"  Target Short:        ${metrics['target_usd_exposure']:,.2f}")
                logger.info(f"  Reason:              Too little BTC exposure ({metrics['allocation_deviation']:+.2f}%)")
        else:
            logger.info("[OK] PORTFOLIO BALANCED")
            logger.info(f"  Within threshold of +/-{metrics['rebalance_threshold']:.2f}%")
        
        logger.info("=" * 80)


class TradingBot:
    """Main trading bot class"""
    
    def __init__(self):
        """Initialize the trading bot"""
        
        # Fix for .exe compatibility: Set working directory to .exe location
        # This ensures .env, logs, and CSV files are in the same folder as .exe
        import sys
        if getattr(sys, 'frozen', False):
            # Running as compiled .exe
            application_path = os.path.dirname(sys.executable)
        else:
            # Running as .py script
            application_path = os.path.dirname(os.path.abspath(__file__))
        
        # Change to application directory
        os.chdir(application_path)
        
        load_dotenv()
        
        # API credentials
        self.api_key = os.getenv('KUCOIN_API_KEY')
        self.api_secret = os.getenv('KUCOIN_API_SECRET')
        self.api_passphrase = os.getenv('KUCOIN_API_PASSPHRASE')
        self.futures_endpoint = os.getenv('KUCOIN_FUTURES_ENDPOINT', 'https://api-futures.kucoin.com')
        
        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            raise ValueError("Missing API credentials. Please check your .env file")
        
        # Portfolio configuration
        self.cold_storage_btc = float(os.getenv('COLD_STORAGE_BTC_AMOUNT', 0.0))
        self.futures_symbol = os.getenv('FUTURES_SYMBOL', 'XBTUSDM')
        self.target_allocation = float(os.getenv('TARGET_BTC_ALLOCATION', 50.0))
        self.rebalance_threshold = float(os.getenv('REBALANCE_THRESHOLD', 1.0))
        self.fetch_interval = int(os.getenv('FETCH_INTERVAL', 5))
        
        # Automation settings
        self.auto_rebalance = os.getenv('AUTO_REBALANCE', 'false').lower() == 'true'
        self.dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
        self.max_order_usd = float(os.getenv('MAX_ORDER_SIZE_USD', 10000))
        self.min_order_usd = float(os.getenv('MIN_ORDER_SIZE_USD', 100))
        self.leverage = int(os.getenv('LEVERAGE', 1))
        self.margin_mode = os.getenv('MARGIN_MODE', 'ISOLATED').upper()
        self.position_mode = os.getenv('POSITION_MODE', 'ONE_WAY').upper()
        self.auto_set_position_mode = os.getenv('AUTO_SET_POSITION_MODE', 'false').lower() == 'true'
        
        # Order execution settings
        self.order_type = os.getenv('ORDER_TYPE', 'market').lower()
        self.time_in_force = os.getenv('TIME_IN_FORCE', 'IOC').upper()
        self.slippage_pct = float(os.getenv('SLIPPAGE_PERCENTAGE', 0.1))
        self.gtc_timeout = int(os.getenv('GTC_TIMEOUT_SECONDS', 300))
        
        # Logging configuration
        self.file_system_logging_enabled = os.getenv('FILE_SYSTEM_LOGGING_ENABLED', 'true').lower() == 'true'
        self.file_trading_logging_enabled = os.getenv('FILE_TRADING_LOGGING_ENABLED', 'true').lower() == 'true'
        self.file_log_level = os.getenv('FILE_LOG_LEVEL', 'TRADE').upper()
        self.file_log_rotation_hours = int(os.getenv('FILE_LOG_ROTATION_HOURS', 168))
        
        # Setup file logging handler
        if self.file_system_logging_enabled:
            file_handler = RotatingFileHandler(
                'trading_bot.log',
                rotation_hours=self.file_log_rotation_hours,
                enabled=True
            )
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            file_handler.addFilter(LogLevelFilter(self.file_log_level))
            logger.addHandler(file_handler)
        
        # Initialize client
        self.client = KuCoinFuturesClient(
            self.api_key,
            self.api_secret,
            self.api_passphrase,
            self.futures_endpoint
        )
        
        # Initialize order executor
        self.executor = OrderExecutor(
            self.client,
            dry_run=self.dry_run,
            max_order_usd=self.max_order_usd,
            min_order_usd=self.min_order_usd,
            margin_mode=self.margin_mode,
            position_mode=self.position_mode,
            auto_set_position_mode=self.auto_set_position_mode,
            order_type=self.order_type,
            time_in_force=self.time_in_force,
            slippage_pct=self.slippage_pct,
            gtc_timeout_seconds=self.gtc_timeout,
            trade_logging_enabled=self.file_trading_logging_enabled
        )
        
        logger.info("[OK] Trading Bot initialized")
        logger.info(f"[OK] Target: {self.target_allocation}% BTC | Threshold: +/-{self.rebalance_threshold}% | {'DRY RUN' if self.dry_run else 'LIVE MODE'}")
    
    def execute_rebalance(self, metrics: dict) -> bool:
        """
        Execute rebalancing trade based on metrics
        
        Args:
            metrics: Portfolio metrics from PortfolioCalculator
            
        Returns:
            True if rebalancing was executed, False otherwise
        """
        if not metrics['needs_rebalancing']:
            return False
        
        if not self.auto_rebalance:
            logger.info("[INFO] Auto-rebalance DISABLED - manual action required")
            return False
        
        contracts = metrics['contracts_to_adjust']
        adjustment_usd = abs(metrics['short_position_adjustment'])
        
        # Safety check: Skip if adjustment too large
        if adjustment_usd > self.max_order_usd:
            logger.warning(f"[SAFETY] Adjustment ${adjustment_usd:,.2f} exceeds max ${self.max_order_usd:,.2f}")
            logger.warning(f"[SAFETY] Skipping automatic rebalance - manual intervention required")
            return False
        
        # Safety check: Skip if adjustment too small
        if adjustment_usd < self.min_order_usd:
            logger.info(f"[INFO] Adjustment ${adjustment_usd:,.2f} below minimum ${self.min_order_usd:,.2f}")
            logger.info(f"[INFO] Skipping rebalance")
            return False
        
        # Execute rebalance
        if metrics['allocation_deviation'] > 0:
            result = self.executor.open_short(self.futures_symbol, contracts, self.leverage)
        else:
            result = self.executor.close_short(self.futures_symbol, contracts)
        
        if result['success']:
            return True
        else:
            logger.error(f"[ERROR] Rebalance failed: {result.get('error', 'Unknown error')}")
            return False
    
    def run(self):
        """Main bot loop"""
        logger.info("[OK] Bot started - Press Ctrl+C to stop")
        
        try:
            iteration = 0
            while True:
                iteration += 1
                
                # Fetch BTC price
                btc_price = self.client.get_ticker_price(self.futures_symbol)
                
                # Fetch account balance
                account_data = self.client.get_futures_account(currency='XBT')
                
                # Fetch open positions
                positions = self.client.get_positions(currency='XBT')
                
                # Calculate portfolio metrics
                if btc_price > 0 and account_data:
                    metrics = PortfolioCalculator.calculate_metrics(
                        self.cold_storage_btc,
                        account_data,
                        positions,
                        btc_price,
                        self.target_allocation,
                        self.rebalance_threshold
                    )
                    
                    # Minimal status line
                    total_value_usd = metrics['total_portfolio_usd']
                    btc_allocation = metrics['current_btc_allocation']
                    status = "Balanced" if not metrics['needs_rebalancing'] else f"Rebalance needed"
                    logger.info(f"[OK] ${total_value_usd:,.0f} | {btc_allocation:.1f}% BTC | {status}")
                    
                    # Execute automatic rebalancing if enabled
                    if metrics['needs_rebalancing']:
                        executed = self.execute_rebalance(metrics)
                        if executed:
                            logger.info("[INFO] Waiting 10 seconds for order to settle...")
                            time.sleep(10)
                            # Skip the normal sleep interval after rebalancing
                            continue
                
                # Wait before next fetch
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("\n[OK] Bot stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            raise


def main():
    """Main entry point"""
    try:
        bot = TradingBot()
        bot.run()
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())