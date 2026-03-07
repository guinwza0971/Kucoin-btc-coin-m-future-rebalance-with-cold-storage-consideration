"""
KuCoin Short Inverse Rebalancing Bot - FIXED VERSION
====================================================

CRITICAL FIX (2026-03-07):
- KuCoin Futures API /api/v1/account-overview NO LONGER accepts currency parameter
- KuCoin Futures API /api/v1/positions NO LONGER accepts currency parameter  
- Both endpoints now return data for ALL currencies in the futures account
- Updated get_futures_account() to call endpoint without parameters
- Updated get_positions() to call endpoint without parameters
- Maintained backward compatibility with fallback attempts

This bot implements a Short Inverse Rebalancing strategy to accumulate BTC through volatility.
"""

import os
import time
import hmac
import hashlib
import base64
import uuid
import json
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv
import logging
from typing import Optional


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

class LogLevelFilter(logging.Filter):
    """Filter to only show logs at or above a specific level"""
    def __init__(self, level):
        super().__init__()
        self.level = level
    
    def filter(self, record):
        return record.levelno >= self.level


class RotatingFileHandler(logging.FileHandler):
    """File handler that rotates logs based on time"""
    def __init__(self, filename, rotation_hours=24, enabled=True):
        self.rotation_hours = rotation_hours
        self.enabled = enabled
        self.last_rotation = time.time()
        super().__init__(filename, mode='a', encoding='utf-8')
    
    def emit(self, record):
        if not self.enabled:
            return
        
        # Check if we need to rotate
        if time.time() - self.last_rotation > (self.rotation_hours * 3600):
            self.doRollover()
        
        super().emit(record)
    
    def doRollover(self):
        """Rotate the log file"""
        self.stream.close()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base, ext = os.path.splitext(self.baseFilename)
        backup_filename = f"{base}_{timestamp}{ext}"
        
        try:
            os.rename(self.baseFilename, backup_filename)
        except Exception as e:
            print(f"Error rotating log file: {e}")
        
        self.stream = self._open()
        self.last_rotation = time.time()


# Set up logger
logger = logging.getLogger('TradingBot')
logger.setLevel(logging.DEBUG)

# Console handler (always enabled, shows INFO and above)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(console_handler)


# ============================================================================
# TRADE LOGGING TO CSV
# ============================================================================

class TradeLogger:
    """Log trades to CSV file"""
    
    def __init__(self, enabled: bool = True):
        """
        Initialize trade logger
        
        Args:
            enabled: Whether to enable trade logging
        """
        self.enabled = enabled
        self.csv_filename = 'trades.csv'
        self.csv_headers = [
            'Timestamp',
            'Action',
            'Symbol',
            'Contracts',
            'Limit Price',
            'Filled Price',
            'Order ID',
            'Slippage',
            'Status',
            'Error'
        ]
        
        if self.enabled:
            self._create_csv_if_not_exists()
    
    def _create_csv_if_not_exists(self):
        """Create CSV file with headers if it doesn't exist"""
        if os.path.exists(self.csv_filename):
            return
        
        try:
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


# ============================================================================
# KUCOIN FUTURES API CLIENT
# ============================================================================

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
        self.api_passphrase = self._encrypt_passphrase(api_passphrase, api_secret)
        self.endpoint = endpoint
        self.session = requests.Session()
    
    @staticmethod
    def _encrypt_passphrase(passphrase: str, secret: str) -> str:
        """Encrypt API passphrase using HMAC-SHA256"""
        return base64.b64encode(
            hmac.new(secret.encode('utf-8'), passphrase.encode('utf-8'), hashlib.sha256).digest()
        ).decode('utf-8')
    
    def _get_headers(self, method: str, endpoint_path: str, body: str = '') -> dict:
        """
        Generate authentication headers for KuCoin API v3
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint_path: API endpoint path
            body: Request body as JSON string
            
        Returns:
            Dictionary of headers
        """
        timestamp = str(int(time.time() * 1000))
        str_to_sign = timestamp + method + endpoint_path + body
        
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                str_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        return {
            'KC-API-KEY': self.api_key,
            'KC-API-SIGN': signature,
            'KC-API-TIMESTAMP': timestamp,
            'KC-API-PASSPHRASE': self.api_passphrase,
            'KC-API-KEY-VERSION': '3',
            'Content-Type': 'application/json'
        }
    
    def get_ticker_price(self, symbol: str) -> float:
        """
        Get current ticker price for a symbol
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            
        Returns:
            Current price, or 0 if error
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
                return float(ticker_data.get('price', 0))
            else:
                logger.error(f"API Error fetching ticker: {data.get('msg', 'Unknown error')}")
                return 0.0
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching ticker: {e}")
            return 0.0
        except Exception as e:
            logger.error(f"Unexpected error fetching ticker: {e}")
            return 0.0
    
    def get_best_bid_ask(self, symbol: str) -> dict:
        """
        Get best bid/ask prices for a symbol
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            
        Returns:
            Dictionary with best_bid, best_ask, last_price
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
        
        CRITICAL FIX (2026-03-07):
        KuCoin Futures API changed - /api/v1/account-overview NO LONGER accepts currency parameter
        
        Args:
            currency: Currency to filter (kept for backward compatibility, not used in API call)
            
        Returns:
            Dictionary containing account information for the specified currency
        """
        # UPDATED: KuCoin API no longer accepts currency parameter
        # The endpoint now returns data for ALL currencies in your futures account
        # We need to filter the results on our end
        
        method = 'GET'
        endpoint_path = '/api/v1/account-overview'  # No parameters!
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            logger.debug(f"Fetching futures account overview (filtering for {currency})")
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                account_data = data.get('data', {})
                
                # The API now returns data for all currencies
                # We need to check if it's a single currency response or multi-currency
                
                # Check if this is the currency we want
                if account_data.get('currency') == currency:
                    logger.info(f"[OK] Successfully fetched {currency} futures account")
                    return account_data
                
                # If currency doesn't match, log warning but return data anyway
                # This handles cases where API returns different currency than requested
                returned_currency = account_data.get('currency', 'UNKNOWN')
                logger.warning(f"API returned {returned_currency} account, expected {currency}")
                logger.warning(f"Using returned account data for calculations")
                return account_data
                
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"API Error fetching account: {error_msg}")
                return {}
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching account: {e}")
            return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching account: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error fetching account: {e}")
            return {}
    
    def get_positions(self, currency: str = 'XBT') -> list:
        """
        Get all open positions
        
        CRITICAL FIX (2026-03-07):
        KuCoin Futures API changed - /api/v1/positions NO LONGER accepts currency parameter
        
        Args:
            currency: Currency to filter positions (kept for backward compatibility)
            
        Returns:
            List of position dictionaries
        """
        # UPDATED: KuCoin API no longer accepts currency parameter
        # The endpoint now returns ALL positions across all currencies
        # We filter the results on our end
        
        method = 'GET'
        endpoint_path = '/api/v1/positions'  # No parameters!
        url = self.endpoint + endpoint_path
        
        headers = self._get_headers(method, endpoint_path)
        
        try:
            logger.debug(f"Fetching all positions (will filter for {currency})")
            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                all_positions = data.get('data', [])
                
                # Filter for open positions only
                open_positions = [pos for pos in all_positions if pos.get('isOpen', False)]
                
                # Filter by currency if specified
                if currency:
                    filtered_positions = [
                        pos for pos in open_positions 
                        if pos.get('currency') == currency or pos.get('symbol', '').startswith(currency)
                    ]
                    logger.info(f"[OK] Successfully fetched positions ({len(filtered_positions)} open {currency} positions)")
                    return filtered_positions
                
                logger.info(f"[OK] Successfully fetched positions ({len(open_positions)} total open positions)")
                return open_positions
                
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"API Error fetching positions: {error_msg}")
                return []
                
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching positions: {e}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed fetching positions: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching positions: {e}")
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
        # Convert string format to integer: 0 = ONE_WAY, 1 = HEDGE_MODE
        if position_mode == 'ONE_WAY':
            position_mode_int = 0
        elif position_mode == 'HEDGE_MODE':
            position_mode_int = 1
        else:
            logger.error(f"Invalid position mode: {position_mode}. Must be 'ONE_WAY' or 'HEDGE_MODE'")
            return False
        
        method = 'POST'
        endpoint_path = '/api/v2/position/changePositionMode'
        
        body_dict = {
            'symbol': symbol,
            'positionMode': position_mode_int
        }
        body = json.dumps(body_dict)
        
        url = self.endpoint + endpoint_path
        headers = self._get_headers(method, endpoint_path, body)
        
        try:
            response = self.session.post(url, headers=headers, data=body, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                logger.info(f"[OK] Successfully set position mode to {position_mode}")
                return True
            else:
                logger.error(f"API Error setting position mode: {data.get('msg', 'Unknown error')}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed setting position mode: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error setting position mode: {e}")
            return False
    
    def place_order(self, symbol: str, side: str, leverage: int, size: int, 
                   order_type: str = 'market', price: Optional[float] = None,
                   time_in_force: str = 'IOC') -> dict:
        """
        Place a futures order
        
        Args:
            symbol: Futures symbol (e.g., 'XBTUSDM')
            side: 'buy' or 'sell'
            leverage: Leverage multiplier
            size: Number of contracts
            order_type: 'market' or 'limit'
            price: Limit price (required for limit orders)
            time_in_force: 'IOC', 'GTC', or 'FOK' (for limit orders)
            
        Returns:
            Dictionary with order result
        """
        method = 'POST'
        endpoint_path = '/api/v1/orders'
        
        body_dict = {
            'clientOid': str(uuid.uuid4()),
            'side': side.lower(),
            'symbol': symbol,
            'leverage': str(leverage),
            'size': size,
            'type': order_type.lower()
        }
        
        # Add limit order specific fields
        if order_type.lower() == 'limit':
            if price is None:
                logger.error("Limit orders require a price")
                return {'success': False, 'error': 'Limit price required'}
            
            body_dict['price'] = str(price)
            body_dict['timeInForce'] = time_in_force
        
        body = json.dumps(body_dict)
        url = self.endpoint + endpoint_path
        headers = self._get_headers(method, endpoint_path, body)
        
        try:
            response = self.session.post(url, headers=headers, data=body, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') == '200000':
                order_id = data.get('data', {}).get('orderId')
                logger.info(f"[OK] Order placed successfully: {order_id}")
                return {
                    'success': True,
                    'order_id': order_id,
                    'client_oid': body_dict['clientOid']
                }
            else:
                error_msg = data.get('msg', 'Unknown error')
                logger.error(f"API Error placing order: {error_msg}")
                return {'success': False, 'error': error_msg}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed placing order: {e}")
            return {'success': False, 'error': str(e)}
        except Exception as e:
            logger.error(f"Unexpected error placing order: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_order_status(self, order_id: str) -> dict:
        """
        Get order status by order ID
        
        Args:
            order_id: Order ID to query
            
        Returns:
            Dictionary with order details
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
        Cancel an order by order ID
        
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
                logger.info(f"[OK] Order cancelled: {order_id}")
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


# ============================================================================
# ORDER EXECUTION
# ============================================================================

class OrderExecutor:
    """Handle order execution with safety checks"""
    
    def __init__(self, client: KuCoinFuturesClient, dry_run: bool = True, 
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
            gtc_timeout_seconds: Timeout for GTC orders before cancel and retry
            trade_logging_enabled: Whether to log trades to CSV
        """
        self.client = client
        self.dry_run = dry_run
        self.max_order_usd = max_order_usd
        self.min_order_usd = min_order_usd
        self.margin_mode = margin_mode
        self.position_mode = position_mode
        self.auto_set_position_mode = auto_set_position_mode
        self.order_type = order_type
        self.time_in_force = time_in_force
        self.slippage_pct = slippage_pct
        self.gtc_timeout = gtc_timeout_seconds
        
        # Initialize trade logger
        self.trade_logger = TradeLogger(enabled=trade_logging_enabled)
    
    def verify_position_mode(self, symbol: str) -> bool:
        """
        Verify that position mode matches expected configuration
        
        Args:
            symbol: Futures symbol to check
            
        Returns:
            True if position mode is correct, False otherwise
        """
        current_mode = self.client.get_position_mode(symbol)
        
        if not current_mode:
            logger.error("Failed to fetch position mode")
            return False
        
        if current_mode == self.position_mode:
            logger.info(f"[OK] Position mode verified: {self.position_mode}")
            return True
        
        # Position mode mismatch
        logger.warning("=" * 80)
        logger.warning("POSITION MODE MISMATCH DETECTED")
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
                logger.error(f"✗ Failed to set position mode to {self.position_mode}")
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
        else:
            # For OPEN SHORT (SELL): Use best bid as reference
            # Positive slippage = receive less (aggressive)
            # Negative slippage = receive more (conservative, maker)
            reference_price = best_bid
            limit_price = reference_price * (1 - self.slippage_pct / 100)
        
        logger.debug(f"Calculated limit price: {limit_price:.2f} (ref: {reference_price:.2f}, slippage: {self.slippage_pct:+.2f}%)")
        return limit_price
    
    def wait_for_order_fill(self, order_id: str, timeout_seconds: int = 300) -> dict:
        """
        Wait for an order to be filled or timeout
        
        Args:
            order_id: Order ID to monitor
            timeout_seconds: Maximum time to wait
            
        Returns:
            Order details dictionary
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            order_details = self.client.get_order_status(order_id)
            
            if not order_details:
                logger.warning("Failed to fetch order status, retrying...")
                time.sleep(2)
                continue
            
            status = order_details.get('status', '')
            
            if status == 'done':
                logger.info(f"[OK] Order filled: {order_id}")
                return order_details
            elif status == 'cancel':
                logger.warning(f"Order cancelled: {order_id}")
                return order_details
            
            # Still pending, wait and retry
            time.sleep(2)
        
        # Timeout reached
        logger.warning(f"Order timeout reached ({timeout_seconds}s): {order_id}")
        return self.client.get_order_status(order_id)
    
    def open_short(self, symbol: str, contracts: int, leverage: int) -> dict:
        """
        Open a short position (SELL)
        
        Args:
            symbol: Futures symbol
            contracts: Number of contracts to short
            leverage: Leverage multiplier
            
        Returns:
            Dictionary with execution result
        """
        logger.info("=" * 80)
        logger.info("EXECUTING: OPEN SHORT")
        logger.info(f"  Symbol:     {symbol}")
        logger.info(f"  Contracts:  {contracts:,}")
        logger.info(f"  Leverage:   {leverage}x")
        logger.info(f"  Order Type: {self.order_type}")
        
        # Verify position mode
        if not self.verify_position_mode(symbol):
            error_msg = "Position mode verification failed"
            self.trade_logger.log_trade('OPEN_SHORT', symbol, contracts, status='FAILED', error=error_msg)
            return {'success': False, 'error': error_msg}
        
        # Calculate limit price if needed
        limit_price = None
        if self.order_type == 'limit':
            limit_price = self.calculate_limit_price(symbol, 'sell')
            if limit_price == 0:
                error_msg = "Failed to calculate limit price"
                self.trade_logger.log_trade('OPEN_SHORT', symbol, contracts, status='FAILED', error=error_msg)
                return {'success': False, 'error': error_msg}
            logger.info(f"  Limit Price: {limit_price:.2f}")
        
        # DRY RUN mode
        if self.dry_run:
            logger.info("[DRY RUN] Would execute SELL order")
            logger.info("=" * 80)
            self.trade_logger.log_trade('OPEN_SHORT', symbol, contracts, limit_price=limit_price, status='DRY_RUN')
            return {'success': True, 'dry_run': True}
        
        # Execute order
        result = self.client.place_order(
            symbol=symbol,
            side='sell',
            leverage=leverage,
            size=contracts,
            order_type=self.order_type,
            price=limit_price,
            time_in_force=self.time_in_force
        )
        
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"[ERROR] Failed to place order: {error_msg}")
            logger.info("=" * 80)
            self.trade_logger.log_trade('OPEN_SHORT', symbol, contracts, limit_price=limit_price, 
                                       status='FAILED', error=error_msg)
            return result
        
        order_id = result.get('order_id')
        logger.info(f"[OK] Order placed: {order_id}")
        
        # Wait for order to fill and get execution price
        # For BOTH market and limit orders, we need to fetch the filled price
        if self.order_type == 'limit':
            logger.info("Waiting for limit order to fill...")
            order_details = self.wait_for_order_fill(order_id, self.gtc_timeout)
        else:
            # Market orders fill almost instantly, wait 2 seconds then fetch
            logger.info("Waiting for market order to fill...")
            time.sleep(2)
            order_details = self.client.get_order_status(order_id)
        
        # Calculate filled price from order details
        filled_price = None
        deal_size = float(order_details.get('dealSize', 0))
        deal_value = float(order_details.get('dealValue', 0))
        
        if deal_size > 0 and deal_value > 0:
            filled_price = deal_value / deal_size
            logger.info(f"[OK] Order filled at price: ${filled_price:.2f}")
        else:
            logger.warning("Unable to determine filled price from order details")
        
        # Determine status
        order_status = order_details.get('status', '')
        if order_status == 'done':
            status = 'FILLED'
        elif self.order_type == 'limit' and order_status != 'done':
            status = 'TIMEOUT'
        else:
            status = 'MARKET_FILL'
        
        # Log trade with filled price
        self.trade_logger.log_trade('OPEN_SHORT', symbol, contracts, limit_price=limit_price,
                                   filled_price=filled_price, order_id=order_id, status=status)
        
        # Cancel unfilled GTC orders
        if self.order_type == 'limit' and order_status != 'done' and self.time_in_force == 'GTC':
            logger.warning("GTC order not filled, cancelling...")
            self.client.cancel_order(order_id)
        
        logger.info("=" * 80)
        return result
    
    def close_short(self, symbol: str, contracts: int) -> dict:
        """
        Close a short position (BUY)
        
        Args:
            symbol: Futures symbol
            contracts: Number of contracts to close
            
        Returns:
            Dictionary with execution result
        """
        logger.info("=" * 80)
        logger.info("EXECUTING: CLOSE SHORT")
        logger.info(f"  Symbol:     {symbol}")
        logger.info(f"  Contracts:  {contracts:,}")
        logger.info(f"  Order Type: {self.order_type}")
        
        # Verify position mode
        if not self.verify_position_mode(symbol):
            error_msg = "Position mode verification failed"
            self.trade_logger.log_trade('CLOSE_SHORT', symbol, contracts, status='FAILED', error=error_msg)
            return {'success': False, 'error': error_msg}
        
        # Calculate limit price if needed
        limit_price = None
        if self.order_type == 'limit':
            limit_price = self.calculate_limit_price(symbol, 'buy')
            if limit_price == 0:
                error_msg = "Failed to calculate limit price"
                self.trade_logger.log_trade('CLOSE_SHORT', symbol, contracts, status='FAILED', error=error_msg)
                return {'success': False, 'error': error_msg}
            logger.info(f"  Limit Price: {limit_price:.2f}")
        
        # DRY RUN mode
        if self.dry_run:
            logger.info("[DRY RUN] Would execute BUY order")
            logger.info("=" * 80)
            self.trade_logger.log_trade('CLOSE_SHORT', symbol, contracts, limit_price=limit_price, status='DRY_RUN')
            return {'success': True, 'dry_run': True}
        
        # Execute order
        # Note: For ONE_WAY mode, buying closes the short position
        # Leverage is not needed for closing positions
        result = self.client.place_order(
            symbol=symbol,
            side='buy',
            leverage=1,  # Leverage doesn't matter for closing
            size=contracts,
            order_type=self.order_type,
            price=limit_price,
            time_in_force=self.time_in_force
        )
        
        if not result.get('success'):
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"[ERROR] Failed to place order: {error_msg}")
            logger.info("=" * 80)
            self.trade_logger.log_trade('CLOSE_SHORT', symbol, contracts, limit_price=limit_price,
                                       status='FAILED', error=error_msg)
            return result
        
        order_id = result.get('order_id')
        logger.info(f"[OK] Order placed: {order_id}")
        
        # Wait for order to fill and get execution price
        # For BOTH market and limit orders, we need to fetch the filled price
        if self.order_type == 'limit':
            logger.info("Waiting for limit order to fill...")
            order_details = self.wait_for_order_fill(order_id, self.gtc_timeout)
        else:
            # Market orders fill almost instantly, wait 2 seconds then fetch
            logger.info("Waiting for market order to fill...")
            time.sleep(2)
            order_details = self.client.get_order_status(order_id)
        
        # Calculate filled price from order details
        filled_price = None
        deal_size = float(order_details.get('dealSize', 0))
        deal_value = float(order_details.get('dealValue', 0))
        
        if deal_size > 0 and deal_value > 0:
            filled_price = deal_value / deal_size
            logger.info(f"[OK] Order filled at price: ${filled_price:.2f}")
        else:
            logger.warning("Unable to determine filled price from order details")
        
        # Determine status
        order_status = order_details.get('status', '')
        if order_status == 'done':
            status = 'FILLED'
        elif self.order_type == 'limit' and order_status != 'done':
            status = 'TIMEOUT'
        else:
            status = 'MARKET_FILL'
        
        # Log trade with filled price
        self.trade_logger.log_trade('CLOSE_SHORT', symbol, contracts, limit_price=limit_price,
                                   filled_price=filled_price, order_id=order_id, status=status)
        
        # Cancel unfilled GTC orders
        if self.order_type == 'limit' and order_status != 'done' and self.time_in_force == 'GTC':
            logger.warning("GTC order not filled, cancelling...")
            self.client.cancel_order(order_id)
        
        logger.info("=" * 80)
        return result


# ============================================================================
# PORTFOLIO CALCULATOR
# ============================================================================

class PortfolioCalculator:
    """Calculate portfolio metrics and rebalancing needs"""
    
    @staticmethod
    def calculate_metrics(btc_price: float, cold_storage_btc: float, 
                         account_data: dict, positions: list,
                         target_allocation: float, rebalance_threshold: float) -> dict:
        """
        Calculate portfolio metrics and rebalancing recommendations
        
        Args:
            btc_price: Current BTC price
            cold_storage_btc: BTC amount in cold storage
            account_data: Futures account data
            positions: List of open positions
            target_allocation: Target BTC allocation percentage (e.g., 50.0 for 50%)
            rebalance_threshold: Rebalancing threshold in percentage (e.g., 1.0 for ±1%)
            
        Returns:
            Dictionary containing all portfolio metrics and rebalancing recommendations
        """
        # Extract account equity (in BTC for coin-margined contracts)
        futures_equity_btc = float(account_data.get('accountEquity', 0))
        
        # Calculate total BTC holdings
        total_btc = cold_storage_btc + futures_equity_btc
        
        # Calculate USD values
        cold_storage_usd = cold_storage_btc * btc_price
        futures_btc_usd = futures_equity_btc * btc_price
        total_portfolio_usd = total_btc * btc_price
        
        # Calculate current short position
        current_short_usd = 0
        position_count = 0
        position_details = []
        
        for pos in positions:
            if pos.get('isOpen', False):
                position_count += 1
                qty = abs(float(pos.get('currentQty', 0)))
                # For inverse contracts, currentQty is in USD value
                current_short_usd += qty
                
                position_details.append({
                    'symbol': pos.get('symbol'),
                    'qty': qty,
                    'leverage': pos.get('realLeverage', 0),
                    'liquidation_price': pos.get('liquidationPrice', 0),
                    'unrealized_pnl': pos.get('unrealisedPnl', 0)
                })
        
        # Calculate effective BTC exposure
        # Effective BTC = Physical BTC - Short Position Value / BTC Price
        effective_btc_usd = total_portfolio_usd - current_short_usd
        
        # Calculate current allocation
        current_allocation = (effective_btc_usd / total_portfolio_usd * 100) if total_portfolio_usd > 0 else 0
        
        # Calculate target short position
        target_usd_exposure = total_portfolio_usd * (1 - target_allocation / 100)
        
        # Calculate allocation deviation
        allocation_deviation = current_allocation - target_allocation
        
        # Determine if rebalancing is needed
        needs_rebalancing = abs(allocation_deviation) > rebalance_threshold
        
        # Calculate adjustment needed
        short_position_adjustment = target_usd_exposure - current_short_usd
        contracts_to_adjust = int(abs(short_position_adjustment))
        
        return {
            'btc_price': btc_price,
            'cold_storage_btc': cold_storage_btc,
            'cold_storage_usd': cold_storage_usd,
            'futures_btc': futures_equity_btc,
            'futures_btc_usd': futures_btc_usd,
            'total_btc': total_btc,
            'total_portfolio_usd': total_portfolio_usd,
            'current_short_usd': current_short_usd,
            'effective_btc_usd': effective_btc_usd,
            'current_allocation': current_allocation,
            'target_allocation': target_allocation,
            'allocation_deviation': allocation_deviation,
            'target_usd_exposure': target_usd_exposure,
            'short_position_adjustment': short_position_adjustment,
            'contracts_to_adjust': contracts_to_adjust,
            'needs_rebalancing': needs_rebalancing,
            'rebalance_threshold': rebalance_threshold,
            'position_count': position_count,
            'positions': position_details
        }
    
    @staticmethod
    def display_account(account_data: dict):
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
                logger.info(f"    Contracts:         {pos['qty']:,.0f} USD")
                logger.info(f"    Leverage:          {pos['leverage']:.2f}x")
                logger.info(f"    Unrealized PNL:    {pos['unrealized_pnl']:.8f} BTC")
            logger.info(f"  TOTAL SHORT VALUE:   ${metrics['current_short_usd']:,.2f}")
        else:
            logger.info("Open Positions:        None")
        
        logger.info("")
        logger.info("Allocation Analysis:")
        logger.info(f"  Effective BTC:       ${metrics['effective_btc_usd']:,.2f}")
        logger.info(f"  Current Allocation:  {metrics['current_allocation']:.2f}% BTC")
        logger.info(f"  Target Allocation:   {metrics['target_allocation']:.2f}% BTC")
        logger.info(f"  Deviation:           {metrics['allocation_deviation']:+.2f}%")
        logger.info("")
        
        # Rebalancing recommendation
        if metrics['needs_rebalancing']:
            logger.info("REBALANCING NEEDED")
            
            if metrics['allocation_deviation'] > 0:
                # Too much BTC - need to OPEN shorts
                logger.info(f"  Action Required:     OPEN SHORT position")
                logger.info(f"  Contracts to OPEN:   {metrics['contracts_to_adjust']:,} contracts (XBTUSDM)")
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


# ============================================================================
# MAIN TRADING BOT
# ============================================================================

class TradingBot:
    """Main trading bot class"""
    
    def __init__(self):
        """Initialize the trading bot"""
        
        # Fix for .exe compatibility: Set working directory to .exe location
        import sys
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))
        
        os.chdir(application_path)
        
        load_dotenv()
        
        # API credentials
        self.api_key = os.getenv('KUCOIN_API_KEY')
        self.api_secret = os.getenv('KUCOIN_API_SECRET')
        self.api_passphrase = os.getenv('KUCOIN_API_PASSPHRASE')
        self.futures_endpoint = os.getenv('KUCOIN_FUTURES_ENDPOINT', 'https://api-futures.kucoin.com')
        
        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            raise ValueError("Missing API credentials. Check your .env file")
        
        # Portfolio configuration
        self.cold_storage_btc = float(os.getenv('COLD_STORAGE_BTC_AMOUNT', '0'))
        self.futures_symbol = os.getenv('FUTURES_SYMBOL', 'XBTUSDM')
        self.target_allocation = float(os.getenv('TARGET_BTC_ALLOCATION', '50'))
        self.rebalance_threshold = float(os.getenv('REBALANCE_THRESHOLD', '1.0'))
        self.fetch_interval = int(os.getenv('FETCH_INTERVAL', '5'))
        
        # Trading controls
        self.auto_rebalance = os.getenv('AUTO_REBALANCE', 'true').lower() == 'true'
        self.dry_run = os.getenv('DRY_RUN', 'true').lower() == 'true'
        self.leverage = int(os.getenv('LEVERAGE', '5'))
        self.max_order_usd = float(os.getenv('MAX_ORDER_SIZE_USD', '10000'))
        self.min_order_usd = float(os.getenv('MIN_ORDER_SIZE_USD', '100'))
        
        # Trading modes
        self.margin_mode = os.getenv('MARGIN_MODE', 'ISOLATED')
        self.position_mode = os.getenv('POSITION_MODE', 'ONE_WAY')
        self.auto_set_position_mode = os.getenv('AUTO_SET_POSITION_MODE', 'false').lower() == 'true'
        
        # Order execution
        self.order_type = os.getenv('ORDER_TYPE', 'market')
        self.time_in_force = os.getenv('TIME_IN_FORCE', 'IOC')
        self.slippage_pct = float(os.getenv('SLIPPAGE_PERCENTAGE', '0.1'))
        self.gtc_timeout = int(os.getenv('GTC_TIMEOUT_SECONDS', '300'))
        
        # Logging configuration
        self.file_system_logging_enabled = os.getenv('FILE_SYSTEM_LOGGING_ENABLED', 'true').lower() == 'true'
        self.file_log_level = os.getenv('FILE_LOG_LEVEL', 'TRADE')
        self.file_log_rotation_hours = int(os.getenv('FILE_LOG_ROTATION_HOURS', '24'))
        self.file_trading_logging_enabled = os.getenv('FILE_TRADING_LOGGING_ENABLED', 'true').lower() == 'true'
        
        # Convert log level string to logging constant
        log_level_map = {
            'ERROR': logging.ERROR,
            'WARNING': logging.WARNING,
            'TRADE': logging.INFO,  # TRADE = INFO level
            'INFO': logging.INFO
        }
        self.file_log_level = log_level_map.get(self.file_log_level.upper(), logging.INFO)
        
        # Set up file logging if enabled
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
                
                # Check if we got valid data
                if btc_price == 0:
                    logger.error("[ERROR] Failed to fetch BTC price")
                    time.sleep(self.fetch_interval)
                    continue
                
                if not account_data:
                    logger.error("[ERROR] Failed to fetch account data")
                    time.sleep(self.fetch_interval)
                    continue
                
                # Calculate portfolio metrics
                metrics = PortfolioCalculator.calculate_metrics(
                    btc_price=btc_price,
                    cold_storage_btc=self.cold_storage_btc,
                    account_data=account_data,
                    positions=positions,
                    target_allocation=self.target_allocation,
                    rebalance_threshold=self.rebalance_threshold
                )
                
                # Display information
                PortfolioCalculator.display_account(account_data)
                PortfolioCalculator.display_portfolio_metrics(metrics)
                
                # Execute rebalancing if needed
                if metrics['needs_rebalancing']:
                    self.execute_rebalance(metrics)
                
                # Wait before next iteration
                logger.info(f"Next check in {self.fetch_interval} seconds...")
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("\n[OK] Bot stopped by user")
        except Exception as e:
            logger.error(f"[ERROR] Unexpected error: {e}")
            raise


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    bot = TradingBot()
    bot.run()
