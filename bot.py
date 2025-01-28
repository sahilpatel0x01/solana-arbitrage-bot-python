import nest_asyncio
import asyncio
import aiohttp
from solana.rpc.async_api import AsyncClient
from decimal import Decimal
import time
import json
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
from base58 import b58encode, b58decode

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('arbitrage.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TokenConfig:
    """Configuration for tokens and their official contracts"""
    TOKENS = {
        "SOL": "So11111111111111111111111111111111111111112",
        "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
        "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfkmzuaPXkBHRYh",
        "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm"
    }
    
    DECIMALS = {
        "SOL": 9,
        "USDC": 6,
        "USDT": 6,
        "RAY": 6,
        "BONK": 5,
        "JTO": 6,
        "WIF": 6
    }

class DexConfig:
    """Configuration for DEX APIs and endpoints"""
    # Raydium V3
    RAYDIUM_API = "https://api-v3.raydium.io/main"
    RAYDIUM_POOLS = f"{RAYDIUM_API}/pools/info/list"
    
    # Jupiter (Aggregator)
    JUPITER_API = "https://quote-api.jup.ag/v6"
    JUPITER_PRICE = f"{JUPITER_API}/price"
    JUPITER_QUOTE = f"{JUPITER_API}/quote"
    
    # Meteora
    METEORA_API = "https://api.meteora.ag/v1"
    METEORA_POOLS = f"{METEORA_API}/pools"
    
    # Phoenix
    PHOENIX_API = "https://phoenix.trade/api/v1"
    PHOENIX_MARKETS = f"{PHOENIX_API}/markets"

class ArbConfig:
    """Configuration for arbitrage parameters"""
    MIN_PROFIT_THRESHOLD = Decimal('0.008')     # 0.8% minimum profit
    MIN_LIQUIDITY_USD = Decimal('50000')        # $50,000 minimum liquidity
    MAX_SLIPPAGE = Decimal('0.002')            # 0.2% maximum slippage
    GAS_ESTIMATE_SOL = Decimal('0.002')        # Estimated gas cost in SOL
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 1  # seconds
    REQUEST_TIMEOUT = 5  # seconds

class BaseClient:
    """Base client with common functionality"""
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.retry_attempts = ArbConfig.MAX_RETRY_ATTEMPTS
        self.retry_delay = ArbConfig.RETRY_DELAY

    async def _make_request(self, url: str, method: str = 'GET', **kwargs) -> Optional[dict]:
        """Make HTTP request with retry logic"""
        for attempt in range(self.retry_attempts):
            try:
                async with self.session.request(
                    method, 
                    url, 
                    timeout=ArbConfig.REQUEST_TIMEOUT,
                    **kwargs
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    logger.warning(f"Request failed: {url}, status: {response.status}")
                    
            except asyncio.TimeoutError:
                logger.warning(f"Request timeout: {url}")
            except Exception as e:
                logger.error(f"Request error: {url}, error: {str(e)}")
                
            if attempt < self.retry_attempts - 1:
                await asyncio.sleep(self.retry_delay * (attempt + 1))
                
        return None

class RaydiumV3Client(BaseClient):
    """Client for Raydium V3 DEX"""
    async def get_pools(self) -> List[dict]:
        try:
            response = await self._make_request(DexConfig.RAYDIUM_POOLS)
            if not response or not response.get('success'):
                logger.error(f"Raydium API error: {response.get('msg') if response else 'No response'}")
                return []
                
            pools = []
            for pool in response['data']:
                try:
                    if (pool['baseMint'] not in TokenConfig.TOKENS.values() or 
                        pool['quoteMint'] not in TokenConfig.TOKENS.values()):
                        continue
                        
                    pool_info = {
                        'dex': 'Raydium',
                        'base_token': next(k for k, v in TokenConfig.TOKENS.items() if v == pool['baseMint']),
                        'quote_token': next(k for k, v in TokenConfig.TOKENS.items() if v == pool['quoteMint']),
                        'price': Decimal(str(pool['price'])),
                        'liquidity_base': Decimal(str(pool['baseReserve'])) / Decimal(str(10 ** TokenConfig.DECIMALS[pool['baseMint']])),
                        'liquidity_quote': Decimal(str(pool['quoteReserve'])) / Decimal(str(10 ** TokenConfig.DECIMALS[pool['quoteMint']])),
                        'fee_rate': Decimal(str(pool.get('fee', 0.0025))),
                        'pool_id': pool['id']
                    }
                    pools.append(pool_info)
                except (KeyError, ValueError) as e:
                    logger.warning(f"Error processing Raydium pool: {e}")
                    continue
                    
            return pools
        except Exception as e:
            logger.error(f"Error fetching Raydium pools: {e}")
            return []

class JupiterClient(BaseClient):
    """Client for Jupiter Aggregator"""
    async def get_prices(self, token_pairs: List[Tuple[str, str]]) -> List[dict]:
        prices = []
        for base, quote in token_pairs:
            try:
                params = {
                    'inputMint': TokenConfig.TOKENS[base],
                    'outputMint': TokenConfig.TOKENS[quote],
                    'amount': '1000000'  # 1 unit in lowest denomination
                }
                response = await self._make_request(f"{DexConfig.JUPITER_PRICE}", params=params)
                
                if response and 'data' in response:
                    prices.append({
                        'dex': 'Jupiter',
                        'base_token': base,
                        'quote_token': quote,
                        'price': Decimal(str(response['data']['price'])),
                        'liquidity_base': Decimal('inf'),  # Jupiter aggregates liquidity
                        'liquidity_quote': Decimal('inf')
                    })
            except Exception as e:
                logger.error(f"Error fetching Jupiter price for {base}-{quote}: {e}")
                
        return prices

class MeteoraClient(BaseClient):
    """Client for Meteora DEX"""
    async def get_pools(self) -> List[dict]:
        try:
            response = await self._make_request(DexConfig.METEORA_POOLS)
            if not response or 'pools' not in response:
                return []
                
            pools = []
            for pool in response['pools']:
                try:
                    if (pool['token0'] not in TokenConfig.TOKENS.values() or 
                        pool['token1'] not in TokenConfig.TOKENS.values()):
                        continue
                        
                    pool_info = {
                        'dex': 'Meteora',
                        'base_token': next(k for k, v in TokenConfig.TOKENS.items() if v == pool['token0']),
                        'quote_token': next(k for k, v in TokenConfig.TOKENS.items() if v == pool['token1']),
                        'price': Decimal(str(pool['price'])),
                        'liquidity_base': Decimal(str(pool['reserve0'])),
                        'liquidity_quote': Decimal(str(pool['reserve1'])),
                        'fee_rate': Decimal(str(pool.get('fee', 0.003))),
                        'pool_id': pool['address']
                    }
                    pools.append(pool_info)
                except (KeyError, ValueError) as e:
                    logger.warning(f"Error processing Meteora pool: {e}")
                    continue
                    
            return pools
        except Exception as e:
            logger.error(f"Error fetching Meteora pools: {e}")
            return []

class ArbitrageBot:
    """Main arbitrage bot class"""
    def __init__(self, solana_client: AsyncClient):
        self.solana_client = solana_client
        self.session = None
        self.clients = {}
        self.last_sol_price = None
        self.sol_price_timestamp = 0
        self.monitoring = set()  # Track monitored pairs

    async def initialize(self):
        """Initialize HTTP session and DEX clients"""
        self.session = aiohttp.ClientSession()
        self.clients = {
            'raydium': RaydiumV3Client(self.session),
            'jupiter': JupiterClient(self.session),
            'meteora': MeteoraClient(self.session)
        }

    async def close(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()

    async def get_sol_price(self) -> Decimal:
        """Get SOL price with caching"""
        current_time = time.time()
        if self.last_sol_price and current_time - self.sol_price_timestamp < 60:
            return self.last_sol_price
            
        try:
            response = await self.clients['jupiter']._make_request(
                f"{DexConfig.JUPITER_PRICE}",
                params={'inputMint': TokenConfig.TOKENS['SOL'], 
                       'outputMint': TokenConfig.TOKENS['USDC'], 
                       'amount': '1000000000'}  # 1 SOL
            )
            if response and 'data' in response:
                self.last_sol_price = Decimal(str(response['data']['price']))
                self.sol_price_timestamp = current_time
                return self.last_sol_price
        except Exception as e:
            logger.error(f"Error fetching SOL price: {e}")
            
        return Decimal('0')

    async def calculate_profit(self, buy_pool: dict, sell_pool: dict, amount: Decimal) -> Tuple[Decimal, Decimal]:
        """Calculate potential profit and optimal trade size for an arbitrage opportunity"""
        # Calculate execution prices with slippage
        buy_price_with_slip = buy_pool['price'] * (1 + ArbConfig.MAX_SLIPPAGE)
        sell_price_with_slip = sell_pool['price'] * (1 - ArbConfig.MAX_SLIPPAGE)
        
        # Calculate maximum trade size based on liquidity
        max_trade = min(
            buy_pool['liquidity_base'],
            sell_pool['liquidity_base'],
            buy_pool['liquidity_quote'] / buy_pool['price'],
            sell_pool['liquidity_quote'] / sell_pool['price']
        )
        
        # Limit trade size based on liquidity threshold
        optimal_trade = min(max_trade, ArbConfig.MIN_LIQUIDITY_USD / buy_price_with_slip)
        
        # Calculate gross profit
        cost = optimal_trade * buy_price_with_slip
        revenue = optimal_trade * sell_price_with_slip
        gross_profit = revenue - cost
        
        # Estimate gas costs in USD
        sol_price = await self.get_sol_price()
        gas_cost_usd = ArbConfig.GAS_ESTIMATE_SOL * sol_price
        
        # Calculate net profit
        net_profit = gross_profit - gas_cost_usd
        
        return net_profit, optimal_trade

    async def find_arbitrage_opportunities(self) -> List[dict]:
        """Find arbitrage opportunities across DEXes"""
        opportunities = []
        
        # Fetch pools from all DEXes
        all_pools = []
        all_pools.extend(await self.clients['raydium'].get_pools())
        
        # Get relevant token pairs for Jupiter
        token_pairs = [(base, quote) 
                      for base in TokenConfig.TOKENS 
                      for quote in TokenConfig.TOKENS 
                      if base < quote]  # Ensure unique pairs
        all_pools.extend(await self.clients['jupiter'].get_prices(token_pairs))
        
        # Add Meteora pools
        all_pools.extend(await self.clients['meteora'].get_pools())
        
        # Group pools by token pair
        pools_by_pair = {}
        for pool in all_pools:
            pair = tuple(sorted([pool['base_token'], pool['quote_token']]))
            if pair not in pools_by_pair:
                pools_by_pair[pair] = []
            pools_by_pair[pair].append(pool)
        
        # Find arbitrage opportunities
        for pair, pools in pools_by_pair.items():
            if len(pools) < 2:
                continue
                
            for i, pool1 in enumerate(pools):
                for pool2 in pools[i+1:]:
                    # Skip if same DEX
                    if pool1['dex'] == pool2['dex']:
                        continue
                        
                    # Check if prices differ enough for arbitrage
                    price_diff = abs(pool1['price'] - pool2['price']) / min(pool1['price'], pool2['price'])
                    
                    if price_diff > ArbConfig.MIN_PROFIT_THRESHOLD:
                        # Determine buy and sell sides
                        buy_pool = pool1 if pool1['price'] < pool2['price'] else pool2
                        sell_pool = pool2 if pool1['price'] < pool2['price'] else pool1
                        
                        # Calculate potential profit and optimal trade size
                        profit, trade_amount = await self.calculate_profit(buy_pool, sell_pool, Decimal('1'))
                        
                        if profit > 0:
                            opportunity = {
                                'buy_dex': buy_pool['dex'],
                                'sell_dex': sell_pool['dex'],
                                'base_token': pair[0],
                                'quote_token': pair[1],
                                'buy_price': float(buy_pool['price']),
                                'sell_price': float(sell_pool['price']),
                                'trade_amount': float(trade_amount),
                                'estimated_profit_usd': float(profit),
                                'buy_pool': buy_pool.get('pool_id', ''),
                                'sell_pool': sell_pool.get('pool_id', ''),
                                'timestamp': datetime.now().isoformat()
                            }
                            
                            # Add to monitoring if significant opportunity
                            pair_key = f"{pair[0]}/{pair[1]}"
                            if profit > ArbConfig.MIN_PROFIT_THRESHOLD * 2:
                                self.monitoring.add(pair_key)
                            
                            opportunities.append(opportunity)
        
        return sorted(opportunities, key=lambda x: x['estimated_profit_usd'], reverse=True)

    async def execute_arbitrage(self, opportunity: dict) -> bool:
        """Execute an arbitrage trade"""
        logger.info(f"Executing arbitrage: {json.dumps(opportunity, indent=2)}")
        
        try:
            # Validate opportunity is still profitable
            buy_price_check = await self.get_current_price(
                opportunity['buy_dex'],
                opportunity['base_token'],
                opportunity['quote_token']
            )
            sell_price_check = await self.get_current_price(
                opportunity['sell_dex'],
                opportunity['base_token'],
                opportunity['quote_token']
            )
            
            if not buy_price_check or not sell_price_check:
                logger.warning("Failed to validate current prices")
                return False
            
            # Check if opportunity is still profitable
            price_diff = (sell_price_check - buy_price_check) / buy_price_check
            if price_diff < ArbConfig.MIN_PROFIT_THRESHOLD:
                logger.warning("Opportunity no longer profitable")
                return False
            
            # Execute trades
            # Note: This is where you would implement the actual trading logic
            # using Solana transactions. This would involve:
            
            # 1. Create transaction for buying on first DEX
            # buy_tx = await self.create_buy_transaction(
            #     dex=opportunity['buy_dex'],
            #     pool_id=opportunity['buy_pool'],
            #     amount=opportunity['trade_amount']
            # )
            
            # 2. Create transaction for selling on second DEX
            # sell_tx = await self.create_sell_transaction(
            #     dex=opportunity['sell_dex'],
            #     pool_id=opportunity['sell_pool'],
            #     amount=opportunity['trade_amount']
            # )
            
            # 3. Bundle transactions
            # tx_bundle = await self.create_transaction_bundle([buy_tx, sell_tx])
            
            # 4. Sign and send transaction
            # signature = await self.solana_client.send_transaction(tx_bundle)
            
            # 5. Wait for confirmation
            # await self.solana_client.confirm_transaction(signature)
            
            # For now, we'll just log the attempt
            logger.info(f"Would execute: Buy {opportunity['trade_amount']} {opportunity['base_token']} " +
                       f"on {opportunity['buy_dex']} at {opportunity['buy_price']}, " +
                       f"Sell on {opportunity['sell_dex']} at {opportunity['sell_price']}")
            
            return False  # Change to True when actual trading is implemented
            
        except Exception as e:
            logger.error(f"Error executing arbitrage: {e}")
            return False

    async def get_current_price(self, dex: str, base_token: str, quote_token: str) -> Optional[Decimal]:
        """Get current price for a token pair on a specific DEX"""
        try:
            if dex == 'Raydium':
                pools = await self.clients['raydium'].get_pools()
                for pool in pools:
                    if (pool['base_token'] == base_token and 
                        pool['quote_token'] == quote_token):
                        return pool['price']
            elif dex == 'Jupiter':
                prices = await self.clients['jupiter'].get_prices([(base_token, quote_token)])
                if prices:
                    return prices[0]['price']
            elif dex == 'Meteora':
                pools = await self.clients['meteora'].get_pools()
                for pool in pools:
                    if (pool['base_token'] == base_token and 
                        pool['quote_token'] == quote_token):
                        return pool['price']
                        
        except Exception as e:
            logger.error(f"Error getting current price for {base_token}/{quote_token} on {dex}: {e}")
        
        return None

    async def monitor_opportunities(self):
        """Monitor specific trading pairs for opportunities"""
        while self.monitoring:
            opportunities = await self.find_arbitrage_opportunities()
            
            # Filter opportunities for monitored pairs
            monitored_opps = [
                opp for opp in opportunities
                if f"{opp['base_token']}/{opp['quote_token']}" in self.monitoring
            ]
            
            for opp in monitored_opps:
                if opp['estimated_profit_usd'] > ArbConfig.MIN_PROFIT_THRESHOLD * 3:
                    logger.info(f"High profit opportunity detected: {json.dumps(opp, indent=2)}")
                    
            await asyncio.sleep(1)

    async def run(self):
        """Main bot loop"""
        await self.initialize()
        
        try:
            # Start monitoring task
            monitor_task = asyncio.create_task(self.monitor_opportunities())
            
            while True:
                try:
                    opportunities = await self.find_arbitrage_opportunities()
                    
                    for opportunity in opportunities[:5]:  # Look at top 5 opportunities
                        logger.info(f"Found opportunity: {json.dumps(opportunity, indent=2)}")
                        
                        if opportunity['estimated_profit_usd'] > ArbConfig.MIN_PROFIT_THRESHOLD:
                            success = await self.execute_arbitrage(opportunity)
                            if success:
                                logger.info("Arbitrage executed successfully")
                            else:
                                logger.warning("Arbitrage execution failed")
                    
                    await asyncio.sleep(1)  # Adjust polling interval as needed
                    
                except Exception as e:
                    logger.error(f"Error in main loop iteration: {e}")
                    await asyncio.sleep(5)  # Back off on error
                
        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
        finally:
            monitor_task.cancel()
            await self.close()

async def main():
    """Entry point for the arbitrage bot"""
    # Apply nest_asyncio to allow nested event loops
    nest_asyncio.apply()
    
    # Initialize Solana client
    solana_client = AsyncClient("https://api.mainnet-beta.solana.com")
    
    try:
        # Create and run the arbitrage bot
        bot = ArbitrageBot(solana_client)
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Shutting down bot...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        await solana_client.close()

if __name__ == "__main__":
    asyncio.run(main())
