import os

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.live import StockDataStream
from alpaca.data.models import Trade
from dotenv import load_dotenv

load_dotenv()

client = TradingClient(
    os.environ["ALPACA_API_KEY"],
    os.environ["ALPACA_API_SECRET"],
    paper=True,
)

# Check account
account = client.get_account()
print("Account status:", account.status)
print("Buying power:  ", account.buying_power)

order = client.submit_order(
    MarketOrderRequest(
        symbol="AAPL",
        qty=1,
        side=OrderSide.BUY,
        time_in_force=TimeInForce.GTC,
    )
)
print("Order ID:  ", order.id)
print("Status:    ", order.status)
print("Submitted: ", order.submitted_at)

# Stream live trades for AAPL
stream = StockDataStream(
    os.environ["ALPACA_API_KEY"],
    os.environ["ALPACA_API_SECRET"],
)

async def on_trade(trade: Trade):
    print({
        "symbol":    trade.symbol,
        "price":     trade.price,
        "size":      trade.size,
        "timestamp": trade.timestamp,
        "id":        trade.id,
    })

stream.subscribe_trades(on_trade, "AAPL", "MSFT")
stream.run()
