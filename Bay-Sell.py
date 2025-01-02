import json
import requests
import time
import hmac
import threading
import hashlib
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Binance API Keys
BINANCE_API_KEY = ''
BINANCE_API_SECRET = ''

# 初始化客户端
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

def get_timestamp():
    return int(time.time() * 1000)

def sign_request(params):
    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
    return hmac.new(BINANCE_API_SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

def load_config(filename="config.json"):
    with open(filename, "r") as f:
        return json.load(f)

def get_market_price(symbol):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)
    if response.status_code == 200:
        return float(response.json()["price"])
    else:
        print(f"{symbol}: 获取市场价格失败: {response.status_code}, {response.text}")
        return None

def get_flexible_positions():
    url = "https://api.binance.com/sapi/v1/simple-earn/flexible/position"
    params = {"timestamp": get_timestamp()}
    params["signature"] = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        positions = response.json()
        return positions
    else:
        print(f"获取持仓失败: {response.status_code}, {response.text}")
        return None

def redeem_savings(product_id, amount):
    url = "https://api.binance.com/sapi/v1/simple-earn/flexible/redeem"
    params = {
        "productId": product_id,
        "amount": amount,  # 使用明确的数量
        "type": "FAST",
        "timestamp": get_timestamp(),
    }
    params["signature"] = sign_request(params)
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}

    response = requests.post(url, headers=headers, params=params)
    if response.status_code == 200:
        print(f"赎回成功: {response.json()}")
    else:
        print(f"赎回失败: {response.status_code}, {response.text}")

def get_symbol_info(symbol):
    try:
        exchange_info = client.get_exchange_info()
        for symbol_info in exchange_info['symbols']:
            if symbol_info['symbol'] == symbol:
                return symbol_info
        return None
    except BinanceAPIException as e:
        print(f"获取交易对信息失败: {e}")
        return None

def get_symbol_filters(symbol):
    try:
        symbol_info = get_symbol_info(symbol)
        if not symbol_info:
            print(f"{symbol}: 无法获取交易对信息")
            return None

        filters = {f["filterType"]: f for f in symbol_info["filters"]}

        # 检查是否存在 NOTIONAL 过滤器
        min_notional = float(filters["NOTIONAL"]["minNotional"]) if "NOTIONAL" in filters else None

        return {
            "min_qty": float(filters["LOT_SIZE"]["minQty"]),
            "step_size": float(filters["LOT_SIZE"]["stepSize"]),
            "tick_size": float(filters["PRICE_FILTER"]["tickSize"]),
            "min_notional": min_notional,  # 使用 NOTIONAL 的 minNotional
        }
    except BinanceAPIException as e:
        print(f"{symbol}: 获取交易对过滤器失败: {e}")
        return None
    except Exception as e:
        print(f"{symbol}: 未知错误 {e}")
        return None

def place_order_thread(symbol, quantity, price, index, side):
    try:
        order = client.create_order(
            symbol=symbol,
            side=side,
            type="LIMIT",
            timeInForce="GTC",
            quantity=round(quantity, 8),
            price=str(price),
        )
        print(f"{symbol}: 第 {index} 个限价{'买单' if side == 'BUY' else '卖单'}成功: {order}")
    except BinanceAPIException as e:
        print(f"{symbol}: 第 {index} 个限价{'买单' if side == 'BUY' else '卖单'}失败: {e}")

def sell_asset_in_parts(asset_balance, sell_price, split_count, symbol):
    try:
        filters = get_symbol_filters(symbol)
        if not filters:
            print(f"{symbol}: 无法获取过滤器信息，跳过卖出操作")
            return

        min_qty = filters["min_qty"]
        step_size = filters["step_size"]
        tick_size = filters["tick_size"]
        min_notional = filters["min_notional"]

        # 检查是否满足最小名义值
        total_notional = asset_balance * sell_price
        if total_notional < min_notional:
            print(f"{symbol}: 资产总值 {total_notional} 小于最小名义值 {min_notional}，跳过卖出操作")
            return

        part_quantity = (asset_balance / split_count // step_size) * step_size
        if part_quantity < min_qty:
            print(f"{symbol}: 单笔出售数量低于最小交易量 {min_qty}，跳过")
            return

        prices = [
            round(sell_price + (i - split_count // 2) * tick_size, 8)
            for i in range(split_count)
        ]

        threads = []
        for i, adjusted_price in enumerate(prices):
            thread = threading.Thread(
                target=place_order_thread,
                args=(symbol, part_quantity, adjusted_price, i + 1, "SELL"),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    except BinanceAPIException as e:
        print(f"{symbol}: 分批出售失败: {e}")
    except Exception as e:
        print(f"{symbol}: 未知错误 {e}")

def buy_asset_in_parts(usdt_balance, buy_price, split_count, symbol):
    try:
        # 获取交易对过滤器
        filters = get_symbol_filters(symbol)
        if not filters:
            return

        min_qty = filters["min_qty"]
        step_size = filters["step_size"]
        tick_size = filters["tick_size"]
        min_notional = filters["min_notional"]

        part_usdt = usdt_balance / split_count
        prices = [
            round(buy_price - (split_count // 2 - i) * tick_size, 8)
            for i in range(split_count)
        ]

        threads = []
        for i, adjusted_price in enumerate(prices):
            # 根据调整价格计算买入数量
            quantity = round(part_usdt / adjusted_price // step_size * step_size, 8)

            # 检查是否满足最小名义值和最小交易量
            if adjusted_price * quantity < min_notional:
                print(f"{symbol}: 订单金额 {adjusted_price * quantity} 小于最小名义值 {min_notional}，跳过")
                continue

            if quantity < min_qty:
                print(f"{symbol}: 购买数量 {quantity} 小于最小交易量 {min_qty}，跳过")
                continue

            print(f"{symbol}: 挂单价格 {adjusted_price}, 数量 {quantity}")
            thread = threading.Thread(
                target=place_order_thread,
                args=(symbol, quantity, adjusted_price, i + 1, "BUY"),
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    except BinanceAPIException as e:
        print(f"{symbol}: 分批买入失败: {e}")
    except Exception as e:
        print(f"{symbol}: 未知错误 {e}")

def process_asset(asset_data):
    asset = asset_data["asset"]
    target_price = asset_data["target_price"]
    sell_price = asset_data["sell_price"]
    buy_price = asset_data.get("buy_price", None)
    split_count = asset_data["split_count"]
    symbol = f"{asset}USDT"

    market_price = get_market_price(symbol)
    if market_price is None:
        return

    print(f"{symbol}: 当前市场价格为 {market_price}")

    if buy_price and market_price <= buy_price:
        usdt_balance = get_balance("USDT")
        if usdt_balance > 10:
            print(f"{symbol}: 市场价格 {market_price} 低于买入价格 {buy_price}，准备买入")
            print(f"现货 USDT 余额为 {usdt_balance}，开始挂单购买")
            buy_asset_in_parts(usdt_balance, buy_price, split_count, symbol)
            return
        positions = get_flexible_positions()
        if positions:
            for position in positions.get("rows", []):
                if position["asset"] == "USDT" and position["canRedeem"]:
                    print(f"{symbol}: 需要从理财账户赎回 USDT，数量为 {position['totalAmount']}")
                    redeem_savings(position["productId"], position["totalAmount"])
                    usdt_balance = get_balance("USDT")
                    if usdt_balance > 0:
                        print(f"{symbol}: 赎回成功，现货 USDT 余额为 {usdt_balance}，开始挂单购买")
                        buy_asset_in_parts(usdt_balance, buy_price, split_count, symbol)

    elif market_price >= target_price:
        print(f"{symbol}: 市场价格 {market_price} 达到目标价格 {target_price}，准备卖出")
        asset_balance = get_balance(asset)

        # # 检查资产是否满足 NOTIONAL
        # filters = get_symbol_filters(symbol)
        # if filters and asset_balance * market_price < filters["min_notional"]:
        #     print(
        #         f"{symbol}: 资产总值 {asset_balance * market_price} 小于最小名义值 {filters['min_notional']}，跳过卖出操作")
        #     return

        if asset_balance > 0:
            print(f"现货 {asset} 余额为 {asset_balance}，开始挂单出售")
            sell_asset_in_parts(asset_balance, sell_price, split_count, symbol)
            return
        positions = get_flexible_positions()
        if positions:
            print(f"{symbol}: 获取的理财持仓信息: {positions}")
            for position in positions.get("rows", []):
                if position["asset"] == asset and position["canRedeem"]:
                    print(f"{symbol}: 需要从理财账户赎回 {asset}，数量为 {position['totalAmount']}")
                    redeem_savings(position["productId"], position["totalAmount"])
                    asset_balance = get_balance(asset)
                    if asset_balance > 0:
                        print(f"{symbol}: 赎回成功，现货 {asset} 余额为 {asset_balance}，开始挂单出售")
                        sell_asset_in_parts(asset_balance, sell_price, split_count, symbol)

def get_balance(asset):
    try:
        account_info = client.get_account()
        for balance in account_info['balances']:
            if balance['asset'] == asset:
                return float(balance['free'])
        return 0.0
    except BinanceAPIException as e:
        print(f"获取余额失败: {e}")
        return 0.0

if __name__ == "__main__":
    config = load_config()
    while True:
        for asset in config:
            try:
                process_asset(asset)
            except Exception as e:
                print(f"处理资产 {asset['asset']} 时发生错误: {e}")
        time.sleep(1)