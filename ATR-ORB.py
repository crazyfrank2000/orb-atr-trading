#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import numpy as np
from ib_insync import *
from datetime import datetime, timedelta
import pytz
import time
import os
import configparser

# 全局变量用于存储信号K线数据和交易记录
signal_candle_data = None
trades_record = []
global_min_tick = 0.01 # Default minTick, will be updated after contract details are fetched

# 确保logs文件夹存在
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
    print(f"Created logs directory: {logs_dir}")

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "xau_atr_trading.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ORB_ATR_XAU')

# 设置ib_insync日志级别为WARNING，减少冗余输出
util.logToConsole(logging.WARNING)

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

trade_symbol = config.get('Trading', 'symbol', fallback='XAUUSD')
trade_sec_type = config.get('Trading', 'secType', fallback='CMDTY')
trade_exchange = config.get('Trading', 'exchange', fallback='SMART')
trade_currency = config.get('Trading', 'currency', fallback='USD')
trade_last_trade_date = config.get('Trading', 'lastTradeDateOrContractMonth', fallback=None)
if trade_last_trade_date == '': # Handle empty string from config
    trade_last_trade_date = None
trade_what_to_show = config.get('Trading', 'whatToShow', fallback='MIDPOINT')

# Add new config values for exit strategy
raw_exit_strategy_from_config = config.get('Trading', 'exitStrategy', fallback='EOD')
# Process to take the first part before a semicolon, strip whitespace, and uppercase
processed_strategy_token = raw_exit_strategy_from_config.split(';')[0].strip().upper()

if processed_strategy_token in ['EOD', 'MAX_DURATION']:
    trade_exit_strategy = processed_strategy_token
    logger.info(f"Using exit strategy: {trade_exit_strategy} (derived from config value: '{raw_exit_strategy_from_config}')")
else:
    logger.warning(f"Invalid 'exitStrategy' in config.ini: '{raw_exit_strategy_from_config}'. Primary token '{processed_strategy_token}' is not 'EOD' or 'MAX_DURATION'. Defaulting to 'EOD'.")
    trade_exit_strategy = 'EOD' # Fallback to EOD

trade_max_hold_duration_minutes = config.getint('Trading', 'maxHoldDurationMinutes', fallback=60)

# 连接到IBKR
ib = IB()
try:
    ib.connect('127.0.0.1', 7497, clientId=1)
    logger.info("Connected to IBKR")
except Exception as e:
    logger.error(f"Failed to connect to IBKR: {e}")
    exit(1)

# 获取用户选择的合约类型
def get_contract_from_config(config_data):
    global global_min_tick # Declare global at the beginning of the function
    """获取配置文件中指定的合约"""
    try:
        logger.info(f"Attempting to load contract for {trade_symbol} from config...")
        
        contract_args = {
            "symbol": trade_symbol,
            "secType": trade_sec_type,
            "exchange": trade_exchange,
            "currency": trade_currency
        }

        base_contract_obj = None # To hold the initial contract object before qualification/detailing
        if trade_sec_type == "FUT":
            fut_args = {k: v for k, v in contract_args.items() if k != 'secType'}
            if trade_last_trade_date:
                fut_args["lastTradeDateOrContractMonth"] = trade_last_trade_date
                base_contract_obj = Future(**fut_args)
            else:
                logger.error("Future contract type specified but lastTradeDateOrContractMonth is missing in config.")
                return None
        elif trade_sec_type == "STK":
            stk_args = {k: v for k, v in contract_args.items() if k != 'secType'}
            base_contract_obj = Stock(**stk_args)
        elif trade_sec_type == "CMDTY":
            base_contract_obj = Contract(**contract_args) # For spot XAUUSD etc.
        elif trade_sec_type == "CASH":
             pair = f"{trade_symbol}{trade_currency}"
             if trade_symbol == "XAU" and trade_currency == "USD":
                 pair = "XAUUSD"
                 base_contract_obj = Forex(pair)
                 base_contract_obj.exchange = trade_exchange
             elif len(trade_symbol) == 3 and len(trade_currency) == 3 :
                 base_contract_obj = Forex(pair)
                 base_contract_obj.exchange = trade_exchange
             else:
                logger.error(f"Invalid symbol/currency for CASH secType: {trade_symbol}/{trade_currency}. Must be standard Forex pair e.g. EUR/USD.")
                return None
        else:
            logger.error(f"Unsupported secType in config: {trade_sec_type}")
            return None

        logger.info(f"Initial contract object: {base_contract_obj}")
        
        qualified_contract = None
        current_contract_min_tick = 0.01 # Default min_tick for this scope

        if isinstance(base_contract_obj, (Future, Stock)):
            contracts = ib.qualifyContracts(base_contract_obj)
            if contracts:
                qualified_contract = contracts[0]
                logger.info(f"Successfully qualified {trade_symbol}: {qualified_contract}")
            else:
                logger.error(f"Failed to qualify {trade_symbol} using qualifyContracts.")
                return None
        else:
            details = ib.reqContractDetails(base_contract_obj)
            if details:
                qualified_contract = details[0].contract
                # For CMDTY/CASH etc., minTick is directly available from details
                if details[0].minTick > 0:
                    current_contract_min_tick = details[0].minTick
                logger.info(f"Successfully found details for {trade_symbol}: {qualified_contract}")
            else:
                logger.error(f"Failed to find details for {trade_symbol} using reqContractDetails.")
                return None
        
        # If qualified_contract is set (either from Future/Stock or other types), get its full details for minTick if not already available
        if qualified_contract and current_contract_min_tick == 0.01: # Check if minTick wasn't set from initial details (e.g. for Future/Stock)
            try:
                contract_details_list = ib.reqContractDetails(qualified_contract)
                if contract_details_list and contract_details_list[0].minTick > 0:
                    current_contract_min_tick = contract_details_list[0].minTick
                    logger.info(f"MinTick for {qualified_contract.symbol} (after qualification/detailing) set to: {current_contract_min_tick}")
                else:
                    logger.warning(f"Could not retrieve valid minTick for {qualified_contract.symbol} from its details. Using default {current_contract_min_tick}")    
            except Exception as e_min_tick_detail:
                logger.error(f"Error fetching details for minTick for {qualified_contract.symbol}: {e_min_tick_detail}. Using default {current_contract_min_tick}")

        # At this point, qualified_contract should be the contract to test, and current_contract_min_tick has its best-effort minTick.
        if not qualified_contract:
            logger.error(f"Contract for {trade_symbol} could not be resolved.")
            return None
            
        try:
            logger.info(f"Testing historical data retrieval for {trade_symbol} with whatToShow='{trade_what_to_show}'...")
            bars = ib.reqHistoricalData(
                qualified_contract, # Use the resolved contract
                endDateTime='',
                durationStr='1 D',
                barSizeSetting='1 hour',
                whatToShow=trade_what_to_show,
                useRTH=False
            )
            if bars:
                logger.info(f"Successfully retrieved {len(bars)} bars for {trade_symbol}")
                global_min_tick = current_contract_min_tick
                logger.info(f"Global minTick updated to: {global_min_tick} for contract {qualified_contract.symbol}")
                return qualified_contract
            else:
                logger.warning(f"Could not get historical data for {trade_symbol}. This might be an issue with 'whatToShow' ({trade_what_to_show}) or market data permissions.")
                if trade_what_to_show != 'MIDPOINT' and trade_sec_type != 'FUT':
                    logger.info("Retrying with whatToShow='MIDPOINT'...")
                    bars_midpoint = ib.reqHistoricalData(
                        qualified_contract, # Use the resolved contract
                        endDateTime='',
                        durationStr='1 D',
                        barSizeSetting='1 hour',
                        whatToShow='MIDPOINT',
                        useRTH=False
                    )
                    if bars_midpoint:
                        logger.info(f"Successfully retrieved {len(bars_midpoint)} bars with MIDPOINT. Consider updating config if this works consistently.")
                        global_min_tick = current_contract_min_tick
                        logger.info(f"Global minTick updated to: {global_min_tick} for contract {qualified_contract.symbol} (using MIDPOINT)")
                        return qualified_contract
                    else:
                        logger.error(f"Still could not get historical data for {qualified_contract.symbol} even with MIDPOINT.")
                        return None
                return None
        except Exception as e:
            logger.error(f"Error during initial historical data check for {qualified_contract.symbol}: {e}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to create or qualify contract for {trade_symbol}: {e}")
        return None

# 获取合约
contract = get_contract_from_config(config)
if contract is None:
    logger.error(f"Unable to find/qualify a tradable contract for {trade_symbol} as configured. Exiting.")
    ib.disconnect()
    exit(1)

# 打印最终使用的合约信息
logger.info("=" * 50)
logger.info("FINAL CONTRACT SELECTION:")
logger.info(f"Symbol: {contract.symbol}")
logger.info(f"Type: {contract.secType}")
logger.info(f"Exchange: {contract.exchange}")
if hasattr(contract, 'localSymbol') and contract.localSymbol:
    logger.info(f"Local Symbol: {contract.localSymbol}")
logger.info("=" * 50)

# 确认用户想要继续
logger.info("If this is not the contract you want to trade, please stop the script now.")
logger.info("Starting trading in 10 seconds...")
for i in range(10, 0, -1):
    logger.info(f"{i}...")
    time.sleep(1)

ACCOUNT_SIZE = 25000
LEVERAGE = 4
RISK_PCT = 0.01

# 获取历史K线（含夜盘）
def get_bars(duration, bar_size):
    try:
        # 转换时间格式为IBKR所需的格式
        # 假设输入格式为'30 D'这样的字符串，需要确保格式符合要求
        duration_parts = duration.split()
        if len(duration_parts) == 2:
            value = duration_parts[0]
            unit = duration_parts[1][0].upper()  # 取单位的第一个字母并大写
            duration_formatted = f"{value} {unit}"
        else:
            # 如果格式已经正确或无法解析，保持原样
            duration_formatted = duration
            
        logger.info(f"请求历史数据: 周期={duration_formatted}, 时间粒度={bar_size}")
            
        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr=duration_formatted,
            barSizeSetting=bar_size,
            whatToShow=trade_what_to_show,
            useRTH=False
        )
        if not bars:
            logger.warning(f"No historical data returned for {duration_formatted} {bar_size}")
            return pd.DataFrame()

        df = util.df(bars)
        return df
    except Exception as e:
        logger.error(f"Error getting historical data: {e}")
        return pd.DataFrame()

# 获取最新完整的5分钟K线
def get_latest_complete_5min_bar():
    try:
        # 获取当前时间
        now = datetime.now(pytz.timezone('US/Eastern'))
        
        # 计算最近的完整5分钟K线的结束时间
        # 例如：当前10:07，最近的完整K线是10:05，结束于10:05
        minutes = now.minute
        latest_bar_minute = (minutes // 5) * 5
        
        # 创建最近完整K线的结束时间
        bar_end_time = now.replace(minute=latest_bar_minute, second=0, microsecond=0)
        
        # 如果当前分钟恰好是K线的结束分钟，且秒数很小，那么最近的完整K线应该是上一个
        if minutes % 5 == 0 and now.second < 3:
            bar_end_time = bar_end_time - timedelta(minutes=5)
        
        # 格式化为IB API需要的格式
        end_time_str = bar_end_time.strftime('%Y%m%d %H:%M:%S US/Eastern') # Corrected timezone format
        
        logger.info(f"获取截至 {end_time_str} 的最新完整5分钟K线")
        
        # 请求历史数据，获取2根K线
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_time_str,
            durationStr='1800 S',  # 使用1800秒(30分钟)，确保包含至少几根5分钟K线
            barSizeSetting='5 mins',
            whatToShow=trade_what_to_show,
            useRTH=False
        )
        
        if not bars or len(bars) < 1:
            logger.warning("未能获取最新完整5分钟K线")
            return None
        
        # 返回最新的一根完整K线
        latest_bar = bars[-1]
        logger.info(f"获取到K线: 开盘:{latest_bar.open} 最高:{latest_bar.high} 最低:{latest_bar.low} 收盘:{latest_bar.close} 时间:{latest_bar.date}")
        
        return latest_bar
    
    except Exception as e:
        logger.error(f"获取最新K线时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# 计算ATR
def calculate_atr(df, period=14):
    df['H-L'] = df['high'] - df['low']
    df['H-PC'] = np.abs(df['high'] - df['close'].shift(1))
    df['L-PC'] = np.abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=period).mean()
    return df['ATR'].iloc[-1]

# 价格精度处理函数
def format_price(price):
    """根据合约的minTick格式化价格，确保符合交易所要求"""
    effective_tick_size = 0.01 # Fallback default
    if global_min_tick is not None and global_min_tick > 0:
        effective_tick_size = global_min_tick
    else:
        logger.warning(f"global_min_tick is invalid ({global_min_tick}). Defaulting to 0.01 for price formatting.")
    
    if effective_tick_size <= 0: # Final safety check
        effective_tick_size = 0.01
        
    return round(price / effective_tick_size) * effective_tick_size

# 下单函数
def place_trade(action, quantity, stop_price):
    """主要下单函数，执行交易并添加止损单"""
    try:
        # 先取消所有现有订单
        cancel_all_orders()
        
        # 获取合约详情并打印确认信息
        logger.info("=" * 50)
        logger.info(f"开始交易执行 | {contract.symbol} ({contract.secType}) | {contract.exchange}")
        if hasattr(contract, 'localSymbol') and contract.localSymbol:
            logger.info(f"合约详情: {contract.localSymbol}")
        
        # 使用全局存储的信号K线数据
        global signal_candle_data
        if not signal_candle_data:
            logger.error("无法获取信号K线数据，无法下单")
            return None, None
        
        # 使用信号K线的收盘价作为限价单价格
        limit_price = format_price(signal_candle_data['close'])
        logger.info(f"使用信号K线收盘价作为限价单价格: ${limit_price:.2f}")
        
        # 创建限价单并设置唯一引用ID
        order_ref = f"Entry_{datetime.now().strftime('%H%M%S')}"
        order = LimitOrder(action, quantity, limit_price)
        order.orderRef = order_ref
        order.transmit = True
        order.outsideRth = True
        logger.info(f"创建{action}限价单 | 数量: {quantity} | 价格: ${limit_price:.2f} | 引用ID: {order_ref} | OutsideRTH: {order.outsideRth}")
        
        # 下订单并获取订单ID
        trade = ib.placeOrder(contract, order)
        if hasattr(trade, 'order') and hasattr(trade.order, 'orderId'):
            order_id = trade.order.orderId
            logger.info(f"订单已提交 | ID: {order_id}")
        else:
            logger.warning("无法获取订单ID")
        
        # 等待订单执行
        filled = False
        fill_price = None
        start_time = time.time()
        price_adjustment_count = 0
        max_attempts = 40  # 最多等待40秒
        
        for attempt in range(max_attempts):
            ib.sleep(1)
            
            # 查询实时订单状态
            if hasattr(trade, 'orderStatus'):
                status = trade.orderStatus.status
                filled_qty = trade.orderStatus.filled if hasattr(trade.orderStatus, 'filled') else 0
                
                # 简化订单状态输出
                if attempt % 3 == 0 or status in ['Filled', 'Cancelled', 'ApiCancelled', 'Inactive']:
                    logger.info(f"订单状态: {status} | 成交: {filled_qty}/{quantity} | 尝试: {attempt+1}/{max_attempts}")
                
                if status == 'Filled':
                    filled = True
                    fill_price = float(trade.orderStatus.avgFillPrice)
                    logger.info(f"订单已成交 | 均价: ${fill_price:.2f}")
                    break
                elif status in ['Cancelled', 'ApiCancelled', 'Inactive']:
                    logger.warning(f"订单已取消或失效: {status}")
                    return None, None
                    
                # 如果订单停留在Submitted状态且时间超过了20秒，调整价格重新下单
                elapsed_time = time.time() - start_time
                if elapsed_time > 20 and price_adjustment_count < 2 and status == 'Submitted':
                    price_adjustment_count += 1
                    
                    # 根据交易方向调整价格
                    if action == 'BUY':
                        # 买入订单，调高价格0.1%
                        new_limit_price = format_price(limit_price * 1.001)
                    else:
                        # 卖出订单，调低价格0.1%
                        new_limit_price = format_price(limit_price * 0.999)
                    
                    logger.info(f"订单20秒未成交 | 调整价格 ${limit_price:.2f} -> ${new_limit_price:.2f}")
                    
                    # 取消当前订单
                    ib.cancelOrder(trade.order)
                    ib.sleep(1)  # 等待取消处理
                    
                    # 创建新订单
                    limit_price = new_limit_price
                    new_order_ref = f"EntryAdj{price_adjustment_count}_{datetime.now().strftime('%H%M%S')}"
                    new_order = LimitOrder(action, quantity, limit_price)
                    new_order.orderRef = new_order_ref
                    new_order.transmit = True
                    new_order.outsideRth = True
                    
                    logger.info(f"创建新{action}限价单 | 数量: {quantity} | 调整后价格: ${limit_price:.2f} | 引用ID: {new_order_ref} | OutsideRTH: {new_order.outsideRth}")
                    trade = ib.placeOrder(contract, new_order)
                    
                    # 重置计时器
                    start_time = time.time()
        
        # 检查订单是否成交
        if filled and fill_price:
            entry_time = datetime.now(pytz.timezone('US/Eastern'))
            logger.info(f"主订单成交时间: {entry_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 确认当前持仓
            positions = ib.positions()
            position_found = False
            for pos in positions:
                if pos.contract.symbol == contract.symbol:
                    position_found = True
                    logger.info(f"当前持仓确认: {pos.position} {pos.contract.symbol} @ ${pos.avgCost:.2f}")
            
            if not position_found:
                logger.warning(f"交易成交后无法在持仓中找到 {contract.symbol}")
            
            logger.info("=" * 50)
            return fill_price, entry_time
        else:
            logger.warning(f"限价单未在{max_attempts}秒内成交，取消订单")
            if hasattr(trade, 'order'):
                ib.cancelOrder(trade.order)
            return None, None
    except Exception as e:
        logger.error(f"交易执行错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None

# 取消所有现有订单的辅助函数
def cancel_all_orders():
    """取消所有现有订单，确保正确处理ID"""
    try:
        # 先通过ib.trades()获取活动交易中的订单并尝试取消
        active_trades = ib.trades()
        if active_trades:
            logger.info(f"检查 {len(active_trades)} 个活动交易中的订单...")
            for trade_obj in active_trades:
                if hasattr(trade_obj, 'order') and hasattr(trade_obj.order, 'orderId') and trade_obj.order.orderId > 0:
                    # Check if order is in a cancellable state
                    if trade_obj.orderStatus.status not in OrderStatus.DoneStates:
                        logger.info(f"从活动交易中取消订单 ID: {trade_obj.order.orderId}, 状态: {trade_obj.orderStatus.status}")
                        try:
                            ib.cancelOrder(trade_obj.order)
                        except Exception as e_cancel_trade_order:
                            logger.warning(f"取消活动交易订单 {trade_obj.order.orderId} 时出错: {e_cancel_trade_order}")
                    else:
                        logger.info(f"活动交易订单 ID: {trade_obj.order.orderId} 已处于完成状态 ({trade_obj.orderStatus.status}), 无需取消.")
        else:
            logger.info("没有从 ib.trades() 中找到活动交易订单.")

        # 然后通过 ib.openOrders() 获取并取消所有全局开放订单
        logger.info("请求更新并获取所有全局开放订单...")
        ib.reqAllOpenOrders()  # 请求更新列表
        ib.sleep(0.5)          # 短暂等待事件处理以更新内部列表
        
        current_open_orders = ib.openOrders() # 获取当前所有开放订单的列表
        
        if current_open_orders:
            logger.info(f"发现 {len(current_open_orders)} 个全局开放订单 (via ib.openOrders())...")
            for order_to_cancel in current_open_orders:
                if hasattr(order_to_cancel, 'orderId') and order_to_cancel.orderId > 0:
                    # Ensure it's an order that can be cancelled
                    if order_to_cancel.orderStatus not in OrderStatus.DoneStates and order_to_cancel.permId != 0:
                        logger.info(f"取消全局开放订单 ID: {order_to_cancel.orderId}, 状态: {order_to_cancel.orderStatus}")
                        try:
                            ib.cancelOrder(order_to_cancel)
                        except Exception as e_cancel_open_order:
                            logger.warning(f"取消全局开放订单 {order_to_cancel.orderId} 时出错: {e_cancel_open_order}")
                    elif order_to_cancel.permId == 0:
                        logger.info(f"全局开放订单 ID: {order_to_cancel.orderId} permId is 0, may not be cancellable yet or is a TWS internal order.")
                    else:
                        logger.info(f"全局开放订单 ID: {order_to_cancel.orderId} 已处于完成状态 ({order_to_cancel.orderStatus}), 无需取消.")
        else:
            logger.info("没有从 ib.openOrders() 中找到全局开放订单.")
        
        # 给系统时间处理取消请求
        logger.info("等待2秒以便系统处理取消请求...")
        ib.sleep(2)
        
    except Exception as e:
        logger.error(f"取消订单时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())

# 下止损单
def place_stoploss_order(entry_action, quantity, stop_price, fill_price):
    """设置止损单"""
    sl_action = 'SELL' if entry_action == 'BUY' else 'BUY'
    
    # 首先取消所有之前可能的止损单
    cancel_all_orders()
    
    # 调整止损价格
    if sl_action == 'BUY':
        # 买入止损（针对卖出仓位）应该高于当前市价
        stop_price = max(stop_price, fill_price * 1.001)  # 至少比填充价高0.1%
    else:
        # 卖出止损（针对买入仓位）应该低于当前市价
        stop_price = min(stop_price, fill_price * 0.999)  # 至少比填充价低0.1%
    
    # 格式化价格，确保符合交易所要求
    formatted_stop_price = format_price(stop_price)
    logger.info(f"设置止损单 | {sl_action} {quantity} @ ${formatted_stop_price:.2f}")
    
    try:
        # 使用StopOrder函数创建止损单
        sl_order = StopOrder(sl_action, quantity, formatted_stop_price, tif='GTC')
        sl_order.outsideRth = True  # 允许在常规交易时间之外触发
        sl_order.transmit = True    # 确保订单被传输
        sl_order.orderRef = f"Stop_{datetime.now().strftime('%H%M%S')}"  # 添加引用便于识别
        
        # 下止损单
        sl_trade = ib.placeOrder(contract, sl_order)
        
        # 获取订单ID
        if hasattr(sl_trade, 'order'):
            order_id = sl_trade.order.orderId
            logger.info(f"止损单已提交 | ID: {order_id}")
        else:
            logger.warning("无法获取止损单ID")
            return False
        
        # 等待订单状态更新
        for i in range(10):
            ib.sleep(1)
            
            # 检查订单状态
            if hasattr(sl_trade, 'orderStatus'):
                status = sl_trade.orderStatus.status
                
                if i % 3 == 0 or status in ['Submitted', 'PreSubmitted', 'Filled', 'PendingSubmit']:
                    logger.info(f"止损单状态: {status} | 尝试: {i+1}/10")
                
                if status in ['Submitted', 'PreSubmitted', 'Filled']:
                    logger.info(f"止损单已被接受: {status}")
                    return True
                elif status == 'PendingSubmit' and i >= 8:
                    # 如果长时间处于PendingSubmit状态，尝试微调价格并重新提交
                    logger.warning("止损单卡在PendingSubmit状态，尝试调整价格重试")
                    ib.cancelOrder(sl_trade.order)
                    ib.sleep(2)
                    
                    # 微调价格并重新提交
                    adjustment = 0.01  # 一分钱的调整
                    new_price = format_price(formatted_stop_price + (adjustment if sl_action == 'BUY' else -adjustment))
                    new_order = StopOrder(sl_action, quantity, new_price, tif='GTC')
                    new_order.outsideRth = True
                    new_order.transmit = True
                    new_order.orderRef = f"StopRetry_{datetime.now().strftime('%H%M%S')}"
                    
                    logger.info(f"重试止损单 | 新价格: ${new_price:.2f}")
                    new_trade = ib.placeOrder(contract, new_order)
                    
                    # 等待新订单状态
                    ib.sleep(3)
                    if hasattr(new_trade, 'orderStatus'):
                        new_status = new_trade.orderStatus.status
                        logger.info(f"重试止损单状态: {new_status}")
                        if new_status in ['Submitted', 'PreSubmitted']:
                            return True
        
        # 验证开放订单列表
        open_orders = ib.reqAllOpenOrders()
        stop_orders_found = 0
        for o in open_orders:
            if o.order.orderType in ['STP', 'STOP', 'LMT']:
                logger.info(f"活跃止损单: {o.order.action} {o.order.totalQuantity} @ ${o.order.auxPrice if hasattr(o.order, 'auxPrice') else 0:.2f}")
                stop_orders_found += 1
        
        if stop_orders_found == 0:
            logger.warning("警告: 未在活跃订单列表中找到止损单")
            return False
            
        # 最后检查开放订单列表
        final_orders = ib.reqAllOpenOrders()
        for order in final_orders:
            if order.orderType in ['STP', 'STOP'] and order.action == sl_action:
                logger.info(f"确认: 找到{sl_action}止损单")
                return True
        
        logger.warning("无法确认止损单状态")
        return False
        
    except Exception as e:
        logger.error(f"设置止损单时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# 计算到下一个5分钟周期的等待时间
def calculate_wait_time_to_next_5min():
    """计算到下一个5分钟周期的等待时间（秒）"""
    now = datetime.now(pytz.timezone('US/Eastern'))
    
    # 计算当前分钟在5分钟周期中的位置
    current_minute = now.minute
    seconds_passed = now.second
    
    # 计算距离下一个5分钟周期的时间（秒）
    minutes_to_next = 5 - (current_minute % 5)
    if minutes_to_next == 5 and seconds_passed == 0:
        return 0  # 刚好在整点5分钟
        
    seconds_to_next = minutes_to_next * 60 - seconds_passed
    
    # 确保等待时间至少为15秒，给系统处理时间
    return max(15, seconds_to_next)

def wait_for_next_5min_candle():
    """
    等待到下一个5分钟K线开始
    
    返回:
        下一个5分钟K线的开始和结束时间
    """
    now = datetime.now(pytz.timezone('US/Eastern'))
    current_minute = now.minute
    current_second = now.second
    
    # 计算当前5分钟区间
    current_5min = (current_minute // 5) * 5
    
    # 计算下一个5分钟区间
    next_5min = (current_5min + 5) % 60
    
    # 如果下一个5分钟区间是0，则需要加一小时
    next_hour = now.hour
    if next_5min < current_5min:
        next_hour = (next_hour + 1) % 24
    
    # 创建下一个5分钟区间的开始时间
    next_candle_start = now.replace(hour=next_hour, minute=next_5min, second=0, microsecond=0)
    
    # 确保时间是未来的
    if next_candle_start <= now:
        next_candle_start = next_candle_start + timedelta(minutes=5)
    
    next_candle_end = next_candle_start + timedelta(minutes=5)
    
    # 计算等待时间
    wait_seconds = (next_candle_start - now).total_seconds()
    
    logger.info(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    logger.info(f"下一个5分钟K线开始于: {next_candle_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"等待 {wait_seconds:.1f} 秒...")
    
    # 等待直到下一个K线开始
    time.sleep(wait_seconds)
    
    logger.info(f"K线开始: {datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    
    return next_candle_start, next_candle_end

def wait_for_candle_complete(next_candle_end):
    """
    等待当前K线形成完毕
    
    参数:
        next_candle_end: K线结束时间
    """
    now = datetime.now(pytz.timezone('US/Eastern'))
    wait_seconds = (next_candle_end - now).total_seconds()
    
    if wait_seconds > 0:
        logger.info(f"等待K线完成形成，还需 {wait_seconds:.1f} 秒...")
        time.sleep(wait_seconds)
    
    # 额外等待1秒确保数据记录完毕
    time.sleep(1)
    logger.info(f"K线完成: {datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")

def get_historical_data(end_time, bar_size='5 mins', duration='1800 S'):
    """
    获取指定时间的历史K线数据
    
    参数:
        end_time: 结束时间字符串，格式为'%Y%m%d %H:%M:%S'
        bar_size: K线大小
        duration: 持续时间
    
    返回:
        DataFrame: K线数据
    """
    try:
        logger.info(f"获取历史数据: 结束时间={end_time}, K线大小={bar_size}, 持续时间={duration}")
        
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_time,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=trade_what_to_show,
            useRTH=False
        )
        
        if not bars or len(bars) == 0:
            logger.warning(f"未能获取历史数据")
            return pd.DataFrame()
        
        df = util.df(bars)
        logger.info(f"成功获取到 {len(df)} 根K线")
        return df
        
    except Exception as e:
        logger.error(f"获取历史数据时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

# 打印交易表格
def print_trade_table(action, entry_price, exit_price, quantity, profit_loss, profit_percent, duration, exit_reason):
    """打印交易结果表格"""
    direction = "LONG" if action == "BUY" else "SHORT"
    result = "盈利" if profit_loss > 0 else "亏损" if profit_loss < 0 else "持平"
    
    # 简化为两行输出
    logger.info("\n" + "="*80)
    logger.info(f"交易过程 | 方向: {direction} | 数量: {quantity} | 入场: ${entry_price:.2f} | 出场: ${exit_price:.2f} | 持仓时间: {duration}")
    logger.info(f"交易结果 | P/L: ${profit_loss:.2f} ({profit_percent:.2f}%) | 状态: {result} | 出场原因: {exit_reason}")
    logger.info("="*80)

# 打印本次交易详细总结
def print_trade_summary():
    """打印本次运行的交易详细总结"""
    if not trades_record:
        logger.info("没有交易记录")
        return
    
    logger.info("\n交易汇总")
    logger.info("-"*50)
    
    total_pnl = 0.0
    winning_trades = 0
    total_trades = 0
    
    for trade in trades_record:
        # 提取交易信息
        direction = trade.get('Direction', '')
        entry_price = trade.get('EntryPrice', 0)
        exit_price = trade.get('ExitPrice', 0)
        quantity = trade.get('Quantity', 0)
        pnl = trade.get('PnL', 0)
        pnl_percent = trade.get('PnLPercent', 0)
        exit_reason = trade.get('ExitReason', '')
        
        # 计算统计数据
        if exit_price > 0:  # 如果交易已平仓
            total_trades += 1
            total_pnl += pnl
            if pnl > 0:
                winning_trades += 1
            
            # 简化输出为单行
            logger.info(f"{direction} {quantity} | ${entry_price:.2f} → ${exit_price:.2f} | P/L: ${pnl:.2f} ({pnl_percent:.2f}%) | {exit_reason}")
    
    # 打印汇总信息
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
    logger.info("-"*50)
    logger.info(f"总计: {total_trades}笔交易 | 胜率: {win_rate:.1f}% | 总盈亏: ${total_pnl:.2f}")
    logger.info("-"*50)

# 日报记录
trades_record = []

def print_daily_report():
    if not trades_record:
        logger.info("No trades executed today")
        return

    # 准备当前交易数据
    current_trades_df = pd.DataFrame(trades_record)
    
    # Round PnL and PnLPercent columns to 2 decimal places if they exist
    if 'PnL' in current_trades_df.columns:
        current_trades_df['PnL'] = current_trades_df['PnL'].round(2)
    if 'PnLPercent' in current_trades_df.columns:
        current_trades_df['PnLPercent'] = current_trades_df['PnLPercent'].round(2)
    
    total_current_trades = len(current_trades_df)
    
    # 确保reports文件夹存在
    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
        logger.info(f"Created reports directory: {reports_dir}")
    
    # 使用固定的文件名，不包含日期
    symbol_name = contract.symbol if hasattr(contract, 'symbol') else "UNKNOWN_SYMBOL"
    csv_filename = os.path.join(reports_dir, f'trades_{symbol_name}_history.csv')
    
    # 检查文件是否已存在，如果存在则加载现有数据
    all_trades_df = current_trades_df.copy()  # 默认情况下使用当前交易
    if os.path.exists(csv_filename):
        try:
            existing_trades_df = pd.read_csv(csv_filename)
            logger.info(f"找到现有交易记录，包含 {len(existing_trades_df)} 笔交易")
            
            # Ensure PnL and PnLPercent in existing trades are rounded to 2 decimal places
            if 'PnL' in existing_trades_df.columns:
                existing_trades_df['PnL'] = existing_trades_df['PnL'].round(2)
            if 'PnLPercent' in existing_trades_df.columns:
                existing_trades_df['PnLPercent'] = existing_trades_df['PnLPercent'].round(2)
                
            # 检查是否有重复项
            if 'Time' in existing_trades_df.columns and 'Time' in current_trades_df.columns:
                # 基于交易时间和入场价格检查重复
                new_trades = []
                for _, new_trade in current_trades_df.iterrows():
                    # 检查此交易是否已存在于现有记录中
                    duplicate = False
                    for _, existing_trade in existing_trades_df.iterrows():
                        if 'Time' in new_trade and 'Time' in existing_trade and 'EntryPrice' in new_trade and 'EntryPrice' in existing_trade:
                            if new_trade['Time'] == existing_trade['Time'] and abs(float(new_trade['EntryPrice']) - float(existing_trade['EntryPrice'])) < 0.1:
                                duplicate = True
                                break
                    
                    if not duplicate:
                        new_trades.append(new_trade)
                
                if new_trades:
                    # 将新交易添加到现有交易
                    new_trades_df = pd.DataFrame(new_trades)
                    all_trades_df = pd.concat([existing_trades_df, new_trades_df], ignore_index=True)
                    logger.info(f"添加了 {len(new_trades)} 笔新交易")
                else:
                    all_trades_df = existing_trades_df
                    logger.info("没有找到新的交易记录需要添加")
            else:
                # 如果列名不匹配，直接合并
                all_trades_df = pd.concat([existing_trades_df, current_trades_df], ignore_index=True)
                logger.info("合并了现有和新交易记录")
        except Exception as e:
            logger.warning(f"读取现有交易记录失败: {e}，将只保存当前交易")
    
    # 计算汇总统计
    total_trades = len(all_trades_df)
    winning_trades = len(all_trades_df[all_trades_df['PnL'] > 0]) if 'PnL' in all_trades_df.columns else 0
    win_rate = winning_trades / total_trades if total_trades > 0 else 0

    # 获取今天的日期
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # 筛选今天的交易
    today_trades = all_trades_df[all_trades_df['Time'].str.startswith(today_date)] if 'Time' in all_trades_df.columns else pd.DataFrame()
    
    logger.info(f"=== 今日交易报告 ({today_date}) ===")
    logger.info(f"总交易次数: {total_trades} (今日: {len(today_trades)})")
    
    if 'PnL' in all_trades_df.columns and not all_trades_df['PnL'].isna().all():
        # 过滤掉PnL为0的记录(未平仓的交易)
        closed_trades = all_trades_df[all_trades_df['PnL'] != 0]
        if len(closed_trades) > 0:
            total_pnl = closed_trades['PnL'].sum()
            avg_pnl = closed_trades['PnL'].mean()
            
            winning_trades = len(closed_trades[closed_trades['PnL'] > 0])
            win_rate = winning_trades / len(closed_trades) if len(closed_trades) > 0 else 0
            
            # 计算今日已平仓交易的统计
            today_closed = today_trades[today_trades['PnL'] != 0]
            today_pnl = today_closed['PnL'].sum() if not today_closed.empty else 0
            today_win_rate = len(today_closed[today_closed['PnL'] > 0]) / len(today_closed) if len(today_closed) > 0 else 0
            
            logger.info(f"已平仓交易: {len(closed_trades)} (今日: {len(today_closed)})")
            logger.info(f"总胜率: {win_rate:.1%} (今日: {today_win_rate:.1%})")
            logger.info(f"总盈亏: ${total_pnl:.2f} (今日: ${today_pnl:.2f})")
            logger.info(f"平均盈亏: ${avg_pnl:.2f}")
        else:
            logger.info("没有已平仓的交易")
    else:
        logger.info("No P&L data available")

    # 保存所有交易记录
    all_trades_df.to_csv(csv_filename, index=False)
    logger.info(f"交易记录已保存至: {csv_filename}")
    logger.info(f"包含 {total_trades} 笔交易记录")

# 监控交易并处理平仓
def monitor_trade_and_exit(action, quantity, entry_price, stop_price, config_exit_strategy, config_max_hold_duration_minutes):
    """
    监控已执行的交易，处理平仓
    
    参数:
        action: 交易方向 ('BUY' 或 'SELL')
        quantity: 交易数量
        entry_price: 入场价格
        stop_price: 止损价格
        config_exit_strategy: 配置的退出策略 ('EOD' 或 'MAX_DURATION')
        config_max_hold_duration_minutes: 配置的最大持仓时间 (分钟)
    """
    try:
        # 使用东部时区初始化所有时间变量
        eastern_tz = pytz.timezone('US/Eastern')
        start_time = datetime.now(eastern_tz)
        last_update_time = start_time
        
        # 设置收盘前平仓的时间阈值 (3:50pm开始准备平仓)
        EOD_HOUR = 15  # 3pm
        EOD_MINUTE_START = 50  # 开始平仓的分钟
        # EOD_MINUTE_DEADLINE = 55  # 最晚平仓时间 (reference, not directly used in this check)

        logger.info(f"开始监控持仓 | {action} {quantity} | 入场: ${entry_price:.2f} | 止损: ${stop_price:.2f}")
        if config_exit_strategy == 'EOD':
            logger.info(f"Monitoring position. Exit strategy: EOD (close starting {EOD_HOUR}:{EOD_MINUTE_START} US/Eastern) or stop-loss.")
        elif config_exit_strategy == 'MAX_DURATION':
            logger.info(f"Monitoring position. Exit strategy: Max duration ({config_max_hold_duration_minutes} minutes) or stop-loss.")
        else:
            logger.warning(f"Unknown or invalid exit strategy: '{config_exit_strategy}'. Expected 'EOD' or 'MAX_DURATION'. Only stop-loss based exit will be active. Please check your 'exitStrategy' in config.ini.")

        # 检查是否有活跃的止损单
        has_active_stop = False
        open_orders = ib.reqAllOpenOrders()
        
        for order in open_orders:
            if order.order.orderType in ['STP', 'STOP'] and \
               abs(float(order.order.auxPrice) - stop_price) < 0.1:
                logger.info(f"活跃止损单确认 | ID: {order.order.orderId} | 价格: ${order.order.auxPrice:.2f}")
                has_active_stop = True
        
        if not has_active_stop:
            logger.warning("未检测到活跃的止损单，可能需要手动干预")
        
        # 监控循环
        is_position_closed = False
        update_interval = 60  # 每60秒(1分钟)更新一次持仓状态
        
        while not is_position_closed:
            current_time = datetime.now(eastern_tz)
            elapsed_minutes = (current_time - start_time).total_seconds() / 60
            time_since_last_update = (current_time - last_update_time).total_seconds()
            
            exit_triggered_by_strategy = False
            exit_reason_for_strategy = ""

            if config_exit_strategy == 'EOD':
                if current_time.hour == EOD_HOUR and current_time.minute >= EOD_MINUTE_START:
                    logger.info(f"EOD condition met ({EOD_HOUR}:{EOD_MINUTE_START}). Initiating market close.")
                    exit_reason_for_strategy = "EOD Market Close"
                    exit_triggered_by_strategy = True
            elif config_exit_strategy == 'MAX_DURATION':
                if elapsed_minutes >= config_max_hold_duration_minutes:
                    logger.info(f"Max hold duration ({config_max_hold_duration_minutes} min) reached. Initiating market close.")
                    exit_reason_for_strategy = f"Max Duration ({config_max_hold_duration_minutes} min) Reached"
                    exit_triggered_by_strategy = True

            if exit_triggered_by_strategy:
                close_position_at_market(action, quantity, entry_price, start_time, exit_reason_for_strategy)
                is_position_closed = True
                break
            
            # 检查持仓状态和止损单
            positions = ib.positions()
            position_exists = False
            
            for pos in positions:
                if pos.contract.symbol == contract.symbol:
                    position_exists = True
                    break
            
            # 如果持仓已关闭，记录并退出监控
            if not position_exists:
                logger.info("持仓已关闭，可能已触发止损")
                is_position_closed = True
                
                # 记录止损触发
                exit_price = stop_price
                trade_end_time = datetime.now(eastern_tz)
                duration_seconds = (trade_end_time - start_time).total_seconds()
                hours, remainder = divmod(duration_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                duration_str = f"{int(hours)}小时{int(minutes)}分钟{int(seconds)}秒"
                
                # 计算盈亏
                if action == 'BUY':
                    profit_loss = (exit_price - entry_price) * quantity
                    profit_percent = ((exit_price - entry_price) / entry_price) * 100
                else:  # SELL
                    profit_loss = (entry_price - exit_price) * quantity
                    profit_percent = ((entry_price - exit_price) / entry_price) * 100
                
                result = "Profit" if profit_loss > 0 else "Loss" if profit_loss < 0 else "Breakeven"
                logger.info(f"交易结束 | 止损触发 | {action} {quantity} | 持仓: {duration_str}")
                logger.info(f"入场: ${entry_price:.2f} → 出场: ${exit_price:.2f} | P/L: ${profit_loss:.2f} ({profit_percent:.2f}%) | 结果: {result}")
                
                # 更新交易记录
                for trade in trades_record:
                    if 'EntryPrice' in trade and abs(trade['EntryPrice'] - entry_price) < 0.1:
                        trade['ExitPrice'] = exit_price
                        trade['PnL'] = round(profit_loss, 2)
                        trade['PnLPercent'] = round(profit_percent, 2)
                        trade['Duration'] = duration_str
                        trade['Result'] = result
                        trade['ExitTime'] = trade_end_time.strftime('%Y-%m-%d %H:%M:%S')
                        trade['ExitReason'] = "Stop Loss Triggered"
                        break
                
                # 打印交易表格
                print_trade_table(action, entry_price, exit_price, quantity, profit_loss, profit_percent, duration_str, "Stop Loss Triggered")
                break
            
            # 更新持仓状态 (每1分钟更新一次)
            if time_since_last_update >= update_interval:
                # 获取当前市场价格
                ticker = ib.reqMktData(contract)
                ib.sleep(1)  # 等待数据返回
                current_market_price = ticker.marketPrice() if hasattr(ticker, 'marketPrice') and callable(ticker.marketPrice) else ticker.last
                ib.cancelMktData(contract)
                
                if current_market_price and current_market_price > 0:
                    # 计算当前盈亏
                    if action == 'BUY':
                        unrealized_pnl = (current_market_price - entry_price) * abs(quantity)
                        pnl_percent = ((current_market_price - entry_price) / entry_price) * 100
                    else:  # SELL
                        unrealized_pnl = (entry_price - current_market_price) * abs(quantity)
                        pnl_percent = ((entry_price - current_market_price) / entry_price) * 100
                    
                    # 确定盈亏状态
                    pnl_status = "盈利" if unrealized_pnl > 0 else "亏损" if unrealized_pnl < 0 else "持平"
                    
                    # 合并为一行输出，显示市场价格信息
                    logger.info(f"持仓状态 | 已持有: {elapsed_minutes:.1f}分钟 | 市场价: ${current_market_price:.2f} | 入场价: ${entry_price:.2f} | 止损价: ${stop_price:.2f} | P/L: ${unrealized_pnl:.2f} ({pnl_percent:.2f}%) | 状态: {pnl_status}")
                    
                    # 检查当前价格是否已经突破止损价
                    if action == 'BUY' and current_market_price <= stop_price:
                        logger.warning(f"价格警告: ${current_market_price:.2f} 已突破止损价 ${stop_price:.2f}，止损可能即将触发")
                    elif action == 'SELL' and current_market_price >= stop_price:
                        logger.warning(f"价格警告: ${current_market_price:.2f} 已突破止损价 ${stop_price:.2f}，止损可能即将触发")
                
                last_update_time = current_time
                
            ib.sleep(5)  # 短暂休息5秒再继续
        
        logger.info("交易监控结束")
        
    except Exception as e:
        logger.error(f"监控交易时发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())

# 市价平仓
def close_position_at_market(action, quantity, entry_price, start_time, determined_exit_reason):
    """
    以市价平仓当前持仓
    
    参数:
        action: 原交易方向
        quantity: 数量
        entry_price: 入场价格
        start_time: 交易开始时间
        determined_exit_reason: 调用者确定的平仓原因
    """
    try:
        # 确定平仓方向（与入场方向相反）
        close_action = 'SELL' if action == 'BUY' else 'BUY'
        
        # 取消所有活跃订单
        open_orders = ib.reqAllOpenOrders()
        if open_orders:
            logger.info(f"取消{len(open_orders)}个活跃订单")
            for order in open_orders:
                ib.cancelOrder(order.order)
        
        # 等待订单取消
        ib.sleep(2)
        
        # 检查当前时间
        eastern_tz = pytz.timezone('US/Eastern')
        # current_time = datetime.now(eastern_tz) # current_time not used here for reason
        # exit_reason = "到达最大持仓时间" # Replaced by determined_exit_reason
        
        # if current_time.hour == 15 and current_time.minute >= 45: # Old specific check, now handled by caller
        #     exit_reason = "收盘前平仓"
        #     logger.info(f"执行收盘前平仓 | 距收盘还有约 {60 - current_time.minute} 分钟")
        logger.info(f"Executing market close due to: {determined_exit_reason}")
        
        # 创建平仓市价单
        close_order = MarketOrder(
            action=close_action,
            totalQuantity=quantity,
            transmit=True
        )
        close_order.outsideRth = True # Allow filling outside regular trading hours
        
        logger.info(f"执行市价平仓 | {close_action} {quantity} | OutsideRTH: {close_order.outsideRth}")
        trade = ib.placeOrder(contract, close_order)
        
        # 等待平仓订单执行
        filled = False
        exit_price = None
        
        for attempt in range(40):
            ib.sleep(1)
            
            if hasattr(trade, 'orderStatus'):
                status = trade.orderStatus.status
                
                if attempt % 3 == 0 or status == 'Filled':
                    logger.info(f"平仓订单状态: {status} | 尝试: {attempt+1}/40")
                
                if status == 'Filled':
                    filled = True
                    exit_price = float(trade.orderStatus.avgFillPrice)
                    logger.info(f"平仓订单已成交 | 价格: ${exit_price:.2f}")
                    break
        
        if filled and exit_price:
            # 计算交易结果
            eastern_tz = pytz.timezone('US/Eastern')
            trade_end_time = datetime.now(eastern_tz)
            duration_seconds = (trade_end_time - start_time).total_seconds()
            hours, remainder = divmod(duration_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            duration_str = f"{int(hours)}小时{int(minutes)}分钟{int(seconds)}秒"
            
            if action == 'BUY':
                profit_loss = (exit_price - entry_price) * quantity
                profit_percent = ((exit_price - entry_price) / entry_price) * 100
            else:  # SELL
                profit_loss = (entry_price - exit_price) * quantity
                profit_percent = ((entry_price - exit_price) / entry_price) * 100
            
            result = "Profit" if profit_loss > 0 else "Loss" if profit_loss < 0 else "Breakeven"
            # exit_reason = "Market Close" if exit_reason == "收盘前平仓" else "Max Duration Reached" # Use determined_exit_reason
            
            # 简化日志输出
            logger.info(f"交易结束 | {action} {quantity} | 持仓时间: {duration_str} | 结果: {result}")
            
            # 更新交易记录
            for trade in trades_record:
                if 'EntryPrice' in trade and abs(trade['EntryPrice'] - entry_price) < 0.1:
                    trade['ExitPrice'] = exit_price
                    trade['PnL'] = round(profit_loss, 2)
                    trade['PnLPercent'] = round(profit_percent, 2)
                    trade['Duration'] = duration_str
                    trade['Result'] = result
                    trade['ExitTime'] = trade_end_time.strftime('%Y-%m-%d %H:%M:%S')
                    trade['ExitReason'] = determined_exit_reason # Use determined_exit_reason
                    break
            
            # 打印交易表格
            print_trade_table(action, entry_price, exit_price, quantity, profit_loss, profit_percent, duration_str, determined_exit_reason) # Use determined_exit_reason
            
            return True
        else:
            logger.warning("平仓订单未能成交")
            return False
            
    except Exception as e:
        logger.error(f"市价平仓时发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# 主逻辑
try:
    logger.info(f"Starting trading strategy for {contract.symbol}")
    logger.info(f"Account size: ${ACCOUNT_SIZE}, Leverage: {LEVERAGE}x, Risk per trade: {RISK_PCT*100}%")
    logger.info("执行单次交易模式，交易后将监控持仓")

    # 等待到下一个5分钟K线开始
    next_candle_start, next_candle_end = wait_for_next_5min_candle()
    
    # 等待当前K线形成完毕
    wait_for_candle_complete(next_candle_end)
    
    # 获取刚刚完成的K线数据
    eastern = pytz.timezone('US/Eastern')
    end_time_str = next_candle_end.astimezone(eastern).strftime('%Y%m%d %H:%M:%S US/Eastern') # Corrected timezone format
    logger.info(f"获取截至 {end_time_str} 的最新K线数据")
    
    # 获取5分钟K线数据
    df = get_historical_data(end_time_str, bar_size='5 mins', duration='1800 S')
    if df.empty or len(df) < 1:
        logger.warning("未能获取有效K线数据，程序退出")
        ib.disconnect()
        exit(1)
        
    # 获取最新的完整K线
    latest_bar = df.iloc[-1]
    
    # 验证K线时间是否为预期时间
    bar_time = latest_bar.date
    expected_time = next_candle_end - timedelta(seconds=1)
    time_diff = abs((bar_time - expected_time).total_seconds())
    
    if time_diff > 300:  # 如果时间差超过5分钟
        logger.warning(f"K线时间异常 | 实际: {bar_time} | 预期: {expected_time} | 差异: {time_diff}秒")
        logger.warning("数据可能不是最新的，程序退出")
        ib.disconnect()
        exit(1)
    
    # 获取日线数据计算ATR
    daily_df = get_bars('30 D', '1 day')
    if daily_df.empty or len(daily_df) < 14:
        logger.warning("日线数据不足，无法计算ATR，程序退出")
        ib.disconnect()
        exit(1)

    ATR = calculate_atr(daily_df)
    if pd.isna(ATR) or ATR <= 0:
        logger.warning(f"ATR计算错误: {ATR}，程序退出")
        ib.disconnect()
        exit(1)

    R = ATR * 0.1
    
    # 分析最新K线并生成交易信号
    price_change_percent = (latest_bar.close - latest_bar.open) / latest_bar.open * 100
    
    # 简化K线分析日志
    logger.info(f"K线分析 | 时间: {bar_time.strftime('%H:%M:%S')} | O: ${latest_bar.open:.2f} | H: ${latest_bar.high:.2f} | L: ${latest_bar.low:.2f} | C: ${latest_bar.close:.2f} | 变化: {price_change_percent:.2f}%")
    
    # 基于最新K线生成交易信号
    action = None
    stop_price_reference = None
    if price_change_percent > 0:
        action = 'BUY'
        raw_stop_price = latest_bar.close - R
        stop_price_reference = format_price(raw_stop_price)
        logger.info(f"信号: {action} | 方向: 上涨 (+{price_change_percent:.2f}%) | 参考止损: ${stop_price_reference:.2f}")
    elif price_change_percent < 0:
        action = 'SELL'
        raw_stop_price = latest_bar.close + R
        stop_price_reference = format_price(raw_stop_price)
        logger.info(f"信号: {action} | 方向: 下跌 ({price_change_percent:.2f}%) | 参考止损: ${stop_price_reference:.2f}")
    else:
        logger.info("无交易信号 | K线方向: 横盘 (0.00%)")
        logger.info("没有明确交易信号，程序退出")
        ib.disconnect()
        exit(0)

    # 计算头寸大小并打印详细计算过程
    # 1. 基于风险的头寸计算 (仓位风险 = ⌊账户资金×1% / R⌋)
    risk_amount = ACCOUNT_SIZE * RISK_PCT  # 每笔交易风险金额 = $25000 * 0.01 = $250
    qty_risk = int(risk_amount / R)  # 向下取整
    qty_risk = max(1, qty_risk)  # 确保至少为1
    
    # 2. 基于杠杆的头寸计算 (仓位杠杆 = ⌊账户资金×杠杆 / 当前价格⌋)
    max_position = ACCOUNT_SIZE * LEVERAGE  # 最大仓位大小 = $25000 * 4 = $100000
    qty_leverage = int(max_position / latest_bar.close)  # 向下取整
    qty_leverage = max(1, qty_leverage)  # 确保至少为1
    
    # 3. 最终下单数量取两者中较小者 (下单数量 = min(仓位风险, 仓位杠杆))
    qty = min(qty_risk, qty_leverage)
    
    # 计算实际风险和杠杆
    actual_risk_amount = qty * R
    actual_risk_pct = (actual_risk_amount / ACCOUNT_SIZE) * 100
    actual_position_size = qty * latest_bar.close
    actual_leverage = actual_position_size / ACCOUNT_SIZE
    
    # 简化位置大小计算输出
    logger.info(f"头寸计算 | ATR: {ATR:.2f} | R: {R:.2f} | 风险: {RISK_PCT*100}% | 杠杆: {LEVERAGE}x")
    logger.info(f"入场价格: ${latest_bar.close:.2f} | 数量: {qty} | 风险金额: ${actual_risk_amount:.2f} | 杠杆率: {actual_leverage:.2f}x")

    logger.info(f"交易信号: {action} {qty} @ ${latest_bar.close:.2f} | 参考止损: ${stop_price_reference:.2f}")

    # 最终检查 - 确保杠杆不超过限制
    if actual_leverage > LEVERAGE:
        max_allowed_qty = int(max_position / latest_bar.close)
        logger.warning(f"杠杆超限 | 从 {qty} 调整为 {max_allowed_qty} 单位 | 原杠杆: {actual_leverage:.2f}x")
        qty = max_allowed_qty
        # 重新计算实际数值
        actual_risk_amount = qty * R
        actual_position_size = qty * latest_bar.close
        actual_leverage = actual_position_size / ACCOUNT_SIZE
        logger.info(f"调整后头寸: {qty} 单位 | 市值: ${actual_position_size:.2f} | 新杠杆: {actual_leverage:.2f}x")

    # 将当前的K线保存到全局变量中，以便place_trade函数使用相同的价格
    signal_candle_data = {
        'time': bar_time,
        'open': latest_bar.open,
        'high': latest_bar.high,
        'low': latest_bar.low,
        'close': latest_bar.close,
        'volume': latest_bar.volume
    }
    
    # 执行交易
    logger.info("执行交易...")
    fill_price, trade_time = place_trade(action, qty, stop_price_reference)
    
    if fill_price:
        # 基于实际成交价格重新计算止损价格
        if action == 'BUY':
            stop_price = format_price(fill_price - R)
        else:  # SELL
            stop_price = format_price(fill_price + R)
            
        logger.info(f"交易执行成功: {action} {qty} @ ${fill_price:.2f}")
        logger.info(f"基于实际成交价重新计算止损价格: ${stop_price:.2f} (R = ${R:.2f})")
        
        # 重新设置止损单
        stoploss_success = place_stoploss_order(action, qty, stop_price, fill_price)
        if stoploss_success:
            logger.info(f"止损单已成功设置: ${stop_price:.2f}")
        else:
            logger.warning("止损单设置失败 - 请手动干预")
            
        # 计算交易风险金额和百分比
        risk_amount = qty * abs(fill_price - stop_price)
        risk_percent = (risk_amount / ACCOUNT_SIZE) * 100
        position_value = qty * fill_price
        leverage = position_value / ACCOUNT_SIZE
        
        # 打印交易信息摘要
        logger.info("\n" + "-"*50)
        logger.info("交易信息摘要")
        logger.info(f"方向: {'做多' if action == 'BUY' else '做空'} | 数量: {qty} | 标的: {contract.symbol}")
        logger.info(f"入场价格: ${fill_price:.2f} | 止损价格: ${stop_price:.2f} | 止损幅度: ${abs(fill_price - stop_price):.2f}")
        logger.info(f"持仓市值: ${position_value:.2f} | 账户杠杆: {leverage:.2f}x")
        logger.info(f"风险金额: ${risk_amount:.2f} | 账户风险: {risk_percent:.2f}%")
        logger.info("仓位计算过程:")
        logger.info(f"1. 风险头寸 = 账户资金 * 风险比例 / R值 = ${ACCOUNT_SIZE} * {RISK_PCT} / ${R:.2f} = {qty_risk} 单位")
        logger.info(f"2. 杠杆头寸 = 账户资金 * 杠杆 / 市价 = ${ACCOUNT_SIZE} * {LEVERAGE} / ${latest_bar.close:.2f} = {qty_leverage} 单位")
        logger.info(f"3. 最终下单数量 = min(风险头寸, 杠杆头寸) = min({qty_risk}, {qty_leverage}) = {qty} 单位")
        logger.info("-"*50 + "\n")
        
        trades_record.append({
            'Time': trade_time.strftime('%Y-%m-%d %H:%M:%S'),
            'Direction': action,
            'EntryPrice': fill_price,
            'Quantity': qty,
            'StopLoss': stop_price,
            'PnL': 0.0,
            'PnLPercent': 0.0,
            'Symbol': contract.symbol,
            'Duration': '',
            'Result': '',
            'ExitTime': '',
            'ExitReason': '',
            'ExitPrice': 0.0
        })
        
        # 监控交易并处理平仓
        logger.info("开始监控交易...")
        monitor_trade_and_exit(action, qty, fill_price, stop_price, trade_exit_strategy, trade_max_hold_duration_minutes)
        
        # 交易结束后打印报告
        print_daily_report()
        print_trade_summary()
    else:
        logger.warning("交易执行失败")
    
    logger.info("交易流程完成，程序退出")

except KeyboardInterrupt:
    logger.info("Strategy manually stopped")
except Exception as e:
    logger.error(f"Strategy error: {e}")
    import traceback
    logger.error(traceback.format_exc())
finally:
    # 确保在退出时关闭所有订单
    try:
        logger.info("清理所有活跃订单...")
        open_orders = ib.reqAllOpenOrders()
        for order in open_orders:
            logger.info(f"取消订单 ID: {order.order.orderId}")
            ib.cancelOrder(order.order)
    except Exception as e:
        logger.error(f"清理订单时发生错误: {e}")
    
    # 如果有交易记录，打印总结
    if trades_record:
        try:
            logger.info("打印最终交易报告...")
            print_daily_report()
            print_trade_summary()
        except Exception as e:
            logger.error(f"打印交易报告时出错: {e}")
    
    logger.info("Closing connection to IBKR")
    ib.disconnect()
