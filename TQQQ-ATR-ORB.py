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

# 全局变量用于存储信号K线数据和交易记录
signal_candle_data = None
trades_record = []

# 确保logs文件夹存在
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
    print(f"Created logs directory: {logs_dir}")

# 设置日志格式
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            record.levelname = ''
        return super().format(record)

# 创建文件处理器
file_handler = logging.FileHandler(os.path.join(logs_dir, "tqqq_atr_trading.log"))
file_handler.setFormatter(CustomFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(CustomFormatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

# 配置logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False  # 防止日志传播到根logger

# 清除现有的处理器
logger.handlers = []

# 添加处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 设置ib_insync日志级别为WARNING，减少冗余输出
util.logToConsole(logging.WARNING)

# 连接到IBKR
ib = IB()
try:
    ib.connect('127.0.0.1', 7497, clientId=1)
    logger.info("Connected to IBKR")
except Exception as e:
    logger.error(f"Failed to connect to IBKR: {e}")
    exit(1)

# 获取TQQQ合约
def get_tqqq_contract():
    """获取TQQQ ETF合约"""
    try:
        logger.info("获取TQQQ ETF合约...")
        contract = Stock(symbol="TQQQ", exchange="SMART", currency="USD")
        
        # 使用qualifyContracts获取完整合约信息
        contracts = ib.qualifyContracts(contract)
        if contracts:
            contract = contracts[0]
            logger.info(f"成功获取TQQQ合约: {contract}")
            
            # 测试能否获取K线数据
            try:
                bars = ib.reqHistoricalData(
                    contract, 
                    endDateTime='', 
                    durationStr='1 D', 
                    barSizeSetting='1 hour',
                    whatToShow='TRADES',
                    useRTH=False  # 设置为False以测试包含夜盘数据
                )
                if bars:
                    logger.info(f"成功获取 {len(bars)} 根TQQQ K线数据 (包含夜盘数据)")
                    return contract
            except Exception as e:
                logger.warning(f"无法获取TQQQ历史数据: {e}")
    except Exception as e:
        logger.error(f"获取TQQQ合约失败: {e}")
    
    logger.error("无法获取有效的TQQQ合约")
    return None

# 获取合约
contract = get_tqqq_contract()
if contract is None:
    logger.error("无法获取TQQQ合约，程序退出")
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
            whatToShow='TRADES',
            useRTH=False  # 设置为False以包含夜盘数据
        )
        if not bars:
            logger.warning(f"No historical data returned for {duration_formatted} {bar_size}")
            return pd.DataFrame()

        df = util.df(bars)
        logger.info(f"成功获取到 {len(df)} 根K线数据，包含夜盘数据")
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
        end_time_str = bar_end_time.strftime('%Y%m%d %H:%M:%S')
        
        logger.info(f"获取截至 {end_time_str} 的最新完整5分钟K线")
        
        # 请求历史数据，获取2根K线
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_time_str,
            durationStr='1800 S',  # 使用1800秒(30分钟)，确保包含至少几根5分钟K线
            barSizeSetting='5 mins',
            whatToShow='TRADES',
            useRTH=False  # 设置为False以包含夜盘数据
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
def format_price(price, tick_size=0.01):
    """根据合约精度格式化价格，确保符合交易所要求"""
    # 对于TQQQ，只保留两位小数
    return round(price * 100) / 100

# 下单函数
def place_trade(action, quantity, stop_price):
    """主要下单函数，执行交易并添加止损单"""
    try:
        # 先取消所有现有订单
        cancel_all_orders()
        
        # 获取合约详情并打印确认信息
        logger.info("=" * 50)
        logger.info(f"开始交易执行 - {contract.symbol} ({contract.secType}) 在 {contract.exchange}")
        if hasattr(contract, 'localSymbol') and contract.localSymbol:
            logger.info(f"合约详情: {contract.localSymbol}")
        
        # 使用全局存储的信号K线数据
        global signal_candle_data
        if not signal_candle_data:
            logger.error("无法获取信号K线数据，无法下单")
            return None, None
        
        # 使用信号K线的收盘价作为限价单价格
        limit_price = format_price(signal_candle_data['close'])
        logger.info(f"使用信号K线收盘价作为限价单价格: {limit_price}")
        
        # 创建限价单并设置唯一引用ID
        order_ref = f"Entry_{datetime.now().strftime('%H%M%S')}"
        order = LimitOrder(action, quantity, limit_price)
        order.orderRef = order_ref
        order.transmit = True
        logger.info(f"创建{action}限价单，数量: {quantity}，价格: {limit_price}，引用ID: {order_ref}")
        
        # 下订单并获取订单ID
        trade = ib.placeOrder(contract, order)
        if hasattr(trade, 'order') and hasattr(trade.order, 'orderId'):
            order_id = trade.order.orderId
            logger.info(f"订单已提交，ID: {order_id}")
        else:
            logger.warning("无法获取订单ID")
        
        # 等待订单执行
        filled = False
        fill_price = None
        start_time = time.time()
        price_adjustment_count = 0
        max_attempts = 20  # 最多等待20秒
        
        for attempt in range(max_attempts):
            ib.sleep(1)
            
            # 查询实时订单状态
            if hasattr(trade, 'orderStatus'):
                status = trade.orderStatus.status
                filled_qty = trade.orderStatus.filled if hasattr(trade.orderStatus, 'filled') else 0
                
                logger.info(f"订单状态 [尝试 {attempt+1}/{max_attempts}]: {status}, 已成交: {filled_qty}/{quantity}")
                
                if status == 'Filled':
                    filled = True
                    fill_price = float(trade.orderStatus.avgFillPrice)
                    logger.info(f"订单全部成交，均价: {fill_price}")
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
                    
                    logger.info(f"订单20秒未成交，调整价格 {limit_price} -> {new_limit_price}")
                    
                    # 取消当前订单
                    ib.cancelOrder(trade.order)
                    ib.sleep(1)  # 等待取消处理
                    
                    # 创建新订单
                    limit_price = new_limit_price
                    new_order_ref = f"EntryAdj{price_adjustment_count}_{datetime.now().strftime('%H%M%S')}"
                    new_order = LimitOrder(action, quantity, limit_price)
                    new_order.orderRef = new_order_ref
                    new_order.transmit = True
                    
                    logger.info(f"创建新{action}限价单，数量: {quantity}，调整后价格: {limit_price}，引用ID: {new_order_ref}")
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
                    logger.info(f"当前持仓确认: {pos.position} {pos.contract.symbol} @ {pos.avgCost}")
            
            if not position_found:
                logger.warning(f"交易成交后无法在持仓中找到 {contract.symbol}")
            
            # 下止损单
            logger.info("开始设置止损单...")
            stoploss_success = place_stoploss_order(action, quantity, stop_price, fill_price)
            
            # 验证止损单是否成功
            if stoploss_success:
                logger.info("止损单成功设置")
                
                # 验证开放订单列表
                open_orders = ib.reqAllOpenOrders()
                stop_orders_found = 0
                for o in open_orders:
                    if o.order.orderType in ['STP', 'STOP', 'LMT']:
                        logger.info(f"活跃止损单: {o.order.action} {o.order.totalQuantity} @ {o.order.auxPrice if hasattr(o.order, 'auxPrice') else 'Unknown'}")
                        stop_orders_found += 1
                
                if stop_orders_found == 0:
                    logger.warning("警告: 止损单标记为成功但未在活跃订单列表中找到")
            else:
                logger.warning("止损单设置失败 - 需要手动干预")
            
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
    
    # 格式化价格，确保符合交易所要求（两位小数）
    formatted_stop_price = format_price(stop_price)
    logger.info(f"设置{sl_action}止损单，价格: {formatted_stop_price:.2f}")
    
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
            logger.info(f"止损单已提交，ID: {order_id}")
        else:
            logger.warning("无法获取止损单ID")
            return False
        
        # 等待订单状态更新
        for i in range(10):
            ib.sleep(1)
            
            # 检查订单状态
            if hasattr(sl_trade, 'orderStatus'):
                status = sl_trade.orderStatus.status
                logger.info(f"止损单状态 [尝试 {i+1}/10]: {status}")
                
                if status in ['Submitted', 'PreSubmitted', 'Filled']:
                    logger.info(f"止损单已被接受: {status}")
                    return True
                elif status == 'PendingSubmit' and i >= 8:
                    # 如果长时间处于PendingSubmit状态，尝试微调价格并重新提交
                    logger.warning("止损单卡在PendingSubmit状态，调整价格重试")
                    ib.cancelOrder(sl_trade.order)
                    ib.sleep(2)
                    
                    # 微调价格并重新提交 - 确保新价格也是两位小数
                    adjustment = 0.01  # 一分钱的调整
                    new_price = format_price(formatted_stop_price + (adjustment if sl_action == 'BUY' else -adjustment))
                    new_order = StopOrder(sl_action, quantity, new_price, tif='GTC')
                    new_order.outsideRth = True
                    new_order.transmit = True
                    new_order.orderRef = f"StopRetry_{datetime.now().strftime('%H%M%S')}"
                    
                    logger.info(f"重新提交调整后价格的止损单: {new_price:.2f}")
                    new_trade = ib.placeOrder(contract, new_order)
                    
                    # 等待新订单状态
                    ib.sleep(3)
                    if hasattr(new_trade, 'orderStatus'):
                        new_status = new_trade.orderStatus.status
                        logger.info(f"重提交止损单状态: {new_status}")
                        if new_status in ['Submitted', 'PreSubmitted']:
                            return True
            
            # 同时检查开放订单列表
            open_orders = ib.reqAllOpenOrders()
            for order in open_orders:
                if (hasattr(order, 'orderId') and hasattr(sl_trade, 'order') and 
                    order.orderId == sl_trade.order.orderId):
                    logger.info(f"在开放订单列表中找到止损单 ID: {order.orderId}")
                    return True
        
        # 如果循环结束仍未确认，进行最后检查
        final_orders = ib.reqAllOpenOrders()
        for order in final_orders:
            if order.orderType in ['STP', 'STOP'] and order.action == sl_action:
                logger.info(f"最终确认: 找到{sl_action}止损单")
                return True
        
        logger.warning("无法确认止损单状态")
        return False
        
    except Exception as e:
        logger.error(f"设置止损单时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# 取消所有现有订单的辅助函数
def cancel_all_orders():
    """取消所有现有订单，确保正确处理ID"""
    try:
        # 先通过trades()获取订单
        for trade in ib.trades():
            if hasattr(trade, 'order') and hasattr(trade.order, 'orderId'):
                order_id = trade.order.orderId
                if order_id > 0:  # 避免ID为0的情况
                    logger.info(f"取消订单 ID: {order_id}")
                    ib.cancelOrder(trade.order)
        
        # 同时通过reqAllOpenOrders()获取订单
        open_orders = ib.reqAllOpenOrders()
        
        if open_orders:
            logger.info(f"发现{len(open_orders)}个开放订单")
            
            # 逐个取消
            for order in open_orders:
                if hasattr(order, 'orderId') and order.orderId > 0:
                    try:
                        logger.info(f"取消开放订单 ID: {order.orderId}")
                        ib.cancelOrder(order)
                    except Exception as e:
                        logger.warning(f"取消订单{order.orderId}时出错: {e}")
        
        # 给系统时间处理取消请求
        ib.sleep(3)
        
    except Exception as e:
        logger.error(f"取消订单时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())

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
            whatToShow='TRADES',
            useRTH=False  # 设置为False以包含夜盘数据
        )
        
        if not bars or len(bars) == 0:
            logger.warning(f"未能获取历史数据")
            return pd.DataFrame()
        
        df = util.df(bars)
        logger.info(f"成功获取到 {len(df)} 根K线数据，包含夜盘数据")
        return df
        
    except Exception as e:
        logger.error(f"获取历史数据时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()

# 日报记录
trades_record = []

def print_daily_report():
    if not trades_record:
        logger.info("No trades executed today")
        return

    # 准备当前交易数据
    current_trades_df = pd.DataFrame(trades_record)
    total_current_trades = len(current_trades_df)
    
    # 确保reports文件夹存在
    reports_dir = "reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
        logger.info(f"Created reports directory: {reports_dir}")
    
    # 使用固定的文件名，不包含日期
    symbol_name = contract.symbol if hasattr(contract, 'symbol') else "TQQQ"
    csv_filename = os.path.join(reports_dir, f'trades_{symbol_name}_history.csv')
    
    # 检查文件是否已存在，如果存在则加载现有数据
    all_trades_df = current_trades_df.copy()  # 默认情况下使用当前交易
    if os.path.exists(csv_filename):
        try:
            existing_trades_df = pd.read_csv(csv_filename)
            logger.info(f"找到现有交易记录，包含 {len(existing_trades_df)} 笔交易")
            
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
def monitor_trade_and_exit(action, quantity, entry_price, stop_price, max_duration_minutes=480):
    try:
        eastern_tz = pytz.timezone('US/Eastern')
        start_time = datetime.now(eastern_tz)
        last_update_time = start_time
        logger.info(f"开始监控交易: {action} {quantity} @ {entry_price}, 止损价: {stop_price}")
        
        EOD_HOUR = 15
        EOD_MINUTE_START = 50
        EOD_MINUTE_DEADLINE = 55
        
        is_position_closed = False
        last_position_check = start_time
        position_check_interval = 60  # 每60秒检查一次持仓状态
        
        while not is_position_closed:
            current_time = datetime.now(eastern_tz)
            elapsed_minutes = (current_time - start_time).total_seconds() / 60
            time_since_last_position_check = (current_time - last_position_check).total_seconds()
            
            if current_time.hour == EOD_HOUR and current_time.minute >= EOD_MINUTE_START:
                logger.info(f"接近收盘时间，执行收盘前平仓")
                close_position_at_market(action, quantity, entry_price, start_time)
                is_position_closed = True
                break
            
            if time_since_last_position_check >= position_check_interval:
                last_position_check = current_time
                
                # 检查持仓状态
                positions = ib.positions()
                position_exists = False
                
                for pos in positions:
                    if pos.contract.symbol == contract.symbol:
                        position_exists = True
                        break
                
                if not position_exists:
                    logger.info("仓位已关闭，可能已触发止损")
                    is_position_closed = True
                    break
                
                # 获取当前市场价格
                ticker = ib.reqMktData(contract)
                ib.sleep(1)
                current_market_price = ticker.marketPrice() if hasattr(ticker, 'marketPrice') and callable(ticker.marketPrice) else ticker.last
                ib.cancelMktData(contract)
                
                if current_market_price and current_market_price > 0:
                    # 计算当前盈亏
                    if action == 'BUY':
                        unrealized_pnl = (current_market_price - entry_price) * quantity
                        pnl_percent = ((current_market_price - entry_price) / entry_price) * 100
                    else:
                        unrealized_pnl = (entry_price - current_market_price) * quantity
                        pnl_percent = ((entry_price - current_market_price) / entry_price) * 100
                    
                    pnl_status = "盈利" if unrealized_pnl > 0 else "亏损" if unrealized_pnl < 0 else "持平"
                    
                    logger.info(f"持仓状态 [{elapsed_minutes:.1f}分钟] - 市价: {current_market_price:.2f} | 入场: {entry_price:.2f} | 止损: {stop_price:.2f} | P/L: ${unrealized_pnl:.2f} ({pnl_percent:.2f}%) | 状态: {pnl_status}")
            
            ib.sleep(5)
            
        logger.info("交易监控结束")
        
    except Exception as e:
        logger.error(f"监控交易时发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())

# 市价平仓
def close_position_at_market(action, quantity, entry_price, start_time):
    """
    以市价平仓当前持仓
    
    参数:
        action: 原交易方向
        quantity: 数量
        entry_price: 入场价格
        start_time: 交易开始时间
    """
    try:
        # 确定平仓方向（与入场方向相反）
        close_action = 'SELL' if action == 'BUY' else 'BUY'
        
        # 取消所有活跃订单
        open_orders = ib.reqAllOpenOrders()
        for order in open_orders:
            logger.info(f"取消订单 ID: {order.order.orderId}")
            ib.cancelOrder(order.order)
        
        # 等待订单取消
        ib.sleep(2)
        
        # 检查当前时间，如果接近收盘则记录
        eastern_tz = pytz.timezone('US/Eastern')
        current_time = datetime.now(eastern_tz)
        is_near_close = False
        if current_time.hour == 15 and current_time.minute >= 45:
            is_near_close = True
            minutes_to_close = 60 - current_time.minute  # 到4:00pm的分钟数
            logger.info(f"执行收盘前平仓，距市场收盘还有约 {minutes_to_close} 分钟")
        
        # 创建平仓市价单
        close_order = MarketOrder(
            action=close_action,
            totalQuantity=quantity,
            transmit=True
        )
        
        logger.info(f"执行市价平仓: {close_action} {quantity} 股")
        trade = ib.placeOrder(contract, close_order)
        
        # 等待平仓订单执行
        filled = False
        exit_price = None
        
        for attempt in range(10):
            ib.sleep(1)
            
            if hasattr(trade, 'orderStatus'):
                status = trade.orderStatus.status
                logger.info(f"平仓订单状态 [尝试 {attempt+1}/10]: {status}")
                
                if status == 'Filled':
                    filled = True
                    exit_price = float(trade.orderStatus.avgFillPrice)
                    logger.info(f"平仓订单成交，价格: {exit_price:.2f}")
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
            
            result = "盈利" if profit_loss > 0 else "亏损" if profit_loss < 0 else "持平"
            exit_reason = "收盘前平仓" if is_near_close else "到达最大持仓时间"
            
            logger.info(f"\n===== 交易总结 =====")
            logger.info(f"交易方向: {action}")
            logger.info(f"交易数量: {quantity}")
            logger.info(f"入场价格: ${entry_price:.2f}")
            logger.info(f"出场价格: ${exit_price:.2f}")
            logger.info(f"持仓时间: {duration_str}")
            logger.info(f"盈亏金额: ${profit_loss:.2f} ({profit_percent:.2f}%)")
            logger.info(f"交易结果: {result}")
            logger.info(f"出场原因: {exit_reason}")
            logger.info("===================")
            
            # 更新交易记录
            for trade in trades_record:
                if 'EntryPrice' in trade and abs(trade['EntryPrice'] - entry_price) < 0.1:
                    trade['ExitPrice'] = exit_price
                    trade['PnL'] = profit_loss
                    trade['PnLPercent'] = profit_percent
                    trade['Duration'] = duration_str
                    trade['Result'] = result
                    trade['ExitTime'] = trade_end_time.strftime('%Y-%m-%d %H:%M:%S')
                    trade['ExitReason'] = exit_reason
                    break
            
            return True
        else:
            logger.warning("平仓订单未能成交")
            return False
            
    except Exception as e:
        logger.error(f"市价平仓时发生错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

# 打印本次交易详细总结
def print_trade_summary():
    """打印本次运行的交易详细总结"""
    if not trades_record:
        logger.info("本次运行没有执行交易")
        return
    
    logger.info("\n" + "="*50)
    logger.info("本次运行交易详细总结")
    logger.info("="*50)
    
    for i, trade in enumerate(trades_record, 1):
        # 提取交易信息
        direction = trade.get('Direction', '')
        entry_price = trade.get('EntryPrice', 0)
        exit_price = trade.get('ExitPrice', 0)
        quantity = trade.get('Quantity', 0)
        stop_loss = trade.get('StopLoss', 0)
        pnl = trade.get('PnL', 0)
        pnl_percent = trade.get('PnLPercent', 0)
        duration = trade.get('Duration', '')
        entry_time = trade.get('Time', '')
        exit_time = trade.get('ExitTime', '')
        result = trade.get('Result', '')
        exit_reason = trade.get('ExitReason', '')
        
        # 检查交易是否已结束
        trade_status = "已平仓" if exit_price > 0 else "持仓中"
        
        # 打印交易摘要
        logger.info(f"交易 #{i}: {direction} {quantity} TQQQ @ ${entry_price:.2f} [{trade_status}]")
        
        # 打印详细信息
        logger.info(f"  入场时间: {entry_time}")
        logger.info(f"  入场价格: ${entry_price:.2f}")
        logger.info(f"  止损价格: ${stop_loss:.2f}")
        
        if exit_price > 0:  # 如果交易已平仓
            logger.info(f"  出场时间: {exit_time}")
            logger.info(f"  出场价格: ${exit_price:.2f}")
            logger.info(f"  持仓时间: {duration}")
            logger.info(f"  盈亏金额: ${pnl:.2f} ({pnl_percent:.2f}%)")
            logger.info(f"  交易结果: {result}")
            logger.info(f"  出场原因: {exit_reason}")
        else:
            # 计算未实现盈亏 (如果可能)
            try:
                # 尝试获取当前市场价格
                ticker = ib.reqMktData(contract)
                ib.sleep(1)
                current_price = ticker.marketPrice() if hasattr(ticker, 'marketPrice') and callable(ticker.marketPrice) else ticker.last
                ib.cancelMktData(contract)
                
                if current_price and current_price > 0:
                    if direction == 'BUY':
                        unrealized_pnl = (current_price - entry_price) * quantity
                        unrealized_pct = ((current_price - entry_price) / entry_price) * 100
                    else:  # SELL
                        unrealized_pnl = (entry_price - current_price) * quantity
                        unrealized_pct = ((entry_price - current_price) / entry_price) * 100
                    
                    logger.info(f"  当前市价: ${current_price:.2f}")
                    logger.info(f"  未实现盈亏: ${unrealized_pnl:.2f} ({unrealized_pct:.2f}%)")
                    logger.info(f"  盈亏状态: {'盈利' if unrealized_pnl > 0 else '亏损' if unrealized_pnl < 0 else '持平'}")
            except Exception as e:
                logger.warning(f"无法计算未实现盈亏: {e}")
        
        # 在交易记录之间添加分隔符
        logger.info("-"*40)
    
    logger.info("="*50)

# 主逻辑
try:
    logger.info(f"Starting trading strategy for {contract.symbol}")
    logger.info(f"Account size: ${ACCOUNT_SIZE}, Leverage: {LEVERAGE}x, Risk per trade: {RISK_PCT*100}%")
    logger.info("执行单次交易模式，交易后将监控持仓")

    # 设置收盘前时间阈值
    EOD_HOUR = 15  # 3pm
    EOD_MINUTE_START = 50  # 开始平仓的分钟
    
    # 检查是否市场即将收盘
    current_time = datetime.now(pytz.timezone('US/Eastern'))
    if current_time.hour == EOD_HOUR and current_time.minute >= (EOD_MINUTE_START - 10):  # 3:40pm之后不再开仓
        minutes_to_close = (EOD_MINUTE_START - current_time.minute) if current_time.minute < EOD_MINUTE_START else 0
        logger.warning(f"市场接近收盘时间 ({EOD_HOUR}:{EOD_MINUTE_START})，距离平仓时间仅剩约 {minutes_to_close} 分钟，今日不再开仓")
        ib.disconnect()
        exit(0)

    # 等待到下一个5分钟K线开始
    next_candle_start, next_candle_end = wait_for_next_5min_candle()
    
    # 等待当前K线形成完毕
    wait_for_candle_complete(next_candle_end)
    
    # 获取刚刚完成的K线数据
    eastern = pytz.timezone('US/Eastern')
    end_time_str = next_candle_end.astimezone(eastern).strftime('%Y%m%d %H:%M:%S')
    logger.info(f"获取截至 {end_time_str} 的最新K线数据")
    
    # 获取5分钟K线数据
    df = get_historical_data(end_time_str, bar_size='5 mins', duration='1800 S')
    if df.empty or len(df) < 1:
        logger.warning("未能获取有效K线数据，程序退出")
        ib.disconnect()
        exit(1)
        
    # 获取最新的完整K线
    latest_bar = df.iloc[-1]
    
    # 验证K线时间是否为预期时间（允许小误差）
    bar_time = latest_bar.date
    expected_time = next_candle_end - timedelta(seconds=1)  # 预期时间应该是K线结束时间前1秒
    time_diff = abs((bar_time - expected_time).total_seconds())
    
    if time_diff > 300:  # 如果时间差超过5分钟
        logger.warning(f"K线时间({bar_time})与预期时间({expected_time})相差过大: {time_diff}秒")
        logger.warning("数据可能不是最新的，程序退出")
        ib.disconnect()
        exit(1)
    
    # 获取日线数据计算ATR
    daily_df = get_bars('30 D', '1 day')  # 这里已经在get_bars函数内部处理了格式
    if daily_df.empty or len(daily_df) < 14:
        logger.warning("不足日线数据计算ATR，程序退出")
        ib.disconnect()
        exit(1)

    ATR = calculate_atr(daily_df)
    if pd.isna(ATR) or ATR <= 0:
        logger.warning(f"ATR值无效: {ATR}，程序退出")
        ib.disconnect()
        exit(1)

    R = ATR * 0.1
    logger.info(f"ATR: {ATR:.2f}, R value: {R:.2f}")

    # 分析最新K线并生成交易信号
    price_change_percent = (latest_bar.close - latest_bar.open) / latest_bar.open * 100
    
    # 保存最新K线的收盘价，确保所有地方使用相同的价格
    current_price = latest_bar.close
    
    logger.info(f"5分钟K线分析:")
    logger.info(f"时间: {bar_time}")
    logger.info(f"开盘: {latest_bar.open:.2f}")
    logger.info(f"最高: {latest_bar.high:.2f}")
    logger.info(f"最低: {latest_bar.low:.2f}")
    logger.info(f"收盘: {current_price:.2f} (Current price)")
    logger.info(f"成交量: {latest_bar.volume}")
    logger.info(f"价格变化: {price_change_percent:.2f}%")
    
    # 基于最新K线生成交易信号
    action = None
    if price_change_percent > 0:
        action = 'BUY'
        raw_stop_price = current_price - R
        stop_price = format_price(raw_stop_price)
        logger.info(f"K线方向: 上涨 (+{price_change_percent:.2f}%)")
        logger.info("交易信号: 买入")
    elif price_change_percent < 0:
        action = 'SELL'
        raw_stop_price = current_price + R
        stop_price = format_price(raw_stop_price)
        logger.info(f"K线方向: 下跌 ({price_change_percent:.2f}%)")
        logger.info("交易信号: 卖出")
    else:
        logger.info("K线方向: 横盘/十字星 (0.00%)")
        logger.info("交易信号: 不交易")
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
    qty_leverage = int(max_position / current_price)  # 向下取整
    qty_leverage = max(1, qty_leverage)  # 确保至少为1
    
    # 3. 最终下单数量取两者中较小者 (下单数量 = min(仓位风险, 仓位杠杆))
    qty = min(qty_risk, qty_leverage)
    
    # 计算实际风险和杠杆
    actual_risk_amount = qty * R
    actual_risk_pct = (actual_risk_amount / ACCOUNT_SIZE) * 100
    actual_position_size = qty * current_price
    actual_leverage = actual_position_size / ACCOUNT_SIZE
    
    # 打印详细计算过程
    logger.info("==== Position Sizing Calculation ====")
    logger.info(f"ATR: {ATR:.2f}, R值(ATR*10%): {R:.2f}")
    logger.info(f"Current price: {current_price:.2f}, Stop loss: {stop_price:.2f}")
    logger.info(f"Stop distance: {abs(current_price - stop_price):.2f} points")
    logger.info(f"Account size: ${ACCOUNT_SIZE}, Risk per trade: {RISK_PCT*100}%")
    
    # 计算公式展示
    logger.info("计算公式:")
    logger.info(f"仓位风险 = [账户资金×1% / R] = [${ACCOUNT_SIZE} × {RISK_PCT} / ${R:.2f}] = [{risk_amount / R:.2f}] = {qty_risk}")
    logger.info(f"仓位杠杆 = [账户资金×杠杆 / 当前价格] = [${ACCOUNT_SIZE} × {LEVERAGE} / ${current_price:.2f}] = [{max_position / current_price:.2f}] = {qty_leverage}")
    logger.info(f"下单数量 = min(仓位风险, 仓位杠杆) = min({qty_risk}, {qty_leverage}) = {qty}")
    
    # 实际风险和头寸大小
    logger.info(f"Actual dollar risk: ${actual_risk_amount:.2f} ({actual_risk_pct:.2f}% of account)")
    logger.info(f"Actual position size: ${actual_position_size:.2f} ({actual_leverage:.2f}x leverage)")
    logger.info("====================================")

    logger.info(f"Signal: {action} {qty} @ {current_price:.2f}, SL: {stop_price:.2f}")

    # 最终检查 - 确保杠杆不超过限制
    if actual_leverage > LEVERAGE:
        max_allowed_qty = int(max_position / current_price)
        logger.warning(f"计算的头寸大小导致杠杆({actual_leverage:.2f}x)超过限制({LEVERAGE}x)")
        logger.warning(f"将头寸大小从 {qty} 调整为 {max_allowed_qty} 以符合杠杆限制")
        qty = max_allowed_qty
        # 重新计算实际数值
        actual_risk_amount = qty * R
        actual_risk_pct = (actual_risk_amount / ACCOUNT_SIZE) * 100
        actual_position_size = qty * current_price
        actual_leverage = actual_position_size / ACCOUNT_SIZE
        logger.info(f"调整后头寸: {qty} 单位, 市值: ${actual_position_size:.2f}, 杠杆: {actual_leverage:.2f}x")

    # 将当前的K线保存到全局变量中，以便place_trade函数使用相同的价格
    signal_candle_data = {
        'time': bar_time,
        'open': latest_bar.open,
        'high': latest_bar.high,
        'low': latest_bar.low,
        'close': current_price,
        'volume': latest_bar.volume
    }
    
    # 执行交易
    logger.info("执行交易...")
    fill_price, trade_time = place_trade(action, qty, stop_price)
    
    if fill_price:
        # 成交后基于实际成交价重新计算止损价
        if action == 'BUY':
            stop_price = format_price(fill_price - R)
        else:
            stop_price = format_price(fill_price + R)
        logger.info(f"交易执行成功: {action} {qty} @ ${fill_price:.2f}, 止损价: ${stop_price:.2f}")

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
        logger.info("仓位计算:")
        logger.info(f"ATR: {ATR:.2f}, R值: {R:.2f}")
        logger.info(f"风险仓位: {qty_risk} (${risk_amount:.2f}风险)")
        logger.info(f"杠杆仓位: {qty_leverage} ({leverage:.2f}x杠杆)")
        logger.info(f"最终仓位: {qty} (取较小值)")
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
        monitor_trade_and_exit(action, qty, fill_price, stop_price, max_duration_minutes=480)
        
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
    # 如果有交易记录，打印总结
    if trades_record:
        try:
            logger.info("打印最终交易报告...")
            print_daily_report()
            print_trade_summary()
        except Exception as e:
            logger.error(f"打印交易报告时出错: {e}")

    # 确保在退出时关闭所有订单
    try:
        logger.info("清理所有活跃订单...")
        open_orders = ib.reqAllOpenOrders()
        for order in open_orders:
            logger.info(f"取消订单 ID: {order.order.orderId}")
            ib.cancelOrder(order.order)
    except Exception as e:
        logger.error(f"清理订单时发生错误: {e}")
    
    logger.info("Closing connection to IBKR")
    ib.disconnect()
