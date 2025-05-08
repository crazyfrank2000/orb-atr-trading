#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
计算TQQQ最近14天的ATR(平均真实波幅)
"""

import os
import sys
import time
import logging
import pytz
import traceback
import pandas as pd
from datetime import datetime, timedelta
from ib_insync import *

# 设置日志
def setup_logger():
    # 创建日志目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 生成基于时间的日志文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"atr_calc_{timestamp}.log")
    
    # 创建logger
    logger = logging.getLogger("atr_calc")
    logger.setLevel(logging.INFO)
    
    # 清除已存在的handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # 创建日志格式
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(log_format)
    file_handler.setFormatter(log_format)
    
    # 添加处理器到logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    logger.info(f"日志文件已创建: {log_file}")
    
    return logger

# 设置logger
logger = setup_logger()

def bars_to_dataframe(bars):
    """将IB的bars数据转换为pandas DataFrame"""
    df = pd.DataFrame({
        'datetime': [bar.date for bar in bars],
        'open': [bar.open for bar in bars],
        'high': [bar.high for bar in bars],
        'low': [bar.low for bar in bars],
        'close': [bar.close for bar in bars],
        'volume': [bar.volume for bar in bars]
    })
    df.set_index('datetime', inplace=True)
    return df

def calculate_atr_pandas(df, period=14):
    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.rolling(window=period, min_periods=period).mean()
    return atr

def save_data_to_csv(df, atr_values, symbol, output_dir='data'):
    # 创建数据目录
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(output_dir, f"{symbol}_data_{timestamp}.csv")
    
    # 复制数据框以添加ATR列
    result_df = df.copy()
    
    # 添加所有ATR值
    for period, atr in atr_values.items():
        result_df[f'ATR_{period}'] = atr
    
    # 添加ATR百分比列
    for period, atr in atr_values.items():
        result_df[f'ATR_{period}_Percent'] = (atr / result_df['close']) * 100
    
    # 保存到CSV
    result_df.to_csv(filename)
    
    return filename

def main():
    logger.info("TQQQ ATR计算程序启动")
    
    host = '127.0.0.1'
    port = 7497
    client_id = 1
    
    symbol = 'TQQQ'
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    contract.primaryExchange = 'NASDAQ'
    
    try:
        logger.info(f"连接到TWS @ {host}:{port}")
        app = IB()
        app.connect(host, port, client_id)
        if not app.isConnected():
            logger.error("无法连接到TWS，请确保TWS或IB Gateway正在运行")
            return
        
        logger.info("TWS连接成功")
        
        eastern = pytz.timezone('US/Eastern')
        now_et = datetime.now(eastern)
        logger.info(f"当前美东时间: {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取日K线数据
        logger.info("\n获取TQQQ日K线数据...")
        bars_all = app.reqHistoricalData(
            contract=contract,
            endDateTime='',
            durationStr='20 D',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=1,  # 仅使用常规交易时段数据
            formatDate=1
        )
        
        if not bars_all or len(bars_all) < 15:
            logger.error(f"未能获取足够的K线数据")
            return
        
        # 转换为DataFrame并计算ATR
        df_all = bars_to_dataframe(bars_all)
        
        # 获取最近14天的数据
        recent_data = df_all.tail(14)
        
        # 计算14天ATR
        period = 14
        atr_values = calculate_atr_pandas(df_all, period)
        recent_atr = atr_values.tail(14)
        
        # 获取日期范围
        start_date = recent_data.index[0].strftime('%Y-%m-%d')
        end_date = recent_data.index[-1].strftime('%Y-%m-%d')
        
        # 打印标题信息
        logger.info("\n========== TQQQ 最近14天 ATR 日报 ==========")
        logger.info(f"分析日期范围: {start_date} 至 {end_date}")
        logger.info(f"当前价格: ${recent_data['close'].iloc[-1]:.2f}")
        logger.info(f"当前ATR({period}): ${recent_atr.iloc[-1]:.2f}")
        logger.info(f"当前ATR({period})百分比: {(recent_atr.iloc[-1]/recent_data['close'].iloc[-1]*100):.2f}%")
        logger.info("\n=== 最近14天每日ATR数据 ===")
        logger.info("日期         收盘价     ATR      ATR%")
        logger.info("-" * 50)
        
        # 打印每天的ATR数据
        for idx, row in recent_data.iterrows():
            date_str = idx.strftime('%Y-%m-%d')
            close_price = row['close']
            atr_value = recent_atr.loc[idx]
            atr_percent = (atr_value / close_price) * 100
            logger.info(f"{date_str}  ${close_price:.2f}    ${atr_value:.2f}    {atr_percent:.2f}%")
        
        logger.info("-" * 50)
        
        # 保存到CSV
        recent_atr_values = {period: recent_atr}
        csv_file = save_data_to_csv(recent_data, recent_atr_values, symbol)
        logger.info(f"\n数据已保存到: {csv_file}")
        
    except Exception as e:
        logger.error(f"发生错误: {e}")
        logger.error(traceback.format_exc())
    
    finally:
        if 'app' in locals() and app.isConnected():
            app.disconnect()
            logger.info("已断开TWS连接")

if __name__ == "__main__":
    main() 