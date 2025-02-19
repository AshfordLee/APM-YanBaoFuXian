import pandas as pd
from APM因子构造 import APM
import numpy as np
from tqdm import tqdm

def prepare_watch_date_list(start,end):
    #获取start到end时间段中每个星期最后一天并且
    #判断是不是交易日
    #如果是交易日，则加入到列表中

    try:
        # 1. 转换日期格式
        start_date = pd.to_datetime(start, format='%Y%m%d')
        end_date = pd.to_datetime(end, format='%Y%m%d')
        
        # 2. 读取交易日历数据
        trade_dates = pd.read_csv("数据/沪深交易所交易日历.csv")
        trade_dates['trade_date'] = pd.to_datetime(trade_dates['cal_date'], format='%Y%m%d')
        trade_dates_set = set(trade_dates['trade_date'].dt.strftime('%Y%m%d'))
        
        # 3. 找到起始日期后的第一个交易日
        current_date = start_date
        while current_date.strftime('%Y%m%d') not in trade_dates_set:
            current_date += pd.Timedelta(days=1)
            if current_date > end_date:
                return []
        
        # 4. 每次前进7天找交易日
        watchback_date_set = []
        while current_date <= end_date:
            # 添加当前交易日
            watchback_date_set.append(current_date.strftime('%Y%m%d'))
            
            # 前进7天
            next_date = current_date + pd.Timedelta(days=7)
            
            # 如果超出结束日期，退出循环
            if next_date > end_date:
                break
                
            # 寻找下一个交易日
            current_date = next_date
            while current_date.strftime('%Y%m%d') not in trade_dates_set:
                current_date += pd.Timedelta(days=1)
                if current_date > end_date:
                    break
        
        return watchback_date_set
        
    except Exception as e:
        print(f"获取周度交易日期出错: {str(e)}")
        return None

def prepare_stocks(watch_date):
    # 1. 先按月份筛选股票
    qualified_stocks = pd.read_csv("数据/合格中证500成分股.csv")
    target_month = pd.to_datetime(watch_date).strftime('%Y%m')
    qualified_stocks['month'] = qualified_stocks['month'].astype(str)
    qualified_stocks = qualified_stocks[qualified_stocks['month'] == target_month]
    qualified_stocks = qualified_stocks['ts_code'].to_list()
    
    try:
        # 2. 读取交易日历和停牌信息
        trade_dates = pd.read_csv("数据/沪深交易所交易日历.csv")
        suspend_info = pd.read_csv("数据/中证500成分股单日停牌信息.csv")
        
        # 3. 转换日期格式
        trade_dates['trade_date'] = pd.to_datetime(trade_dates['cal_date'], format='%Y%m%d')
        watch_date = pd.to_datetime(watch_date, format='%Y%m%d')
        
        # 4. 获取最近20个交易日
        mask = trade_dates['trade_date'] <= watch_date
        last_20_dates = sorted(trade_dates[mask]['cal_date'])[-20:]
        
        # 5. 筛选有效股票
        valid_stocks = []
        for stock in qualified_stocks:
            # 检查该股票在最近20个交易日是否有停牌
            is_suspended = False
            for date in last_20_dates:
                if ((suspend_info['ts_code'] == stock) & 
                    (suspend_info['trade_date'].astype(str) == str(date))).any():
                    is_suspended = True
                    break
            
            # 如果20个交易日都没有停牌，则保留该股票
            if not is_suspended:
                valid_stocks.append(stock)

        
        return valid_stocks
        
    except Exception as e:
        print(f"筛选股票池时出错: {str(e)}")
        return []

def prepare_factors(start,end):
    factors=['APM_RAW',
            'APM_NEW',
            'APM_1',
            'APM_2',
            'APM_3']
    factor_result=dict()
    watch_date_set=prepare_watch_date_list(start,end)


    for watch_date in tqdm(watch_date_set,desc='因子计算中'):
        stocks_set=prepare_stocks(watch_date)
        apm=APM(securities=stocks_set,
                benchmark='600008.SH',
                watch_back_date=watch_date)
        apm.get_daily_data()
        apm.get_benchmark_data()
        apm.get_30min_data()
        apm.get_benchmark_30min_data()
        apm.get_30min_close_data()
        apm.get_30min_open_data()
        apm.get_daily_profit_percent()
        apm.get_overnight_ret()
        factor_df=pd.concat((apm.calc_factor(factor) for factor in factors),axis=1)
        factor_df.columns=factors
        factor_result[watch_date]=factor_df

    final_result=pd.concat(factor_result,names=['watch_date','stock_code'])
    return final_result

if __name__ == "__main__":

    final_result=prepare_factors(start='20200101',end='20231231')
    final_result.to_csv('因子结果.csv')
    print("因子计算完成，保存为因子结果.csv")

    