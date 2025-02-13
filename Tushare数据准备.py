# 导入tushare
import tushare as ts
import os
import time
from datetime import datetime, timedelta
import pandas as pd
# 初始化pro接口
pro = ts.pro_api('31b92128e3ab8eb41a12d9a5fac8cb9bfacaee29fb591eba91d9c675')
#构造因子的时间段为2014-01-01到2020-12-31

class getdata():


    @staticmethod
    def get_stock_basic():
        #拉取股票列表
        # 拉取数据
        df = pro.stock_basic(**{
            "ts_code": "",
            "name": "",
            "exchange": "",
            "market": "",
            "is_hs": "",
            "list_status": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "cnspell",
            "market",
            "list_date",
            "fullname",
            "enname",
            "exchange",
            "curr_type",
            "list_status"
        ])

        # if not os.path.exists("数据"):
        #     os.makedirs("数据")

        df.to_csv("数据/股票列表.csv", index=False)

    @staticmethod
    def get_daily_data(trade_date):
        #获取日线行情
        df = pro.daily(**{
            "ts_code": "",
            "trade_date": trade_date,
            "start_date":"",
            "end_date":"",
            "offset": "",
            "limit": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount"
        ])

        # if not os.path.exists("数据"):
        #     os.makedirs("数据")

        # 追加模式写入CSV
        df.to_csv("数据/日线行情.csv", mode='a', index=False, header=not os.path.exists("数据/日线行情.csv"))

    @staticmethod
    def get_index_basic():
        #获取指数列表
        df=pro.index_basic()
        try:
            df.to_csv("数据/指数列表.csv", index=False)
        except Exception as e:
            print(f"获取指数列表时出错: {str(e)}")

    @staticmethod
    def get_zhongzheng500_stocks(trade_date)->pd.DataFrame:
        #获取中证500的成分股
        df = pro.index_weight(**{
            "index_code": "000905.SH",
            "trade_date": trade_date,
            "start_date": "",
            "end_date": "",
            "ts_code": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "index_code",
            "con_code",
            "trade_date",
            "weight"
        ])
        #注意在这个操作上每个月末的成分股和权重是会随着日期变化而变化的，
        #所以这个表以每月末的追加模式写入

        df.to_csv("数据/中证500成分股.csv", index=False, mode='a', header=not os.path.exists("数据/中证500成分股.csv"))
        return df
    
    @staticmethod
    def get_zhongzheng_paused_information(trade_date):
        #获取中证500成分股的单日停牌信息
        try:
            zhongzheng500_stocks = pd.read_csv("数据/中证500成分股.csv")
            
        except Exception:
            print("没找到中证500成分股数据,请一次性补充所有的数据")

        trade_date_month = trade_date[:6]  # 取前6位作为月份 (例如: 20140131 -> 201401)
        zhongzheng500_stocks['month'] = zhongzheng500_stocks['trade_date'].astype(str).str[:6]
            
        # 获取该月的成分股
        zhongzheng500_stocks = zhongzheng500_stocks[zhongzheng500_stocks['month'] == trade_date_month]
        if zhongzheng500_stocks.empty:
            print(f"未找到 {trade_date_month} 月的中证500成分股数据")
            return

        # 获取成分股代码列表并转换为字符串，用逗号分隔
        zhongzheng500_stocks = ','.join(zhongzheng500_stocks["con_code"].tolist())

        df = pro.suspend_d(**{
            "ts_code": zhongzheng500_stocks,
            "suspend_type": "S",
            "trade_date": trade_date,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "suspend_timing",
            "suspend_type"
        ])
        df.to_csv("数据/中证500成分股单日停牌信息.csv", mode='a', index=False, header=not os.path.exists("数据/中证500成分股单日停牌信息.csv"))


    @staticmethod
    def get_paused_information(trade_date):
        df = pro.suspend_d(**{
            "ts_code": "",
            "suspend_type": "S",
            "trade_date": trade_date,
            "start_date": "",
            "end_date": "",
            "limit": "",
            "offset": ""
        }, fields=[
            "ts_code",
            "trade_date",
            "suspend_timing",
            "suspend_type"
        ])
        df.to_csv("数据/沪深股票单日停牌信息.csv", mode='a', index=False, header=not os.path.exists("数据/中证500成分股单日停牌信息.csv"))

    def get_trade_calendar():
        df = pro.trade_cal(**{
            "start_date": "",
            "end_date": "",
            "is_open": "1",
            "limit": "",})
        df.to_csv("数据/沪深交易所交易日历.csv", mode='a', index=False, header=not os.path.exists("数据/交易日历.csv"))
    
    @staticmethod
    def filter_zhongzheng500_paused_stocks(threshold,now_date):
        #过滤掉上市天数不足threshold的股票
        try:
            # 读取中证500成分股数据
            zhongzheng500_stocks = pd.read_csv("数据/中证500成分股.csv")
            # 获取唯一的股票代码
            unique_stocks = zhongzheng500_stocks['con_code'].unique()
            
            # 分批获取股票的上市日期
            batch_size = 900  # 设置每批次处理的股票数量
            stock_list_dates_all = []
            
            for i in range(0, len(unique_stocks), batch_size):
                batch_stocks = unique_stocks[i:i+batch_size]
                batch_data = pro.stock_basic(
                    ts_code=','.join(batch_stocks),
                    fields=['ts_code', 'list_date']
                )
                stock_list_dates_all.append(batch_data)
                time.sleep(0.1)  # 添加延时避免频繁调用
                
            # 合并所有批次的结果
            stock_list_dates = pd.concat(stock_list_dates_all, ignore_index=True)
            
            # 读取交易日历
            trade_calendar = pd.read_csv("数据/沪深交易所交易日历.csv")
            trade_calendar = trade_calendar[trade_calendar['is_open'] == 1]
            
            # 将now_date转换为datetime对象
            now_date = pd.to_datetime(now_date)
            
            # 计算每只股票的上市交易日数
            qualified_stocks = []
            for _, stock in stock_list_dates.iterrows():
                list_date = pd.to_datetime(stock['list_date'])
                # 获取上市日期到now_date之间的交易日数
                trading_days = trade_calendar[
                    (trade_calendar['cal_date'] >= int(list_date.strftime('%Y%m%d'))) & 
                    (trade_calendar['cal_date'] <= int(now_date.strftime('%Y%m%d')))
                ].shape[0]
                
                if trading_days >= threshold:
                    qualified_stocks.append(stock['ts_code'])
            
            # 创建结果DataFrame并保存
            result_df = pd.DataFrame({'ts_code': qualified_stocks})
            result_df.to_csv("数据/合格中证500成分股.csv", index=False)
            
            return qualified_stocks
            
        except Exception as e:
            print(f"过滤股票时出错: {str(e)}")
            return None

if __name__ == "__main__":
    print("请选择要获取的数据类型：")
    print("1. 获取股票列表")
    print("2. 获取日线行情")
    print("3. 获取指数列表")
    print("4. 获取中证500成分股")
    print("5. 获取中证500成分股停牌信息(自2014-01-01至2020-12-31)")
    print("6. 获取沪深股票单日停牌信息(自2014-01-01至2020-12-31)")
    print("7. 获取沪深交易所交易日历")
    print("8. 过滤掉上市天数不足threshold的股票")
    choice = input("请输入选项(1或2):")
    
    if choice == "1":
        print("正在获取股票列表...")
        getdata.get_stock_basic()
        print("股票列表已保存到'数据/股票列表.csv'")
        
    elif choice == "2":
        print("正在获取日线行情...")
        start_date = "20010101"
        end_date = "20250206"
        
        
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")
        
        while current_date <= end_datetime:
            date_str = current_date.strftime("%Y%m%d")
            print(f"正在获取 {date_str} 的日线数据...")
            try:
                getdata.get_daily_data(date_str)
                time.sleep(0.2)
            except Exception as e:
                print(f"获取 {date_str} 数据时出错: {str(e)}")
            
            current_date += timedelta(days=1)
            
        print("所有日线行情数据获取完成")
    elif choice == "3":
        getdata.get_index_basic()
        print("指数列表已保存到'数据/指数列表.csv'")

    elif choice=="4":
        start_date="20140101"
        end_date="20201231"
        
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")
        
        while current_date <= end_datetime:
            date_str = current_date.strftime("%Y%m%d")
            print(f"正在获取{date_str}的中证500成分股...")
            try:
                getdata.get_zhongzheng500_stocks(date_str)
                time.sleep(0.3)  # 这个接口一分钟最多访问200次
            except Exception as e:
                print(f"获取{date_str}数据时出错: {str(e)}")
            
            current_date += timedelta(days=1)
            
        print("中证500成分股已保存到'数据/中证500成分股.csv'")

    elif choice=="5":
        start_date="20140101"
        end_date="20201231"
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")
        
        # 读取交易日历
        try:
            trade_calendar = pd.read_csv("数据/沪深交易所交易日历.csv")
        except Exception:
            print("没有找到交易日历数据，请先获取交易日历")
            pass
            
        while current_date <= end_datetime:
            date_str = current_date.strftime("%Y%m%d")
            
            # 检查是否为交易日
            is_trade_day = trade_calendar[
                (trade_calendar['cal_date'] == int(date_str)) & 
                (trade_calendar['is_open'] == 1)
            ].shape[0] > 0
            
            if is_trade_day:
                print(f"正在获取{date_str}的中证500成分股停牌信息...")
                try:
                    getdata.get_zhongzheng_paused_information(date_str)
                    time.sleep(0.1)
                except Exception as e:
                    print(f"获取{date_str}数据时出错: {str(e)}")
            
            current_date += timedelta(days=1)
            
        print("所有中证500成分股停牌信息已保存到'数据/中证500成分股单日停牌信息.csv'")
            

    elif choice=="6":
        start_date="20140101"
        end_date="20201231"
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_datetime = datetime.strptime(end_date, "%Y%m%d")
        
        # 读取交易日历
        try:
            trade_calendar = pd.read_csv("数据/沪深交易所交易日历.csv")
        except Exception:
            print("没有找到交易日历数据，请先获取交易日历")
            pass
            
        while current_date <= end_datetime:
            date_str = current_date.strftime("%Y%m%d")
            
            # 检查是否为交易日
            is_trade_day = trade_calendar[
                (trade_calendar['cal_date'] == int(date_str)) & 
                (trade_calendar['is_open'] == 1)
            ].shape[0] > 0
            
            if is_trade_day:
                print(f"正在获取{date_str}的沪深股票停牌信息...")
                try:
                    getdata.get_paused_information(date_str)
                    time.sleep(0.1)
                except Exception as e:
                    print(f"获取{date_str}数据时出错: {str(e)}")
            
            current_date += timedelta(days=1)
            
        print("所有停牌信息已保存到'数据/沪深股票单日停牌信息.csv'")

    elif choice=="7":
        getdata.get_trade_calendar()
        print("交易日历已保存到'数据/交易日历.csv'")

    elif choice=="8":
        getdata.filter_zhongzheng500_paused_stocks(threshold=360,
                                                   now_date="20201231")
    else:
        print("输入无效,请输入1或2")

    