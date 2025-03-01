import tushare as ts
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import baostock as bs
import akshare as ak


from Function_Files.Tushare数据准备 import getdata
from Function_Files.APM因子构造 import APM
from Function_Files.因子实现 import prepare_factors
import Function_Files.因子处理 as factor_process



if __name__=="__main__":

    print("请选择操作类型:")
    print("1. 获取数据(第一部分)")
    print("2. 获取数据(第二部分)")
    print("3. 计算因子")
    print("4. 处理因子与画图")



    choice=input("请输入选项(1或2或3):")

    if choice=="1":
        
        #第0步：创建数据文件夹
        data_dir = "数据"
        if not os.path.exists(data_dir):
            print(f"创建数据文件夹: {data_dir}")
            os.makedirs(data_dir)
        else:
            print(f"数据文件夹已存在: {data_dir}")
        ##########################################################
        #第一步：获取股票列表
        print("正在获取股票列表...")
        getdata.get_stock_basic()
        print("股票列表已保存到'数据/股票列表.csv'")
        ##########################################################

        ##########################################################
        #第二步：获取日线行情
        print("正在获取日线行情...")
        start_date = "20190101"
        end_date = "20250214"
        
        
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
        ##########################################################

        ##########################################################
        #第三步：获取指数列表
        print("正在获取指数列表...")
        getdata.get_index_basic()
        print("指数列表已保存到'数据/指数列表.csv'")
        ##########################################################

        ##########################################################
        #第四步：获取中证500成分股
        print("正在获取中证500成分股...")
        start_date="20190101"
        end_date="20231231"
        
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
        ##########################################################

        ##########################################################
        #第五步：获取交易日历
        getdata.get_trade_calendar()
        print("交易日历已保存到'数据/交易日历.csv'")

        ##########################################################      
        #第六步：获取中证500成分股停牌信息
        print("正在获取中证500成分股停牌信息...")
        start_date="20190101"
        end_date="20231231"
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
        ##########################################################

        ##########################################################
        #第七步：获取沪深股票单日停牌信息
        print("正在获取沪深股票单日停牌信息...")
        
        start_date="20190101"
        end_date="20231231"
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
        ##########################################################

        ##########################################################
        #第八步：筛选出合格的中证500成分股
        print("正在筛选出合格的中证500成分股...")
        getdata.filter_zhongzheng500_paused_stocks(threshold=360)
        print("合格中证500成分股已保存到'数据/合格中证500成分股.csv'")
        ##########################################################

    elif choice=="2":
        ##########################################################
        #第九步：获取个股30分钟线数据
        print("正在获取个股30分钟线数据...")
        getdata.get_30min_data()
        print("所有股票的30分钟数据已保存到'数据/30分钟线'目录下")
        ##########################################################

    
    elif choice=="3":
        ##########################################################
        #第十步：计算因子
        print("正在计算因子...")
        # apm=APM(securities=['600004.SH','600006.SH','600426.SH'],
        #         benchmark='600008.SH',
        #         watch_back_date='2023-01-01')
        
        final_result=prepare_factors(start='20200101',end='20231231')
        final_result.to_csv('因子结果.csv')
        print("因子计算完成，保存为因子结果.csv")


    elif choice=="4":
        ##########################################################
        #第十一步：处理因子与画图
        print("正在处理因子与画图...")
        try:
            factor_df=pd.read_csv('因子结果.csv',index_col=['watch_date','stock_code'],parse_dates=['watch_date'])
        except:
            print("没有找到因子结果.csv文件，请先计算因子")


        try:
            next_return=pd.read_csv('因子下期收益率.csv',index_col=['watch_date','stock_code'],parse_dates=['watch_date'])
        except:
            next_return=factor_process.get_next_returns(factor_df)
            next_return.to_csv('因子下期收益率.csv')

        factor_dic=factor_process.build_factor_data(factor_data=factor_df,
                                                    next_return=next_return,
                                                    quantile=5)
        
        APM_group_return_dict=dict()

        for factor_name,factor_data in factor_dic.items():
            APM_group_return_dict[factor_name]=factor_process.get_factor_Nquantile_return(factor_data)
        
        benchmark_data=factor_process.get_all_benchmark_data()

        factor_process.plot_factor_returns(factor_df=factor_df,
                                          APM_group_return_dict=APM_group_return_dict,
                                          factor_dic=factor_dic)
        

        factor_process.plot_long_short_profit(APM_group_return_dict)

        factor_process.plot_periods_return(returns=APM_group_return_dict['APM_RAW'],
                                          benchmark=benchmark_data,
                                          factor_name='APM_RAW')

        factor_process.plot_periods_return(returns=APM_group_return_dict['APM_NEW'],
                                          benchmark=benchmark_data,
                                          factor_name='APM_NEW')
        
        

        factor_process.plot_periods_return(returns=APM_group_return_dict['APM_1'],
                                          benchmark=benchmark_data,
                                          factor_name='APM_1')
        
        
        
        factor_process.plot_periods_return(returns=APM_group_return_dict['APM_2'],
                                          benchmark=benchmark_data,
                                          factor_name='APM_2')
        
        
        
        factor_process.plot_periods_return(returns=APM_group_return_dict['APM_3'],
                                          benchmark=benchmark_data,
                                          factor_name='APM_3')
        
        
        
        top_ic_dic,bottom_ic_dic=factor_process.calculate_quantile_ic(factor_dic)   

        top_ic_table=pd.concat([
            factor_process.get_IC(pd.DataFrame({factor_name:series}))
            for factor_name,series in top_ic_dic.items()
        ],axis=0)

        bottom_ic_table=pd.concat([
            factor_process.get_IC(pd.DataFrame({factor_name:series}))
            for factor_name,series in bottom_ic_dic.items()
        ],axis=0)

        factor_process.plot_ic_metrics(top_ic_table,bottom_ic_table,"Factor IC Metrics")

