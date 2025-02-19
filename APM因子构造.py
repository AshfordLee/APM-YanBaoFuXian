import pandas as pd
import numpy as np
import statsmodels.api as sm
class APM():


#APM因子的构造是分时间的
#换言之，我们要选定两个时间段来构造这个APM因子
#然后我们计算这两个时间段的收益率，然后进行回归，得到残差
#最后我们计算这个残差的方差，然后进行标准化，得到APM因子



    times={
        '上午':('10:00:00','11:30:00'),
        '下午':('13:30:00','15:00:00'),
        'am1':('10:00:00','10:30:00'),
        'am2':('10:30:00','11:00:00'),
        'pm1':('13:30:00','14:00:00'),
        'pm2':('14:00:00','14:30:00')
    }
    

    def __init__(self,
                 securities:list,
                 benchmark:str,
                 watch_back_date:str,
                 max_window:int=20):
        
        self.securities=securities
        self.benchmark=benchmark
        self.watch_back_date=watch_back_date
        self.max_window=max_window

    def get_daily_data(self):
        try:
            # 读取所有日线数据
            file_path = "数据/日线行情.csv"
            # print(f"尝试读取日线数据: {file_path}")
            
            try:
                # 读取日线数据
                df = pd.read_csv(file_path)
                
                # 转换日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')  # 指定输入格式
                watch_date = pd.to_datetime(self.watch_back_date)
                
                # 筛选目标股票
                df = df[df['ts_code'].isin(self.securities)]
                
                # 筛选日期范围
                df = df[df['trade_date'] <= watch_date]
                
                # 对每只股票分别处理
                all_data = []
                for stock in self.securities:
                    stock_df = df[df['ts_code'] == stock].copy()
                    
                    if not stock_df.empty:
                        # 按日期排序
                        stock_df = stock_df.sort_values('trade_date', ascending=True)
                        
                        # 取最近的max_window天数据
                        stock_data = stock_df.tail(self.max_window).copy()  # 创建真实副本
                        
                        # 使用.loc安全地修改数据
                        stock_data.loc[:, 'trade_date'] = stock_data['trade_date'].dt.strftime('%Y%m%d')
                        
                        all_data.append(stock_data)
                    else:
                        print(f"未找到股票 {stock} 的日线数据")
                
                # 合并所有股票的数据
                if all_data:
                    result_df = pd.concat(all_data, ignore_index=True)
                    self.daily_data = result_df
                    

                    
                    return result_df
                else:
                    print("没有找到任何符合条件的数据")
                    return None
                
            except FileNotFoundError:
                print(f"未找到日线数据文件")
                return None
            except Exception as e:
                print(f"处理日线数据时出错: {str(e)}")
                return None
            
        except Exception as e:
            print(f"获取日线数据时出错: {str(e)}")
            return None

    def get_benchmark_data(self):
        try:
            # 读取基准指数日线数据
            file_path = "数据/日线行情.csv"
            # print(f"尝试读取基准指数日线数据: {file_path}")
            
            try:
                # 读取日线数据
                df = pd.read_csv(file_path)
                
                # 转换日期格式
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')  # 指定输入格式
                watch_date = pd.to_datetime(self.watch_back_date)
                
                # 筛选基准指数数据
                df = df[df['ts_code'] == self.benchmark]
                
                # 筛选日期范围
                df = df[df['trade_date'] <= watch_date]
                
                if not df.empty:
                    # 按日期排序
                    df = df.sort_values('trade_date', ascending=True)
                    
                    # 取最近的max_window天数据
                    benchmark_data = df.tail(self.max_window).copy()  # 创建真实副本
                    
                    # 使用.loc安全地修改数据
                    benchmark_data.loc[:, 'trade_date'] = benchmark_data['trade_date'].dt.strftime('%Y%m%d')
                    
                    self.benchmark_daily_data = benchmark_data
                    
                    # 合并到self.daily_data中
                    self.daily_data = pd.concat([self.daily_data, benchmark_data], ignore_index=True)
                    
                    return benchmark_data
                else:
                    print(f"未找到基准指数 {self.benchmark} 的日线数据")
                    return None
                
            except FileNotFoundError:
                print(f"未找到日线数据文件")
                return None
            except Exception as e:
                print(f"处理基准指数日线数据时出错: {str(e)}")
                return None
            
        except Exception as e:
            print(f"获取基准指数日线数据时出错: {str(e)}")
            return None

    def get_30min_data(self):
        #获取回溯20个交易日，每个交易日8个30分钟数据
        try:
            all_data = []
            
            for stock in self.securities:
                # 转换股票代码格式 (600004.SH -> sh.600004)
                code, market = stock.split('.')
                baostock_code = f"{market.lower()}.{code}"
                
                # 读取该股票的30分钟数据
                file_path = f"数据/30分钟线/{baostock_code}.csv"
                # print(f"尝试读取文件: {file_path}")
                
                try:
                    df = pd.read_csv(file_path)
                    
                    # 找到watch_back_date那天的位置
                    df['time'] = pd.to_datetime(df['time'].astype(str), format='%Y%m%d%H%M%S%f')
                    watch_date = pd.to_datetime(self.watch_back_date)
                    
                    # 找到最接近watch_back_date的位置
                    end_idx = df[df['time'].dt.date <= watch_date.date()].index[-1]
                    
                    # 回溯160行（20个交易日 × 8个30分钟数据）
                    start_idx = max(0, end_idx - 159)
                    
                    # 截取数据
                    stock_data = df.iloc[start_idx:end_idx + 1].copy()
                    stock_data['code'] = stock  # 保持原始股票代码格式
                    
                    all_data.append(stock_data)
                    
                except FileNotFoundError:
                    print(f"未找到股票 {baostock_code} 的数据文件")
                    continue
                except Exception as e:
                    print(f"处理股票 {baostock_code} 数据时出错: {str(e)}")
                    continue
                
            if all_data:
                result_df = pd.concat(all_data, ignore_index=True)
                self.data_30min=result_df
                return result_df
            else:
                print("没有找到任何符合条件的数据")
                return None
            
        except Exception as e:
            print(f"获取数据时出错: {str(e)}")
            return None
        
    def get_benchmark_30min_data(self):
        try:
            #获取基准指数的30分钟数据
            # 转换基准指数代码格式 (000905.SH -> sh.000905)
            code, market = self.benchmark.split('.')
            baostock_code = f"{market.lower()}.{code}"
            
            # 读取基准指数的30分钟数据
            file_path = f"数据/30分钟线/{baostock_code}.csv"
            # print(f"尝试读取基准指数30分钟数据: {file_path}")
            
            try:
                df = pd.read_csv(file_path)
                
                # 找到watch_back_date那天的位置
                df['time'] = pd.to_datetime(df['time'].astype(str), format='%Y%m%d%H%M%S%f')
                watch_date = pd.to_datetime(self.watch_back_date)
                
                # 找到最接近watch_back_date的位置
                end_idx = df[df['time'].dt.date <= watch_date.date()].index[-1]
                
                # 回溯160行（20个交易日 × 8个30分钟数据）
                start_idx = max(0, end_idx - 159)
                
                # 截取数据
                benchmark_data = df.iloc[start_idx:end_idx + 1].copy()
                benchmark_data['code'] = self.benchmark  # 保持原始代码格式
                
                self.benchmark_30min_data = benchmark_data
                
                # 合并到self.data_30min中
                self.data_30min = pd.concat([self.data_30min, benchmark_data], ignore_index=True)
                
                return benchmark_data
                
            except FileNotFoundError:
                print(f"未找到基准指数 {baostock_code} 的30分钟数据文件")
                return None
            except Exception as e:
                print(f"处理基准指数 {baostock_code} 30分钟数据时出错: {str(e)}")
                return None
            
        except Exception as e:
            print(f"获取基准指数30分钟数据时出错: {str(e)}")
            return None
          
    def get_30min_close_data(self):
        #获取30分钟线上的收盘价
        self.close_data_30min=pd.pivot_table(self.data_30min,
                                       index='time',
                                       columns='code',
                                       values='close')
        return self.close_data_30min
    
    def get_30min_open_data(self):
        self.open_data_30min=pd.pivot_table(self.data_30min,
                                      index='time',
                                      columns='code',
                                      values='open')
        return self.open_data_30min
        
    def get_daily_profit_percent(self):
        # 获取每日收益率
        # 1. 先将数据透视为每个股票一列的形式
        daily_pivot = pd.pivot_table(self.daily_data,
                                   index='trade_date',
                                   columns='ts_code',
                                   values='close')
        
        # 2. 确保日期索引格式正确
        # 先将索引转换为datetime，再格式化
        daily_pivot.index = pd.to_datetime(daily_pivot.index).strftime('%Y-%m-%d')
        
        # 3. 计算对数收益率
        daily_returns = np.log(daily_pivot / daily_pivot.shift(1))
        
        # 4. 去掉第一行（因为shift后第一行是NaN）并求和
        self.daily_returns = daily_returns.iloc[1:].sum()
        
        # 5. 去掉基准指数的收益率
        # self.daily_returns = self.daily_returns.drop(self.benchmark)

        #日度收益率
        return self.daily_returns

    def get_overnight_ret(self):
        # 获取隔日收益率
        daily_data = self.daily_data.copy()
        
        # 1. 进行透视
        daily_pivot = pd.pivot_table(daily_data,
                                   index='trade_date',
                                   columns='ts_code',
                                   values=['close', 'open'])
        
        # 删除包含NaN的行

        
        # 2. 计算隔日收益率：今日开盘价/昨日收盘价
        overnight_returns = (daily_pivot['open'] / 
                           daily_pivot['close'].shift(1))
        
        # 3. 去掉第一行（因为shift后第一行是NaN）
        overnight_returns = overnight_returns.iloc[1:]
        
        self.overnight_returns = overnight_returns
        
        return overnight_returns


    def get_logret(self,start:str,end:str):
        #获取30分钟线上的对数收益率
        open_df=self.open_data_30min.at_time(start)
        open_df.index=open_df.index.normalize()
        close_df=self.close_data_30min.at_time(end)
        close_df.index=close_df.index.normalize()
        logret=np.log(close_df/open_df)
        return logret

    @staticmethod
    def _rls(df):
        df=df.fillna(1)
        df=df.set_index('code')
        X=sm.add_constant(df['benchmark'])
        y=df['log_ret']
        mod=sm.OLS(y,X)
        res=mod.fit()
        
        return res.resid

    def regression(self,logret):
        x=logret[self.securities].unstack().reset_index(
            level=0).sort_index()
        x.columns=['code','log_ret']
        x['benchmark']=logret[self.benchmark]
        result=x.groupby(level=0).apply(APM._rls)
        return result
    
    def calc_resid(self,pos1,pos2):
        #计算残差
        if pos1=="隔夜":
            am=self.get_overnight_ret()

        else:
            am=self.get_logret(start=pos1[0],end=pos1[1])
        pm=self.get_logret(start=pos2[0],end=pos2[1])
        #传回两个pd.series构成的Tuple
        # print(am)
        # print(pm)
        result1=self.regression(am)
        result2=self.regression(pm)
        # print(result1)
        # print(result2)

        # if np.any(np.isnan(result1)) or np.any(np.isinf(result1)):
        #     print("警告：result1 包含 NaN 或 Inf 值")
        #     print("NaN 数量:", np.sum(np.isnan(result1)))
        #     print("Inf 数量:", np.sum(np.isinf(result1)))
            
        # if np.any(np.isnan(result2)) or np.any(np.isinf(result2)):
        #     print("警告：result2 包含 NaN 或 Inf 值")
        #     print("NaN 数量:", np.sum(np.isnan(result2)))
        #     print("Inf 数量:", np.sum(np.isinf(result2)))
        return result1,result2

    def calc_factor(self,interval):
        INTERVAL_DICT={'APM_RAW':(self.times['上午'],self.times['下午']),
                       'APM_NEW':('隔夜',self.times['下午']),
                       'APM_1':('隔夜',self.times['pm1']),
                       'APM_2':(self.times['am1'],self.times['pm1']),
                       'APM_3':(self.times['am2'],self.times['pm2'])}
        
        resid1,resid2=self.calc_resid(INTERVAL_DICT[interval][0],INTERVAL_DICT[interval][1])
        diff=resid1-resid2
        # 检查 diff 中的无效值
        # if np.any(np.isnan(diff)) or np.any(np.isinf(diff)):
        #     print("警告：diff 包含 NaN 或 Inf 值")
        #     print("NaN 数量:", np.sum(np.isnan(diff)))
        #     print("Inf 数量:", np.sum(np.isinf(diff)))
        diff_std_normal=diff.groupby(level='code').apply(
            lambda x:(x.mean()*np.sqrt(len(x)))/x.std()
        )

        diff_std_normal,self.daily_returns=diff_std_normal.align(self.daily_returns)

        X=self.daily_returns
        Y=diff_std_normal

        mod=sm.OLS(Y,X)
        res=mod.fit()
        return res.resid
    
if __name__ == "__main__":
    apm=APM(securities=['600004.SH','600006.SH','600426.SH'],
            benchmark='600008.SH',
            watch_back_date='2023-01-01')
    #网上没找到30分钟的指数数据，这里就随便
    #挑一个000671.SZ当指数用作基准数据

    apm.get_daily_data()
    apm.get_benchmark_data()
    apm.get_30min_data()
    apm.get_benchmark_30min_data()
    apm.get_30min_close_data()
    apm.get_30min_open_data()
    apm.get_daily_profit_percent()
    overnight_returns=apm.get_overnight_ret()
    # logret=apm.get_logret(start='10:00:00',end='10:30:00')
    # x=apm.regression(logret)
    x=apm.calc_factor('APM_1')
    print(x)

    # print(logret)
    # print(overnight_returns)
    # daily_data_pivot=apm.get_daily_profit_percent()
    # data=apm.get_30min_data()
    # close_data_30min=apm.get_30min_close_data()
    # print(daily_data)