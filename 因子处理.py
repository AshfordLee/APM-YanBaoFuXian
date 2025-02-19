import pandas as pd
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import stats

def get_next_returns(factor_df,
                     last_date=None):
    #注意这个"当期"和"下期"的定义是从因子计算的日期开始的，由于
    #因子并不是每天计算(例如在这里是一周算一次)所以这个下期可能是当期过了好几个交易日的时间
    daily_data=pd.read_csv('数据/日线行情.csv')
    
    if last_date:
        days=factor_df.index.get_level_values('watch_date').unique().to_list()+[pd.to_datetime(last_date)]


    else:
        days=factor_df.index.get_level_values('watch_date').unique().to_list()


    result=dict()

    for now_day, next_day in zip(days[:-1],days[1:]):
    #遍历计算今天和明天的股票下期收益率
        stocks_list=factor_df.loc[now_day].index.get_level_values('stock_code').unique().to_list()
        
        # 使用 copy() 创建数据的副本
        now_days_data = daily_data[
            (daily_data['trade_date'] == now_day) & 
            (daily_data['ts_code'].isin(stocks_list))
        ].copy()  # 添加 copy()
        
        next_days_data = daily_data[
            (daily_data['trade_date'] == next_day) & 
            (daily_data['ts_code'].isin(stocks_list))
        ].copy()  # 添加 copy()
        
        #找到两期共有的股票
        common_stocks = set(now_days_data['ts_code']) & set(next_days_data['ts_code'])
        
        # 只保留这些股票的数据并排序
        now_days_data = now_days_data[now_days_data['ts_code'].isin(common_stocks)].sort_values(by='ts_code')
        next_days_data = next_days_data[next_days_data['ts_code'].isin(common_stocks)].sort_values(by='ts_code')
        


        now_days_data=now_days_data.set_index('ts_code')['close']
        next_days_data=next_days_data.set_index('ts_code')['close']
        result[now_day]=next_days_data/now_days_data-1
        # print(result[now_day])
        
    final_result=pd.concat(result).to_frame('next_ret')
    final_result.index.names=['watch_date','stock_code']

    return final_result
    # return now_days_data

def factor_cut(factor,number):
    #把因子列按分位数切开
    factor_data=factor.to_frame('factor')
    factor_quantile=pd.qcut(factor_data['factor'],
                            number,
                            labels=range(1,number+1))

    return factor_quantile

def factor_concat(factor_list):
    df=pd.concat(factor_list,axis=1)
    df.columns=['factor','next_return','factor_quantile']
    df.index.names=['watch_date','stock_code']
    return df

def build_factor_data(factor_data,
                      next_return,
                      quantile):
    
    final_result=dict()
    for factor_category in factor_data.columns:
        result=factor_concat(
            factor_list=(factor_data[factor_category],
                         next_return,
                         factor_cut(factor_data[factor_category],quantile)
            )
        )
        final_result[factor_category]=result

    return final_result

def get_factor_Nquantile_return(factor_data):
    return pd.pivot_table(factor_data.reset_index(),
                          index='watch_date',
                          columns='factor_quantile',
                          values='next_return',
                          observed=True

                          )

def plot_periods_return(returns,
                        benchmark,
                        **kwargs):
    return_dict=dict()
    for year,return_series in returns.groupby(pd.Grouper(level='watch_date',freq='Y')):
        return_dict(year.year)=return_series

    size=len(return_series)
    col_num=2
    row_num=math.ceil(size/col_num)
    

    mpl.rcParams['font.family']='serif'
    fig,axes=plt.subplots(row_num,
                          col_num,
                          figsize=(8*col_num,4*row_num))
    
    flattened_axes=[]
    for k in axes:
        for i in k:
            flattened_axes.append(i)
    
    for ax,(year,return_series) in zip(flattened_axes,return_dict.items()):

        return_series_index=return_series.index
        slice_benchmark=benchmark.loc[return_series_index]
        benchmark_nav=(slice_benchmark['close']/slice_benchmark['close'].iloc[0])

        strategy_nav=(1+return_series).cumprod()

        ax.set_title(f'{year}的策略净值')
        ax.set_ylabel('净值')
        benchmark_nav.plot(
            ax=ax,
            label='沪深300',
            color='black',
            linestyle='--',
            linestyle='--'
        )
        strategy_nav.plot(
            ax=ax,
            label='策略',
            color='blue',
            linestyle='-',
            **kwargs
        )
        ax.legend(loc='upper left')

    plt.subplot_adjust(hspace=0.3)
    return axes


def get_IC(ic_data:pd.DataFrame):
    
    ic=pd.DataFrame()
    ic['IC_Mean']=ic_data.mean()
    ic['IC_Std']=ic_data.std()
    ic['Risk_Adjusted_IC']=ic_data.mean()/ic_data.std()
    t_stat,p_value=stats.ttest_1samp(ic_data,0)
    ic['t_stat']=t_stat
    ic['p_value']=p_value
    ic['ic_skew']=stats.skew(ic_data)
    ic['ic_kurtosis']=stats.kurtosis(ic_data)

    return ic
    
    
if __name__ == "__main__":
    factor_df=pd.read_csv('因子结果.csv',index_col=['watch_date','stock_code'])
    # print(get_next_returns(factor_df))
    # print(factor_cut(factor_df['APM_RAW'],10))
    next_return=get_next_returns(factor_df)
    print(next_return)
    factor_data=build_factor_data(factor_df,next_return,10)
    # print(get_factor_Nquantile_return(factor_data['APM_RAW']))
    # get_factor_Nquantile_return(factor_data['APM_RAW']).to_csv('因子分位数收益率.csv')