import pandas as pd
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import stats
from matplotlib.gridspec import GridSpec
import os


def get_next_returns(factor_df,
                     last_date=None):
    #注意这个"当期"和"下期"的定义是从因子计算的日期开始的，由于
    #因子并不是每天计算(例如在这里是一周算一次)所以这个下期可能是当期过了好几个交易日的时间
    daily_data=pd.read_csv('数据/日线行情.csv')

    daily_data['trade_date'] = daily_data['trade_date'].astype(str)

    if last_date:
        days=factor_df.index.get_level_values('watch_date').unique().to_list()+[pd.to_datetime(last_date)]


    else:
        days=factor_df.index.get_level_values('watch_date').unique().to_list()




    days = [day.strftime('%Y%m%d') for day in days]

    result=dict()

    for now_day, next_day in zip(days[:-1],days[1:]):
    #遍历计算今天和明天的股票下期收益率
        stocks_list=factor_df.loc[now_day].index.get_level_values('stock_code').unique().to_list()

        #使用 copy() 创建数据的副本
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
    # 获取 next_return 中的所有股票代码和日期
    available_stocks = next_return.index.get_level_values('stock_code').unique()
    available_dates = next_return.index.get_level_values('watch_date').unique()
    
    # 筛选 factor_data，只保留这些股票和日期的数据
    factor_data = factor_data[
        (factor_data.index.get_level_values('stock_code').isin(available_stocks)) &
        (factor_data.index.get_level_values('watch_date').isin(available_dates))
    ]
    
    final_result = dict()
    for factor_category in factor_data.columns:
        result = factor_concat(
            factor_list=(
                factor_data[factor_category],
                next_return,
                factor_cut(factor_data[factor_category], quantile)
            )
        )
        
        # 去掉含有NaN的行（最后一天的数据）
        result = result.dropna()
        

        final_result[factor_category] = result

    return final_result

def get_factor_Nquantile_return(factor_data):



    result=pd.pivot_table(factor_data.reset_index(),
                          index='watch_date',
                          columns='factor_quantile',
                          values='next_return',
                          observed=True)
                          
    return result

def plot_periods_return(returns,
                        benchmark,
                        **kwargs):
    return_dict=dict()
    for year,return_series in returns.groupby(pd.Grouper(level='watch_date',freq='Y')):
        return_dict[year.year]=return_series

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

def get_beta(Matrix,Vector):
    res=stats.linregress(Matrix,Vector)
    return res.rvalue


def factor_ttest(factor_data):
    beta=factor_data.groupby(level='watch_date').apply(lambda x:get_beta(x['factor'],x['next_return']))
    
    result=beta.rolling(window=5).apply(lambda x:stats.ttest_1samp(x,0)[0],raw=True)

    return result

def get_ttest_table(t_value:pd.DataFrame)->pd.DataFrame:
    
    t_value_table = pd.Series()
    t_value_table['Abs T Mean'] = t_value.abs().mean()
    t_value_table['T Mean'] = t_value.mean()
    t_value_table['T > 2'] = len(t_value[t_value.abs() > 2]) / len(t_value)
    
    return t_value_table.to_frame('回归法检验')

def mean_return_by_quantile(factor_data):
    """
    计算各分位数的平均收益率
    
    参数：
    factor_data: DataFrame，包含 'factor_quantile' 和 'next_ret' 列
    
    返回：
    mean_ret: Series，各分位数的平均收益率
    """
    # 按分位数分组并计算平均收益
    mean_ret = factor_data.groupby('factor_quantile',observed=True)['next_return'].mean()
    
    return mean_ret, None  # 保持与原函数相同的返回格式

def plot_factor_returns(factor_df, APM_group_return_dict, factor_dic):
    # 方案1：使用微软雅黑字体
    # plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 设置中文字体
    # plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
    
    # 或者方案2：使用SimHei字体
    # plt.rcParams['font.sans-serif'] = ['SimHei']
    # plt.rcParams['axes.unicode_minus'] = False
    
    rows = factor_df.shape[1]
    color_map = ['#5D91A7', '#00516C', '#6BCFF6', '#00A4DC', '#6DBBBF',
                '#008982']

    fig = plt.figure(figsize=(18, 5*rows))
    gs = GridSpec(rows, 4, figure=fig)

    for i, (factor_name, factor_data) in enumerate(APM_group_return_dict.items()):
        ax1 = plt.subplot(gs[i, :1])
        ax2 = plt.subplot(gs[i, 1:])

        mean_ret, _ = mean_return_by_quantile(factor_dic[factor_name])
        mean_ret = mean_ret * 10000

        ax1.set_ylabel('Average Grouped Profit Rate')
        mean_ret.plot.bar(ax=ax1, title='Average Profit Rate After Grouped')

        cumu_ret = (1 + factor_data).cumprod()
        cumu_ret.plot(
            ax=ax2,
            title=f'{factor_name.upper()} accumulation profit',
            color=color_map
        )
        ax2.set_ylabel('Accumulated Profit')  # 改为中文
        ax2.yaxis.set_major_formatter(
            mpl.ticker.FuncFormatter(lambda x, pos: '%.2f%%' % (x * 100))
        )

        plt.subplots_adjust(hspace=0.4, wspace=0.35)
    
    plt.show()

if __name__ == "__main__":
    factor_df=pd.read_csv('因子结果.csv',index_col=['watch_date','stock_code'],parse_dates=['watch_date'])

    # print(factor_df)
    try:
        next_return=pd.read_csv('因子下期收益率.csv',index_col=['watch_date','stock_code'],parse_dates=['watch_date'])
    except:
        next_return=get_next_returns(factor_df)
        next_return.to_csv('因子下期收益率.csv')
    # next_return=get_next_returns(factor_df)
    # next_return.to_csv('因子下期收益率.csv')
    # print(next_return)

    factor_dic=build_factor_data(factor_df,next_return,5)



    APM_group_return_dict=dict()
    
    for factor_name,factor_data in factor_dic.items():

        APM_group_return_dict[factor_name]=get_factor_Nquantile_return(factor_data)
        # print(APM_group_return_dict[factor_name])



    # 使用函数
    plot_factor_returns(factor_df, APM_group_return_dict, factor_dic)