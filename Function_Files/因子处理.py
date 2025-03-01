import pandas as pd
import math
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy import stats
from matplotlib.gridspec import GridSpec
import os
# 如果APM因子构造.py在同一目录下，直接导入
# from APM因子构造 import APM
from scipy.stats import spearmanr
import numpy as np



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

def plot_periods_return(returns, benchmark, factor_name):
    """
    绘制各期收益率图表(净值)
    
    参数：
    returns: DataFrame, 因子收益率数据
    benchmark: DataFrame, 基准数据（600008.SH的收盘价）
    factor_name: str, 因子名称
    """
    # 确保索引是datetime类型
    if not isinstance(returns.index, pd.DatetimeIndex):
        returns.index = pd.to_datetime(returns.index)
    if not isinstance(benchmark.index, pd.DatetimeIndex):
        benchmark.index = pd.to_datetime(benchmark.index)
    
    # 计算基准收益率
    benchmark_returns = benchmark['close'].pct_change()
    
    # 按年份分组
    yearly_groups = returns.groupby(pd.Grouper(freq='YE'))
    n_years = len([x for x in yearly_groups])
    
    # 创建图表
    fig, axes = plt.subplots(n_years, 1, figsize=(15, 6*n_years))
    if n_years == 1:
        axes = [axes]
    
    # 为每一年创建子图
    for (year, return_series), ax in zip(yearly_groups, axes):
        # 获取对应时期的基准收益
        slice_benchmark = benchmark_returns.loc[return_series.index]
        
        # 计算净值（从1开始）
        cum_returns = (1 + return_series).cumprod()
        cum_benchmark = (1 + slice_benchmark).cumprod()
        
        # 将起始值设为1
        cum_returns = cum_returns / cum_returns.iloc[0] if not cum_returns.empty else cum_returns
        cum_benchmark = cum_benchmark / cum_benchmark.iloc[0] if not cum_benchmark.empty else cum_benchmark
        
        # 绘制净值曲线
        cum_returns.plot(ax=ax, label='Factor Returns')
        cum_benchmark.plot(ax=ax, label='Benchmark', color='black', linestyle='--')
        
        # 设置标题和标签
        ax.set_title(f'{factor_name} Net Value in {year.year}')
        ax.set_ylabel('Net Value')
        ax.legend()
        ax.grid(True)
        
        # 格式化y轴为小数
        ax.yaxis.set_major_formatter(
            mpl.ticker.FuncFormatter(lambda x, pos: '%.2f' % x)
        )
    
    plt.subplots_adjust(hspace=0.3)
    plt.show()

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


def plot_long_short_profit(APM_group_return_dict):

    color_map = ['#5D91A7', '#00516C', '#6BCFF6', '#00A4DC', '#6DBBBF',
             '#008982']  # 设置颜色

    fig,ax=plt.subplots(figsize=(18,5))

    excess_df = pd.concat(
        ((1 + (df[5] - df[1])).cumprod() for df in APM_group_return_dict.values()),
        axis=1
    )

    excess_df.columns=list(APM_group_return_dict.keys())

    excess_df.plot(
        ax=ax,
        title='Long-Short Hedging Profit',
        color=color_map
    )

    ax.set_ylabel('Cumulative Return')
    ax.yaxis.set_major_formatter(  # 将y轴格式化为百分比
        mpl.ticker.FuncFormatter(lambda x, pos: '%.2f%%' % (x * 100))
    )

    plt.show()

def get_all_benchmark_data():
    """
    获取600008.SH的基准数据
    时间范围：2019-12-31 到 2023-12-31
    返回：包含日期和收盘价的DataFrame
    """
    # 读取日线数据
    daily_data = pd.read_csv('数据/日线行情.csv')
    
    # 转换日期为datetime
    daily_data['trade_date'] = pd.to_datetime(daily_data['trade_date'], format='%Y%m%d')
    
    # 设置日期范围
    start_date = pd.to_datetime('20191231')
    end_date = pd.to_datetime('20231231')
    
    # 筛选数据
    benchmark_data = daily_data[
        (daily_data['ts_code'] == '600008.SH') & 
        (daily_data['trade_date'] >= start_date) & 
        (daily_data['trade_date'] <= end_date)
    ].copy()
    
    # 设置索引并排序
    benchmark_data = benchmark_data.set_index('trade_date').sort_index()
    
    # 只保留需要的列
    # benchmark_data = benchmark_data[['close']]
    
    print(f"获取到的数据范围: {benchmark_data.index.min()} 到 {benchmark_data.index.max()}")
    print(f"总交易日数: {len(benchmark_data)}")
    
    return benchmark_data

def calculate_quantile_ic(factor_dic):
    """
    计算因子头部(第5分位数)和尾部(第1分位数)的信息系数(IC)
    
    参数:
    factor_dic: dict, 包含因子数据的字典，每个值是包含'factor', 'next_return', 'factor_quantile'列的DataFrame
    
    返回:
    tuple: (top_ic_dic, bottom_ic_dic) 两个字典，包含每个因子头部和尾部的IC值
    """
    # 计算头部分组(第5分位)的IC
    top_ic_dic = {}
    # 计算底部分组(第1分位)的IC
    bottom_ic_dic = {}
    
    # 创建子图
    n_factors = len(factor_dic)
    fig, axes = plt.subplots(n_factors, 1, figsize=(15, 5 * n_factors))
    
    # 如果只有一个因子，确保axes是列表
    if n_factors == 1:
        axes = [axes]
    
    for i, (factor_name, factor_data) in enumerate(factor_dic.items()):
        print(f"处理因子: {factor_name}")
        
        # 筛选头部数据(第5分位)
        top_data = factor_data.query('factor_quantile == 5').copy()
        # 筛选底部数据(第1分位)
        bottom_data = factor_data.query('factor_quantile == 1').copy()
        
        # 计算头部IC
        top_ic = calculate_ic_by_date(top_data)
        # 计算底部IC
        bottom_ic = calculate_ic_by_date(bottom_data)
        
        # 存储结果
        top_ic_dic[factor_name] = top_ic
        bottom_ic_dic[factor_name] = bottom_ic
        
        # 在对应的子图上绘制
        ax = axes[i]
        top_ic.rolling(20).mean().plot(ax=ax, label=f"Top Quantile", color='blue')
        bottom_ic.rolling(20).mean().plot(ax=ax, label=f"Bottom Quantile", color='red')
        
        # 设置子图标题和标签
        ax.set_title(f"{factor_name} IC (20-day MA)")
        ax.set_ylabel("IC Value")
        ax.legend()
        ax.grid(True)
        
        # 打印统计信息
        print(f"  头部IC均值: {top_ic.mean():.4f}, 中位数: {top_ic.median():.4f}")
        print(f"  底部IC均值: {bottom_ic.mean():.4f}, 中位数: {bottom_ic.median():.4f}")
    
    # 调整子图间距
    plt.tight_layout()
    # 显示图表
    plt.show()
    
    return top_ic_dic, bottom_ic_dic

def calculate_ic_by_date(factor_data):
    """
    按日期计算因子的信息系数(IC)
    
    参数:
    factor_data: DataFrame, 包含'factor'和'next_return'列的数据
    
    返回:
    Series: 每个日期的IC值
    """
    from scipy.stats import spearmanr
    
    # 按日期分组
    grouped = factor_data.groupby(level='watch_date')
    
    # 计算每个日期的IC
    ic_values = []
    ic_dates = []
    
    for date, group in grouped:
        # 确保组内有足够的数据点
        if len(group) > 5:  # 至少需要5个数据点才能计算有意义的相关性
            # 计算Spearman等级相关系数
            ic = spearmanr(group['factor'], group['next_return'])[0]
            # 如果结果是NaN，用0替代
            if np.isnan(ic):
                ic = 0
            ic_values.append(ic)
            ic_dates.append(date)
    
    # 创建Series
    ic_series = pd.Series(ic_values, index=ic_dates)
    
    return ic_series

def plot_ic_metrics(top_ic_table, bottom_ic_table=None, title_prefix="Factor"):
    """
    为IC指标创建柱状图
    
    参数:
    top_ic_table: DataFrame, 包含因子IC指标的表格
    bottom_ic_table: DataFrame, 可选，包含底部分位数的IC指标
    title_prefix: str, 图表标题前缀
    """
    # 获取指标和因子名称
    metrics = top_ic_table.columns
    factors = top_ic_table.index
    
    # 设置图表
    n_metrics = len(metrics)
    fig, axes = plt.subplots(n_metrics, 1, figsize=(12, 5 * n_metrics))
    
    # 确保axes是列表
    if n_metrics == 1:
        axes = [axes]
    
    # 为每个指标创建柱状图
    for i, metric in enumerate(metrics):
        ax = axes[i]
        
        # 绘制顶部分位数的柱状图
        x = np.arange(len(factors))
        width = 0.35
        
        # 顶部分位数柱状图
        bars1 = ax.bar(x - width/2, top_ic_table[metric], width, label='Top Quantile')
        
        # 如果有底部分位数数据，也绘制
        if bottom_ic_table is not None:
            bars2 = ax.bar(x + width/2, bottom_ic_table[metric], width, label='Bottom Quantile')
        
        # 设置图表标题和标签
        ax.set_title(f'{title_prefix} {metric}')
        ax.set_ylabel(metric)
        ax.set_xticks(x)
        ax.set_xticklabels(factors, rotation=45)
        ax.legend()
        
        # 添加数值标签
        def add_labels(bars):
            for bar in bars:
                height = bar.get_height()
                # 根据值的正负决定标签位置
                if height < 0:
                    va = 'top'
                    offset = -3
                else:
                    va = 'bottom'
                    offset = 3
                ax.annotate(f'{height:.4f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, offset),  # 3点偏移
                            textcoords="offset points",
                            ha='center', va=va,
                            fontsize=8)
        
        add_labels(bars1)
        if bottom_ic_table is not None:
            add_labels(bars2)
        
        # 添加水平线表示零点
        ax.axhline(y=0, color='gray', linestyle='-', alpha=0.3)
        
        # 添加网格
        ax.grid(True, linestyle='--', alpha=0.7)
    
    # 调整子图间距
    plt.tight_layout()
    plt.show()

    
    
if __name__ == "__main__":
    # apm=APM(securities=['600004.SH','600006.SH','600426.SH'],
    #     benchmark='600008.SH',
    #     watch_back_date='2023-01-01')


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

    benchmark_data=get_all_benchmark_data()



    # 使用函数
    plot_factor_returns(factor_df, APM_group_return_dict, factor_dic)

    plot_long_short_profit(APM_group_return_dict)




    # 调用函数时传入因子名称
    plot_periods_return(returns=APM_group_return_dict['APM_RAW'], benchmark=benchmark_data, factor_name='APM_RAW')
    plot_periods_return(returns=APM_group_return_dict['APM_NEW'], benchmark=benchmark_data, factor_name='APM_NEW')
    plot_periods_return(returns=APM_group_return_dict['APM_1'], benchmark=benchmark_data, factor_name='APM_1')
    plot_periods_return(returns=APM_group_return_dict['APM_2'], benchmark=benchmark_data, factor_name='APM_2')
    plot_periods_return(returns=APM_group_return_dict['APM_3'], benchmark=benchmark_data, factor_name='APM_3')
    
    top_ic_dic, bottom_ic_dic = calculate_quantile_ic(factor_dic)

    print(top_ic_dic)
    print(bottom_ic_dic)

    top_ic_table = pd.concat([
        get_IC(pd.DataFrame({factor_name: series}))
        for factor_name, series in top_ic_dic.items()
    ], axis=0)

    bottom_ic_table = pd.concat([
        get_IC(pd.DataFrame({factor_name: series}))
        for factor_name, series in bottom_ic_dic.items()
    ], axis=0)



    # print(top_ic_table)
    # print(bottom_ic_table)

    
    plot_ic_metrics(top_ic_table, bottom_ic_table, "Factor IC Metrics")
    