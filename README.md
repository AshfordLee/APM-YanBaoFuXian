# 写在前面的话----来自一位实习交易员
这是我量化生涯的第一篇研报复现，作为一个已经做过头寸管理与200W账户全权操盘的实习交易员来说，此时开始做因子挖掘似乎有些先上车再补票的嫌疑，但我认为，裸K分析、研报复现、玩模拟盘是一个量化研究员的马步基本功，希望这个项目是一个好的开端，以后能够越做越好。

# APM研报复现

这个项目是基于开源证券20200307的研报<APM因子模型的进阶版>所做的研报复现，数据来源是Tushare和BaoStock，本项目完成了数据获取与清洗、因子挖掘、收益/IC计算并画图等私募基金因子挖掘工作的标准流程。


## 项目结构

```
Tushare研报复现/
├── Function_Files/         # 功能模块文件夹
│   ├── Tushare数据准备.py   # 通过各种API爬取数据
│   ├── APM因子构造.py      # APM因子类的构造模块
│   ├── 因子处理.py         # 因子处理和分析模块
│   ├── 因子实现.py         # 通过数据计算因子并保存
├── main.py                 # 主程序
├── README.md               # 项目说明文档
├── requirements.txt        # 项目依赖文件
```

## 功能介绍

本项目主要实现以下功能：

1. **数据获取与APM因子构造**：获取原始数据并构建APM相关因子
2. **因子处理与分析**：
   - 计算因子的下期收益率
   - 因子分组分析
   - 计算IC值和T检验
   - 绘制各种分析图表

## 安装与依赖

### 环境
```
该项目开发环境为Ubuntu22.04, Python 3.13.2
```

### 依赖包

项目依赖已在`requirements.txt`文件中列出，主要包括：

```
pandas
numpy
matplotlib
scipy
```

### 安装步骤

1. 克隆项目到本地：
   ```bash
   git clone <项目地址>
   cd APM-YanBaoFuXian
   ```

2. 创建并激活虚拟环境：
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   # 或
   .venv\Scripts\activate  # Windows
   ```

3. 安装依赖（使用requirements.txt）：
   ```bash
   pip install -r requirements.txt
   ```

## 使用方法

1. 运行主程序并获取数据：
   - 由于数据量较大，需要先运行main.py的选项1，再运行选项2来获取所有的数据
    ```bash
   python main.py
   ```

2. 运行主程序并计算因子：
   - 运行主程序的选项3进行因子计算
   ```bash
   python main.py
   ```

3. 运行主程序并计算因子指标
   - 运行主程序的选项4计算并查看各种因子指标与图标



## 注意事项


1. 导入模块时可能需要调整Python路径
2. 图表显示需要GUI环境支持
3. 由于没有获取到沪深300的30分钟K线数据，所以随便选了一个股票600008.SH作为基准指数
4. 请先获取数据再计算因子，不然会报错
5. 数据获取速度比较慢，请耐心等待
6. 如果您只是为了看一眼结果，可以直接看PDF当中的图



## 联系方式

如有问题，请联系：[1207833942@qq.com]

