import requests
import pandas as pd
import numpy as np
from math import floor
from termcolor import colored as cl
import matplotlib.pyplot as plt
import  mexcAPI as api

plt.rcParams['figure.figsize'] = (20, 10)
plt.style.use('fivethirtyeight')


def getminutedata(data):
    frame=pd.DataFrame(data)
    frame=frame.iloc[:,:6]
    frame.columns=['Time','Open','High','Low','close','Volume']
    frame=frame.set_index('Time')
    frame.index= pd.to_datetime(frame.index,unit='ms')
    frame=frame.astype(float)
    return frame


# googl = get_historical_data('GOOGL', '2020-01-01')
# googl
eth_usdtt = api.getcoinprice('ETH_USDT')
eth_usdt=getminutedata(eth_usdtt)
print(eth_usdt)


def get_macd(price, slow, fast, smooth):
    exp1 = price.ewm(span=fast, adjust=False).mean()
    exp2 = price.ewm(span=slow, adjust=False).mean()
    macd = pd.DataFrame(exp1 - exp2).rename(columns={'close': 'macd'})
    signal = pd.DataFrame(macd.ewm(span=smooth, adjust=False).mean()).rename(columns={'macd': 'signal'})
    hist = pd.DataFrame(macd['macd'] - signal['signal']).rename(columns={0: 'hist'})
    frames = [macd, signal, hist]
    df = pd.concat(frames, join='inner', axis=1)
    return df

#
# googl_macd = get_macd(googl['close'], 26, 12, 9)
# googl_macd


coin_macd= get_macd(eth_usdt['close'], 26, 12, 9)
# print(result)
# eth_usdt['rsi_14']=result
# eth_usdt = eth_usdt.dropna()
# print(eth_usdt)


def plot_macd(prices, macd, signal, hist):
    ax1 = plt.subplot2grid((8, 1), (0, 0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((8, 1), (5, 0), rowspan=3, colspan=1)

    ax1.plot(prices)
    ax2.plot(macd, color='grey', linewidth=1.5, label='MACD')
    ax2.plot(signal, color='skyblue', linewidth=1.5, label='SIGNAL')

    for i in range(len(prices)):
        if str(hist[i])[0] == '-':
            ax2.bar(prices.index[i], hist[i], color='#ef5350')
        else:
            ax2.bar(prices.index[i], hist[i], color='#26a69a')

    plt.legend(loc='lower right')


plot_macd(eth_usdt['close'], coin_macd['macd'], coin_macd['signal'], coin_macd['hist'])


def implement_macd_strategy(prices, data):
    buy_price = []
    sell_price = []
    macd_signal = []
    signal = 0

    for i in range(len(data)):
        if data['macd'][i] > data['signal'][i]:
            if signal != 1:
                buy_price.append(prices[i])
                sell_price.append(np.nan)
                signal = 1
                macd_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                macd_signal.append(0)
        elif data['macd'][i] < data['signal'][i]:
            if signal != -1:
                buy_price.append(np.nan)
                sell_price.append(prices[i])
                signal = -1
                macd_signal.append(signal)
            else:
                buy_price.append(np.nan)
                sell_price.append(np.nan)
                macd_signal.append(0)
        else:
            buy_price.append(np.nan)
            sell_price.append(np.nan)
            macd_signal.append(0)

    return buy_price, sell_price, macd_signal


buy_price, sell_price, macd_signal = implement_macd_strategy(eth_usdt['close'], coin_macd)

ax1 = plt.subplot2grid((8, 1), (0, 0), rowspan=5, colspan=1)
ax2 = plt.subplot2grid((8, 1), (5, 0), rowspan=3, colspan=1)

ax1.plot(eth_usdt['close'], color='skyblue', linewidth=2, label='GOOGL')
ax1.plot(eth_usdt.index, buy_price, marker='^', color='green', markersize=10, label='BUY SIGNAL', linewidth=0)
ax1.plot(eth_usdt.index, sell_price, marker='v', color='r', markersize=10, label='SELL SIGNAL', linewidth=0)
ax1.legend()
ax1.set_title('GOOGL MACD SIGNALS')
ax2.plot(coin_macd['macd'], color='grey', linewidth=1.5, label='MACD')
ax2.plot(coin_macd['signal'], color='skyblue', linewidth=1.5, label='SIGNAL')

for i in range(len(coin_macd)):
    if str(coin_macd['hist'][i])[0] == '-':
        ax2.bar(coin_macd.index[i], coin_macd['hist'][i], color='#ef5350')
    else:
        ax2.bar(coin_macd.index[i], coin_macd['hist'][i], color='#26a69a')

plt.legend(loc='lower right')
plt.show()

position = []
for i in range(len(macd_signal)):
    if macd_signal[i] > 1:
        position.append(0)
    else:
        position.append(1)

for i in range(len(eth_usdt['close'])):
    if macd_signal[i] == 1:
        position[i] = 1
    elif macd_signal[i] == -1:
        position[i] = 0
    else:
        position[i] = position[i - 1]

macd = coin_macd['macd']
signal = coin_macd['signal']
close_price = eth_usdt['close']
macd_signal = pd.DataFrame(macd_signal).rename(columns={0: 'macd_signal'}).set_index(eth_usdt.index)
position = pd.DataFrame(position).rename(columns={0: 'macd_position'}).set_index(eth_usdt.index)

frames = [close_price, macd, signal, macd_signal, position]
strategy = pd.concat(frames, join='inner', axis=1)

strategy

googl_ret = pd.DataFrame(np.diff(eth_usdt['close'])).rename(columns={0: 'returns'})
macd_strategy_ret = []

for i in range(len(googl_ret)):
    try:
        returns = googl_ret['returns'][i] * strategy['macd_position'][i]
        macd_strategy_ret.append(returns)
    except:
        pass

macd_strategy_ret_df = pd.DataFrame(macd_strategy_ret).rename(columns={0: 'macd_returns'})

investment_value = 100000
number_of_stocks = floor(investment_value / eth_usdt['close'][0])
macd_investment_ret = []

for i in range(len(macd_strategy_ret_df['macd_returns'])):
    returns = number_of_stocks * macd_strategy_ret_df['macd_returns'][i]
    macd_investment_ret.append(returns)

macd_investment_ret_df = pd.DataFrame(macd_investment_ret).rename(columns={0: 'investment_returns'})
total_investment_ret = round(sum(macd_investment_ret_df['investment_returns']), 2)
profit_percentage = floor((total_investment_ret / investment_value) * 100)
print(cl('Profit gained from the MACD strategy by investing $100k in GOOGL : {}'.format(total_investment_ret),
         attrs=['bold']))
print(cl('Profit percentage of the MACD strategy : {}%'.format(profit_percentage), attrs=['bold']))


def get_benchmark(start_date, investment_value):
    spy =eth_usdt['close']
    benchmark = pd.DataFrame(np.diff(spy)).rename(columns={0: 'benchmark_returns'})

    investment_value = investment_value
    number_of_stocks = floor(investment_value / spy[0])
    benchmark_investment_ret = []

    for i in range(len(benchmark['benchmark_returns'])):
        returns = number_of_stocks * benchmark['benchmark_returns'][i]
        benchmark_investment_ret.append(returns)

    benchmark_investment_ret_df = pd.DataFrame(benchmark_investment_ret).rename(columns={0: 'investment_returns'})
    return benchmark_investment_ret_df


benchmark = get_benchmark('2020-01-01', 100000)

investment_value = 100000
total_benchmark_investment_ret = round(sum(benchmark['investment_returns']), 2)
benchmark_profit_percentage = floor((total_benchmark_investment_ret / investment_value) * 100)
print(cl('Benchmark profit by investing $100k : {}'.format(total_benchmark_investment_ret), attrs=['bold']))
print(cl('Benchmark Profit percentage : {}%'.format(benchmark_profit_percentage), attrs=['bold']))
print(cl('MACD Strategy profit is {}% higher than the Benchmark Profit'.format(
    profit_percentage - benchmark_profit_percentage), attrs=['bold']))
