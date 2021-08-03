import os,pickle
import pandas as pd
import sklearn.preprocessing as preprocessing

class BackTester:
    def __init__(self) :
        self.allData=None

    def backTest(self):
        #读取宁稳数据——因为宁稳的excel文件内核有问题，所以我解析成dataframe并重新全部打包到pkl里了
        data_file = open('data.pkl', 'rb')
        self.allData=pickle.load(data_file).reset_index(drop=True)
        #读取所有交易日期
        date_file = open('date.pkl', 'rb')
        trade_days=pickle.load(date_file)
        #每5个交易日进行一次
        run_days={}
        day_indexes=[]
        interval=5#轮动周期
        for i in range(len(trade_days)):
            if(i % interval != 0):
                continue
            run_days[i]=trade_days[i]
            day_indexes.append(i)
        #预留一段历史数据作为某些需要历史数据的因子的前置数据
        day_indexes=day_indexes[10:]
        #持仓属性
        cc_dict={}
        #初始金额、现金、总资产
        init=7000
        cash = 7000
        total=7000
        #用列表储存每期结果
        re_list=[]
        #交易
        for day_index in day_indexes:
            base_date=run_days[day_index]
            his_date=trade_days[day_index-30:day_index]#取前30天的数据
            #计算本期持仓只数
            k=int(total//1400)
            #计算本期标的
            target,today_price = self.get_target_by_date(base_date,his_date,k)
            #根据标的和持仓进行调仓
            cc_dict,change,bond_value = self.trade(target,today_price,cc_dict)
            cash+=change
            total = bond_value+cash
            msg="日期：{}，总市值：{}，持仓市值：{}，现金：{}，净值：{} ".format(base_date,total,bond_value,cash,total/init)
            result={
                'date':base_date,
                'total':total,
                'bond_value':bond_value,
                'cash':cash,
                'net_value':total/init
            }
            print(msg)
            re_list.append(result)
        result_df=pd.DataFrame(re_list)    
        result_df.to_excel('backtest.xlsx',encoding='gbk')

            
        

    def trade(self,target,today_price,cc_dict):
        change = 0#现金流
        bond_value=0#持仓价值
        candidates = {}
        #轮入
        for index, row in target.iterrows():
            c=CB()
            c.id=row['code']
            c.price=row['price']
            if c.id not in cc_dict:                
                c.op = '建仓'
                c.buy_price = row['price']                
                c.gain = '0%'
                change -= c.price*10*1.0002# 买入——现金减少=建仓价*10张（手续费万2）
                bond_value+=c.price*10#当期证券市值
                print('建仓：{},价格{}'.format(c.id,c.price))
            else:
                c.op = '持仓'
                c.buy_price = cc_dict[c.id].price
                diff_price = round(
                    (float(c.price) - float(c.buy_price)) / float(c.buy_price) * 100, 1)
                c.gain = '%s%%' % diff_price
                bond_value+=c.price*10#当期证券市值
                print('持仓：{},价格{},收益：{}'.format(c.id,c.price,c.gain))
            candidates[c.id] = c
        #卖出
        for id, value in cc_dict.items():
            if id not in candidates:
                if id in today_price:
                    diff_price = round((float(today_price[id].price) - float(
                        cc_dict[id].buy_price)) / float(cc_dict[id].buy_price) * 100, 1)
                    today_price[id].op = '清仓'
                    today_price[id].buy_price = cc_dict[id].buy_price
                    today_price[id].gain = '%s%%' % diff_price
                    change +=today_price[id].price*10*(1-0.0002)# 卖出——现金增加=卖出价*10张（手续费万2）
                    print('清仓：{},价格{},收益：{}'.format(id,today_price[id].price,today_price[id].gain))
                else:
                    #有时候会遇到提取强赎/退市，因为宁稳数据前后格式不一，这里方便起见就按上一个调仓日的收盘价卖出
                    print('清仓：{}无法交易'.format(id))                    
                    change +=cc_dict[id].price*10*(1-0.0002)# 卖出——资金增加卖出价*10张（手续费万2）
        return candidates,change,bond_value

    def get_target_by_date(self,base_date,his_date,k):
        allData=self.allData
        # 获得当天数据——自行添加需要的指标
        today_data=allData[allData['base_date']==base_date]
        bond_metric_DF=pd.DataFrame()
        bond_metric_DF['code']=today_data['转债代码'].astype(str)
        bond_metric_DF['short_name']=today_data['转债名称']
        bond_metric_DF['price']=today_data['转债价格']
        bond_metric_DF['convert_premium_ratio']=today_data['转股溢价率'].apply(lambda x:float(x.strip("%")))
        bond_metric_DF['turnover_rt']=today_data['转债换手率'].apply(lambda x:float(x.strip("%")))
        bond_metric_DF['cb_rt']=today_data['涨跌'].apply(lambda x:float(x.strip("%")))
        bond_metric_DF['st_rt']=today_data['涨跌.1'].apply(lambda x:float(x.strip("%")))
        bond_metric_DF['zfdb']=(bond_metric_DF['cb_rt']-bond_metric_DF['st_rt'])/(100+bond_metric_DF['convert_premium_ratio'])
        # 排除150元以上的转债
        # bond_metric_DF=bond_metric_DF[(bond_metric_DF.price<150)]
 
        # 因子标准化
        metrics2mad=['zfdb','convert_premium_ratio']#这里是要用到的因子
        bond_metric_DF=self.standard(metrics2mad,bond_metric_DF)
        # 计算最终得分
        # bond_metric_DF['final_factor']=0.5*(1-bond_metric_DF['convert_premium_ratio_std'])+0.5*(1-bond_metric_DF['zfdb'])
        bond_metric_DF['final_factor']=(1-bond_metric_DF['convert_premium_ratio_std'])
        # 得分排序
        bond_metric_DF = bond_metric_DF.sort_values('final_factor',ascending=False)
        bond_metric_DF=bond_metric_DF.reset_index(drop = True)
        # 得到标的
        target=bond_metric_DF.iloc[:k,:].loc[:,['code','price']]

        # 当日所有转债收盘价
        today_price={}
        for index, row in today_data.iterrows():
            c=CB()
            c.id=str(row['转债代码'])
            c.price=row['转债价格']
            today_price[c.id]=c
        return target,today_price   
    
    def standard(self,metrics2mad,bond_metric_DF):
        k=3 #拦截倍数   
        for metric in metrics2mad:
            # 计算拦截值
            
            mad=bond_metric_DF[metric].mad()#计算mad值
            median=bond_metric_DF[metric].median()# 计算中位值
            high=median+k*mad#高值
            low=median-k*mad#低值
            var=bond_metric_DF[metric].var()# 计算方差
            #MAD拦截
            cname="{}_MAD".format(metric)
            bond_metric_DF[cname]=bond_metric_DF[metric].apply(func=self.mad_filter,args=(high,low))
            # print("{}:{},{},{},{},{}".format(metric,mad,median,high,low,var))

            #zscore计算
            zname="{}_zcores".format(metric)
            zscaler = preprocessing.StandardScaler()
            scale_param=zscaler.fit(bond_metric_DF[cname].values.reshape(-1, 1))
            bond_metric_DF[zname] = zscaler.fit_transform(bond_metric_DF[cname].values.reshape(-1, 1), scale_param)

            #min-max归一化
            sname="{}_std".format(metric)
            minmax_scaler= preprocessing.MinMaxScaler()
            minmax_scale_param=minmax_scaler.fit(bond_metric_DF[zname].values.reshape(-1, 1))
            bond_metric_DF[sname] = minmax_scaler.fit_transform(bond_metric_DF[zname].values.reshape(-1, 1), minmax_scale_param)
            # bond_metric_DF.to_csv('V8.csv')
        return bond_metric_DF

    def mad_filter(self,x,high,low):
        if x>high:
            return high
        elif x<low:
            return low
        else:
            return x 

class CB():
    def set_fields(self, id, price,):
        self.id = id
        self.price = price
        self.op = None
        self.buy_price = None
        self.gain = None

        
bt=BackTester()
bt.backTest()