# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

from sqlalchemy import func
from sql.ancestor import DataBaseWorker
from items import CZCEMsgItem, CZCERkgItem, CZCETargetItem, CZCEPrice


class CZCEWorker(DataBaseWorker):
    def exist_today(self, date):
        exist = self.worker.query(CZCETargetItem).filter(CZCETargetItem.date == date).first()
        return exist

    def exist_msg_today(self, date):
        exist = self.worker.query(CZCEMsgItem).filter(CZCEMsgItem.date == date).first()
        return exist

    def exist_msg(self, item):
        exist = self.worker.query(CZCEMsgItem).filter(CZCEMsgItem.date == item.date, CZCEMsgItem.name == item.product_name, CZCEMsgItem.contract == item.contract).first()
        return exist

    def exist_rkg_today(self, date):
        exist = self.worker.query(CZCERkgItem).filter(CZCERkgItem.date == date).first()
        return exist

    def exist_rkg(self, item):
        exist = self.worker.query(CZCERkgItem).filter(CZCERkgItem.date == item.date, CZCERkgItem.name == item.name, CZCERkgItem.contract == item.contract, CZCERkgItem.rank == item.rank).first()
        return exist

    def exist_target(self, item):
        exist = self.worker.query(CZCETargetItem).filter(CZCETargetItem.date == item.date, CZCETargetItem.name == item.name, CZCETargetItem.contract == item.contract).first()
        return exist

    def save_msg(self, data_list):
        if not data_list:
            print('没有msg数据可以保存')
            return
        m_daily = []
        date = data_list[0].date
        flag = 0
        if self.exist_msg_today(date):
            flag = 1
        for item in data_list:
            if flag == 1:
                if self.exist_msg(item):
                    print('当前msg_item存在', item.date, item.product_name, item.contract)
                    continue
            # print('日期', type(item.date), item.date)
            # print('商品', type(item.product_name), item.product_name)
            # print('合约', type(item.contract), item.contract)
            # print('前结算', type(item.pre_settlement), item.pre_settlement)
            # print('开盘价', type(item.open_price), item.open_price)
            # print('最高价', type(item.highest_price), item.highest_price)
            # print('最低价', type(item.lowest_price), item.lowest_price)
            # print('收盘价', type(item.close_price), item.close_price)
            # print('今结算', type(item.settlement), item.settlement)
            # print('涨跌1', type(item.zd1), item.zd1)
            # print('涨跌2', type(item.zd2), item.zd2)
            # print('成交量', type(item.volume), item.volume)
            # print('空盘量', type(item.open_interest), item.open_interest)
            # print('增减量', type(item.decrease), item.decrease)
            # print('成交额', type(item.turnover), item.turnover)
            # print('交割结算价', type(item.settlement_price), item.settlement_price)
            # print('')
            msg_item = CZCEMsgItem(
                date=item.date,
                name=item.product_name,
                contract=item.contract,
                pre_settlement=item.pre_settlement,
                open_price=item.open_price,
                highest_price=item.highest_price,
                lowest_price=item.lowest_price,
                close_price=item.close_price,
                settlement=item.settlement,
                zd1=item.zd1,
                zd2=item.zd2,
                volume=item.volume,
                open_interest=item.open_interest,
                decrease=item.decrease,
                turnover=item.turnover,
                settlement_price=item.settlement_price
            )
            m_daily.append(msg_item)
        self.worker.add_all(m_daily)
        self.worker.commit()
        print('郑商所日统计数据保存完成')

    def save_rkg(self, data_list):
        if not data_list:
            print('没有郑商所rkg可以保存')
            return
        r_daily = []
        date = data_list[0].date
        flag = 0
        if self.exist_rkg_today(date):
            flag = 1
        for item in data_list:
            # print('date', type(item.date), item.date)
            #
            # print('company1', type(item.company1), item.company1)
            # print('company2', type(item.company2), item.company2)
            # print('company3', type(item.company3), item.company3)
            # print('cj1', type(item.cj1), item.cj1)
            #
            # print('name', type(item.name), item.name)
            # print('contract', type(item.contract), item.contract)
            # print('rank', type(item.rank), item.rank)
            # print('company1', type(item.company1), item.company1)
            # print('cj1', type(item.cj1), item.cj1)
            # print('cj1_chg', type(item.cj1_chg), item.cj1_chg)
            # print('company2', type(item.company2), item.company2)
            # print('cj2', type(item.cj2), item.cj2)
            # print('cj2_chg', type(item.cj2_chg), item.cj2_chg)
            # print('company3', type(item.company3), item.company3)
            # print('cj3', type(item.cj3), item.cj3)
            # print('cj3_chg', type(item.cj3_chg), item.cj3_chg)
            # print('')
            if flag == 1:
                if self.exist_rkg(item):
                    print('当前rkg_item存在', item.date, item.name, item.contract, item.rank)
                    continue

            rkg_item = CZCERkgItem(
                date=item.date,
                name=item.name,
                contract=item.contract,
                rank=item.rank,
                company1=item.company1,
                cj1=item.cj1,
                cj1_chg=item.cj1_chg,
                company2=item.company2,
                cj2=item.cj2,
                cj2_chg=item.cj2_chg,
                company3=item.company3,
                cj3=item.cj3,
                cj3_chg=item.cj3_chg
            )
            r_daily.append(rkg_item)
        self.worker.add_all(r_daily)
        self.worker.commit()
        print('郑商所排行数据保存完成')

    def save_target(self, data_list):
        if not data_list:
            print('没有数据可以保存')
            return
        targets = []
        date = data_list[0].date
        flag = 0
        if self.exist_today(date):
            flag = 1
        for item in data_list:
            if flag == 1:
                if self.exist_target(item):
                    print('当前target_item存在', item.date, item.name, item.contract)
                    continue
            target_item = CZCETargetItem(
                date=item.date,
                name=item.name,
                contract=item.contract,
                price=item.price,
                holdings=item.holdings,
                b_volume=item.b_volume,
                s_volume=item.s_volume,
                net_holding=item.net_holding,
                holding_rate=item.holding_rate
            )
            targets.append(target_item)

        self.worker.add_all(targets)
        self.worker.commit()
        if targets:
            print('{}保存郑商所target_data完成'.format(targets[0].date))
        else:
            print('保存数据完成，没有新的郑商所target_item')

    def calculate_volume(self, date, name, contract):
        bvolume = self.worker.query(CZCERkgItem, func.sum(CZCERkgItem.cj2)).filter(CZCERkgItem.date==date, CZCERkgItem.name==name, CZCERkgItem.contract==contract).first()
        svolume = self.worker.query(CZCERkgItem, func.sum(CZCERkgItem.cj3)).filter(CZCERkgItem.date==date, CZCERkgItem.name==name, CZCERkgItem.contract==contract).first()
        return bvolume[1], svolume[1]

    def get_contracts(self, good):
        contracts = []
        contract_items = self.worker.query(CZCETargetItem).filter(CZCETargetItem.name == good).all()
        if not contract_items:
            return []
        for item in contract_items:
            if item.contract not in contracts:
                contracts.append(item.contract)
        return contracts

    def get_times(self, good, contract):
        times = []
        # print('查询之前:', times, good, contract)
        contract_items = self.worker.query(CZCETargetItem).filter(CZCETargetItem.name == good, CZCETargetItem.contract == contract).all()
        if not contract_items:
            return '', ''
        for item in contract_items:
            if item.date not in times:
                times.append(item.date)
        # print('查询之后:', times)
        time_min = min(times)
        time_max = max(times)
        return time_min, time_max

    def net_holding(self, date, name, contract):
        """
        查询指定时间价格净持率数据对象
        :return DataItem
        """
        item = self.worker.query(CZCETargetItem).filter(CZCETargetItem.date == date, CZCETargetItem.name == name, CZCETargetItem.contract == contract).first()
        if not item:
            return None
        return item

    def get_latest(self):
        item = self.worker.query(CZCETargetItem).order_by(CZCETargetItem.date.desc()).first()
        if not item:
            return ''
        return item.date

    def price_index_time_interval(self, variety):
        """价格指数品种时间区间"""
        items = self.worker.query(CZCEPrice).filter(CZCEPrice.name == variety).all()
        if not items:
            return ['', '']
        start_item = items[0]
        end_item = items[len(items)-1]
        return [start_item.date, end_item.date]

    def variety_price(self, date, variety):
        """查询指定时间段品种权重价格指数"""
        item = self.worker.query(CZCEPrice).filter(CZCEPrice.name == variety, CZCEPrice.date == date).first()
        if not item:
            return ()
        return date, item.variety_price, item.volume, item.holdings

    def main_contract_price(self, date, variety):
        """主力合约价格指数"""
        item = self.worker.query(CZCEPrice).filter(CZCEPrice.name == variety, CZCEPrice.date == date).first()
        if not item:
            return ()
        return date, item.contract_price, item.volume, item.holdings

    def variety_price_table_start(self):
        # 获取数据库中最新一条的时间，如果没有则获取原数据表的开始时间，获取当前日期，循环获取这个时间段每天的所有品种
        first = self.worker.query(CZCEPrice).order_by(CZCEPrice.id.desc()).first()
        if not first:
            first = self.worker.query(CZCEMsgItem).get(1)  # 假如没有就获取原数据的第一个时间
        return first.date

    def make_variety_price_table(self, date):
        """更新价格指数的数据库表"""
        price_daily = []
        groups = self.worker.query(CZCEMsgItem).filter(CZCEMsgItem.date == date).group_by(CZCEMsgItem.name).all()
        for group in groups:
            if group.name in ["total", "subtotal", "品种"]:  # 原数据保存了表头，需要提出表头记录
                continue
            # 根据分组的结果(品种)查询出这个品种的所有数据
            variety_all = self.worker.query(CZCEMsgItem).filter(CZCEMsgItem.date == date, CZCEMsgItem.name == group.name).all()
            holding_price = 0
            sum_holdings = 0
            sum_volume = 0
            # 记录第一个持仓量和第一个价格
            holdings = int(float(variety_all[0].open_interest.replace(',', '')))
            contract_price = float(variety_all[0].close_price.replace(',', ''))
            for item in variety_all:
                if item.contract in ["subtotal", "total", "月份"]:  # 保存msg数据的时候将小计合计名称保存在contract字段，剔除这些数据（区别于其他交易所）
                    continue
                # 计算品种权重价格指数
                holding_price += int(float(item.close_price.replace(',', ''))) * int(float(item.open_interest.replace(',', '')))
                sum_volume += int(float(item.volume.replace(',', '')))
                h = int(float(item.open_interest.replace(',', '')))
                sum_holdings += h
                # 判断主力合约
                if h > holdings:
                    holdings = h
                    contract_price = float('%.4f' % float(item.close_price.replace(',', '')))
            # 总数持仓量依然为0
            if not sum_holdings:
                variety_price = 0.0
                contract_price = 0.0
                print("郑商所{}.{}没有持仓量,权重价格指数、主力合约价格指数均为0.0".format(date, group.name))
                with open("log/czce_not_holdings.log", "a+", encoding="utf-8") as f:
                    f.write("郑商所{}.{}没有持仓量,权重价格指数、主力合约价格指数均为0.0\n".format(date, group.name))
            else:
                variety_price = float('%.4f' % (holding_price / sum_holdings))
            # 检测当日数据是否存在
            exist_today = self.worker.query(CZCEPrice).filter(CZCEPrice.date == date).first()
            if exist_today:
                # 检测当前数据是否存在
                exist_record = self.worker.query(CZCEPrice).filter(CZCEPrice.date == date, CZCEPrice.name == group.name).first()
                if exist_record:
                    print("{}的{}记录已经存在".format(date, group.name))
                    continue
            # 将相应的数据入库
            price_item = CZCEPrice(
                date=date,
                name=group.name,
                volume=sum_volume,
                holdings=sum_holdings,
                variety_price=variety_price,
                contract_price=contract_price
            )
            price_daily.append(price_item)
        self.worker.add_all(price_daily)
        self.worker.commit()
        print("郑商所{}的数据保存完成，大小：{}个".format(date, len(price_daily)))