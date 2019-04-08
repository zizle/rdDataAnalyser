# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

from sql.ancestor import DataBaseWorker
from items import SHFEMsgItem, SHFERkgItem, SHFETargetItem, SHFEMetalItem, SHFETotalItem, SHFEPrice
from utils.decoration import cost_time


class SHFEWorker(DataBaseWorker):
    def exist_today(self, date):
        exist = self.worker.query(SHFETargetItem).filter(SHFETargetItem.date == date).first()
        return exist

    def exist_msg_today(self, date):
        exist = self.worker.query(SHFEMsgItem).filter(SHFEMsgItem.date == date).first()
        return exist

    def exist_msg(self, item):
        exist = self.worker.query(SHFEMsgItem).filter(SHFEMsgItem.date == item.date, SHFEMsgItem.name == item.product_name, SHFEMsgItem.contract == item.delivery_month).first()
        return exist

    def exist_rkg_today(self, date):
        exist = self.worker.query(SHFERkgItem).filter(SHFERkgItem.date == date).first()
        return exist

    def exist_rkg(self, item):
        exist = self.worker.query(SHFERkgItem).filter(SHFERkgItem.date == item.date, SHFERkgItem.name == item.product_name, SHFERkgItem.contract == item.contract, SHFERkgItem.rank == item.rank).first()
        return exist

    def exist_metal_day(self, date):
        exist = self.worker.query(SHFEMetalItem).filter(SHFEMetalItem.date == date).first()
        return exist

    def exist_metal(self, item):
        exist = self.worker.query(SHFEMetalItem).filter(SHFEMetalItem.date == item.date).first()
        return exist

    def exist_total_day(self, date):
        exist = self.worker.query(SHFETotalItem).filter(SHFETotalItem.date == date).first()
        return exist

    def exist_total(self, item):
        exist = self.worker.query(SHFETotalItem).filter(SHFETotalItem.date == item.date, SHFETotalItem.name == item.product_name).first()
        return exist

    def exist_target(self, item):
        exist = self.worker.query(SHFETargetItem).filter(SHFETargetItem.date == item.date, SHFETargetItem.name == item.product_name, SHFETargetItem.contract == item.contract_id).first()
        return exist

    def save_msg(self, data_list):
        """param accept a list"""
        print('开始保存日交易快讯')
        if not data_list:
            print('没有数据可以保存')
            return
        flag = 0
        date = data_list[0].date
        if self.exist_msg_today(date):
            flag = 1
        m_daily = []
        for item in data_list:
            if flag == 1:
                print('存在')
                if self.exist_msg(item):
                    print('当前msg_item存在', item.date, item.product_name, item.delivery_month)
                    continue
            msg_item = SHFEMsgItem(
                date=item.date,
                name=item.product_name,
                contract=item.delivery_month,
                presettlement_price=item.presettlement_price,
                open_price=item.open_price,
                highest_price=item.highest_price,
                lowest_price=item.lowest_price,
                close_price=item.close_price,
                settlement_price=item.settlement_price,
                zd1_chg=item.zd1_chg,
                zd2_chg=item.zd2_chg,
                volume=item.volume,
                open_interest=item.open_interest,
                open_interest_chg=item.open_interest_chg
            )
            m_daily.append(msg_item)
        self.worker.add_all(m_daily)
        self.worker.commit()
        print('日交易快讯保存完成')

    def save_total(self, total_list):
        print('开始保存日总计信息')
        if not total_list:
            print('没有总计信息可以保存')
            return
        m_total = []
        date = total_list[0].date
        flag = 0
        if self.exist_total_day(date):
            flag = 1
        for item in total_list:
            if flag == 1:
                print('存在')
                if self.exist_total(item):
                        print('当前total_item存在：', item.date, item.product_name)
                        continue
            total_item = SHFETotalItem(
                date=item.date,
                name=item.product_name,
                highest_price=item.highest_price,
                lowest_price=item.lowest_price,
                avg_price=item.avg_price,
                volume=item.volume,
                turnover=item.turnover,
                year_volume=item.year_volume,
                year_turnover=item.year_turnover
            )
            m_total.append(total_item)
        self.worker.add_all(m_total)
        self.worker.commit()
        print('保存日总计信息完成')

    def save_metal(self, metal_list):
        print('开始保存有色金属指数')
        if not metal_list:
            print('没有指数数据可以保存')
            return
        m_metal = []
        date = metal_list[0].date
        flag = 0
        if self.exist_metal_day(date):
            flag = 1
        for item in metal_list:
            if flag == 1:
                print('存在')
                if self.exist_metal(item):
                    print('当前metal_item存在:', item.date)
                    continue
            metal_item = SHFEMetalItem(
                date=item.date,
                last_price=item.last_price,
                open_price=item.open_price,
                highest_price=item.highest_price,
                lowest_price=item.lowest_price,
                avg_price=item.avg_price,
                close_price=item.close_price,
                pre_close_price=item.pre_close_price,
                up_down=item.up_down,
                zd1=item.zd1,
                zd2=item.zd2,
                settlement_price=item.settlement_price
            )
            m_metal.append(metal_item)
        self.worker.add_all(m_metal)
        self.worker.commit()
        print('保存有色金属指数完成')

    def save_rkg(self, data_list):
        print('开始保存日交易排行')
        if not data_list:
            print('没有排行数据可以保存')
            return
        r_daily = []
        date = data_list[0].date
        flag = 0
        if self.exist_rkg_today(date):
            flag = 1
        for item in data_list:
            if flag == 1:
                print('存在')
                if self.exist_rkg(item):
                        print('当前rkg_item存在：', item.date, item.product_name, item.contract, item.rank)
                        continue
            rkg_item = SHFERkgItem(
                date=item.date,
                name=item.product_name,
                contract=item.contract,
                rank=item.rank,
                company1=item.company1,
                company1_id=item.company1_id,
                cj1=item.cj1,
                cj1_chg=item.cj1_chg,

                company2=item.company2,
                company2_id=item.company2_id,
                cj2=item.cj2,
                cj2_chg=item.cj2_chg,

                company3=item.company3,
                company3_id=item.company3_id,
                cj3=item.cj3,
                cj3_chg=item.cj3_chg,
            )
            r_daily.append(rkg_item)
        self.worker.add_all(r_daily)
        self.worker.commit()

    def save_target(self, data_list):
        if not data_list:
            print('没有数据可以保存')
            return
        targets = []
        date = data_list[0].date
        flag = 0
        if self.exist_today(date):
            flag = 1
        print('开始保存最终数据...')
        for item in data_list:
            if flag == 1:
                print('存在')
                if self.exist_target(item):
                        print('当前target_item已存在:', item.date, item.product_name, item.contract_id)
                        continue
            target_item = SHFETargetItem(
                date=item.date,
                name=item.product_name,
                contract=item.contract_id,
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
            print('{}保存上期所target_data完成'.format(targets[0].date))
        else:
            print('保存数据完成，没有上期所新的target_item')

    def get_contracts(self, good):
        """
        获取当前交易所的商品对应合约
        :param good: 商品名称
        :return: 合约s， 最早时间，最晚时间
        """
        contracts = []
        times = []
        contract_items = self.worker.query(SHFETargetItem).filter(SHFETargetItem.name == good).all()
        if not contract_items:
            return []
        for item in contract_items:
            if item.contract not in contracts:
                contracts.append(item.contract)
                times.append(item.date)
        return contracts

    def get_times(self, good, contract):
        """
        获取当前商品合约的最早和最晚时间
        :param good: 商品名
        :param contract: 合约号
        :return: time_min, time_max
        """
        times = []
        # print('查询之前:', times, good, contract)
        contract_items = self.worker.query(SHFETargetItem).filter(SHFETargetItem.name == good, SHFETargetItem.contract == contract).all()
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
        item = self.worker.query(SHFETargetItem).filter(SHFETargetItem.date == date, SHFETargetItem.name == name, SHFETargetItem.contract == contract).first()
        if not item:
            return None
        return item

    def get_latest(self):
        item = self.worker.query(SHFETargetItem).order_by(SHFETargetItem.date.desc()).first()
        if not item:
            return ''
        return item.date

    def price_index_time_interval(self, variety):
        """价格指数品种时间区间"""
        items = self.worker.query(SHFEPrice).filter(SHFEPrice.name == variety).all()
        if not items:
            return ['', '']
        start_item = items[0]
        end_item = items[len(items)-1]
        return [start_item.date, end_item.date]

    def variety_price(self, date, variety):
        """查询指定时间段品种权重价格指数"""
        item = self.worker.query(SHFEPrice).filter(SHFEPrice.name == variety, SHFEPrice.date == date).first()
        if not item:
            return ()
        return date, item.variety_price, item.volume, item.holdings

    def main_contract_price(self, date, variety):
        """查询指定时间段主力合约价格指数"""
        item = self.worker.query(SHFEPrice).filter(SHFEPrice.name == variety, SHFEPrice.date == date).first()
        if not item:
            return ()
        return date, item.contract_price, item.volume, item.holdings

    def variety_price_table_start(self):
        # 获取数据库中最新一条的时间，如果没有则获取原数据表的开始时间，获取当前日期，循环获取这个时间段每天的所有品种
        first = self.worker.query(SHFEPrice).order_by(SHFEPrice.id.desc()).first()
        if not first:
            first = self.worker.query(SHFEMsgItem).get(1)  # 假如没有就获取原数据的第一个时间
        return first.date

    def make_variety_price_table(self, date):
        """ 查询计算品种-价格并按相应日期存入新表 """
        variety_price_daily = []
        # 查询出所有的品种及其合约，分组查询
        groups = self.worker.query(SHFEMsgItem).filter(SHFEMsgItem.date==date).group_by(SHFEMsgItem.name).all()
        for group in groups:
            if group.name in ["total", "subtotal", "total1", "total2", None]:
                continue
            # 根据分组的结果查询出这个品种的所有数据
            variety_all = self.worker.query(SHFEMsgItem).filter(SHFEMsgItem.date == date, SHFEMsgItem.name == group.name).all()
            holding_price = 0  # 计算总持仓价格
            sum_holdings = 0  # 计算总持仓量
            sum_volume = 0  # 计算总成交量
            # 记录第一个持仓量和第一个价格
            holdings = int(float(variety_all[0].open_interest))
            contract_price = float(variety_all[0].close_price)
            for item in variety_all:
                # 计算品种的相应结果，形成一条记录  efp为铝期转现
                if item.contract in ["subtotal", "total", "efp"]:  # 保存msg数据的时候将小计合计名称保存在contract字段，剔除这些数据（区别于其他交易所）
                    continue
                # 计算品种权重价格指数
                holding_price += float(item.close_price) * int(float(item.open_interest))  # (持仓量X价格)的和
                volume = int(float(item.volume)) if item.volume else 0
                sum_volume += volume  # 成交量和
                h = int(float(item.open_interest))  # 取出item的持仓量
                sum_holdings += h  # 持仓量和
                if h > holdings:
                    holdings = h
                    contract_price = float('%.4f' % float(item.close_price))
            # 总数仍没有持仓量
            if not sum_holdings:
                variety_price = 0.0
                contract_price = 0.0
                print("上期所{}.{}没有持仓量权重价格指数、主力合约价格均为0.0".format(date, group.name))
                with open("log/shfe_not_holdings.log", "a+", encoding="utf-8") as f:
                    f.write("上期所{}.{}没有持仓量权重价格指数、主力合约价格均为0.0\n".format(date, group.name))
            else:
                variety_price = float('%.4f' % (holding_price / sum_holdings))  # 品种平均持仓价格
            # 检测今天数据是否存在
            exist_today = self.worker.query(SHFEPrice).filter(SHFEPrice.date == date).first()
            if exist_today:
                # 检测当前记录是否存在
                print("上期所{}的数据已存在".format(date))
                exist_record = self.worker.query(SHFEPrice).filter(SHFEPrice.date == date, SHFEPrice.name == group.name).first()
                if exist_record:
                    print("上期所{}的{}记录已经存在".format(date, group.name))
                    continue
            # 相应记录保存入表
            price_item = SHFEPrice(
                date=date,
                name=group.name,
                volume=sum_volume,
                holdings=sum_holdings,
                variety_price=variety_price,
                contract_price=contract_price
            )
            variety_price_daily.append(price_item)
        self.worker.add_all(variety_price_daily)
        self.worker.commit()
        print("上期所{}的数据保存完成，大小：{}个".format(date, len(variety_price_daily)))
