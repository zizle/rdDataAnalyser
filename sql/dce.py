# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
from sql.ancestor import DataBaseWorker
from items import DCEMsgItem, DCERkgItem, DCETargetItem, DCEPrice


class DCEWorker(DataBaseWorker):
    def exist_today(self, date):
        exist = self.worker.query(DCETargetItem).filter(DCETargetItem.date == date).first()
        return exist

    def exist_msg_today(self, date):
        exist = self.worker.query(DCEMsgItem).filter(DCEMsgItem.date == date).first()
        return exist

    def exist_msg(self, item):
        exist = self.worker.query(DCEMsgItem).filter(DCEMsgItem.date == item.date, DCEMsgItem.name == item.product_name, DCEMsgItem.contract == item.delivery_month).first()
        return exist

    def exist_rkg_today(self, date):
        exist = self.worker.query(DCERkgItem).filter(DCERkgItem.date == date).first()
        return exist

    def exist_rkg(self, item):
        exist = self.worker.query(DCERkgItem).filter(DCERkgItem.date == item.date, DCERkgItem.name == item.product_name, DCERkgItem.contract == item.contract_id, DCERkgItem.rank == item.rank).first()
        return exist

    def exist_target(self, item):
        exist = self.worker.query(DCETargetItem).filter(DCETargetItem.date == item.date, DCETargetItem.name == item.product_name, DCETargetItem.contract == item.contract_id).first()
        return exist

    def save_msg(self, data_list):
        if not data_list:
            print('没有数据可以保存')
            return
        m_daily = []
        date = data_list[0].date
        flag = 0
        if self.exist_msg_today(date):
            flag = 1
        for item in data_list:
            if flag == 1:
                if self.exist_msg(item):
                    print('当前msg_item存在', item.date, item.product_name, item.delivery_month)
                    continue
            msg_item = DCEMsgItem(
                date=item.date,
                name=item.product_name,
                contract=item.delivery_month,
                open_price=item.open_price,
                highest_price=item.highest_price,
                lowest_price=item.lowest_price,
                close_price=item.close_price,
                presettlement_price=item.presettlement_price,
                settlement_price=item.settlement_price,
                zd=item.zd,
                zd1=item.zd1,
                volume=item.volume,
                holdings=item.holdings,
                holdings_chg=item.holdings_chg,
                volume_price=item.volume_price
            )
            m_daily.append(msg_item)
        self.worker.add_all(m_daily)
        self.worker.commit()
        print('大商所日交易快讯保存完成!')

    def save_rkg(self, data_list):
        if not data_list:
            print('没有数据可以保存')
            return
        r_daily = []
        date = data_list[0].date
        flag = 0
        if self.exist_rkg_today(date):
            flag = 1
        for item in data_list:
            if flag == 1:
                if self.exist_rkg(item):
                    print('当前rkg_item已存在', item.date, item.product_name, item.contract_id, item.rank)
                    continue
            rkg_item = DCERkgItem(
                date=item.date,
                name=item.product_name,
                contract=item.contract_id,
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
                    print('当前target_item已存在', item.date, item.product_name, item.contract_id)
                    continue
            target_item = DCETargetItem(
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
            print('{}保存大商所target_data完成'.format(targets[0].date))
        else:
            print('保存数据完成，没有新的大商所target_item')

    def get_contracts(self, good):
        contracts = []
        contract_items = self.worker.query(DCETargetItem).filter(DCETargetItem.name == good).all()
        if not contract_items:
            return []
        for item in contract_items:
            if item.contract not in contracts:
                contracts.append(item.contract)
        return contracts

    def get_times(self, good, contract):
        times = []
        # print('查询之前:', times, good, contract)
        contract_items = self.worker.query(DCETargetItem).filter(DCETargetItem.name == good, DCETargetItem.contract == contract).all()
        if not contract_items:
            return '', ''
        for item in contract_items:
            if item.date not in times:
                times.append(item.date)
        # print('查询之后:', times)
        time_min = min(times)
        time_max = max(times)
        return time_min, time_max

    def get_latest(self):
        item = self.worker.query(DCETargetItem).order_by(DCETargetItem.date.desc()).first()
        if not item:
            return ''
        return item.date

    def net_holding(self, date, name, contract):
        """
        查询指定时间价格净持率数据对象
        :return DataItem
        """
        item = self.worker.query(DCETargetItem).filter(DCETargetItem.date == date, DCETargetItem.name == name, DCETargetItem.contract == contract).first()
        if not item:
            return None
        return item

    def price_index_time_interval(self, variety):
        """价格指数品种时间区间"""
        items = self.worker.query(DCEPrice).filter(DCEPrice.name == variety).all()
        if not items:
            return ['', '']
        start_item = items[0]
        end_item = items[len(items)-1]
        return [start_item.date, end_item.date]

    def variety_price(self, date, variety):
        """查询指定时间段品种权重价格指数"""
        item = self.worker.query(DCEPrice).filter(DCEPrice.name == variety, DCEPrice.date == date).first()
        if not item:
            return ()
        price = round(item.variety_price)
        return date, price, item.volume, item.holdings

    def main_contract_price(self, date, variety):
        """查询指定时间段主力合约价格指数"""
        item = self.worker.query(DCEPrice).filter(DCEPrice.name == variety, DCEPrice.date == date).first()
        if not item:
            return ()
        price = round(item.contract_price)
        return date, price, item.volume, item.holdings

    def variety_price_table_start(self):
        # 获取数据库中最新一条的时间，如果没有则获取原数据表的开始时间，获取当前日期，循环获取这个时间段每天的所有品种
        first = self.worker.query(DCEPrice).order_by(DCEPrice.id.desc()).first()
        if not first:
            first = self.worker.query(DCEMsgItem).get(1)  # 假如没有就获取原数据的第一个时间
        return first.date

    def make_variety_price_table(self, date):
        """更新价格指数的数据库表"""
        price_daily = []
        groups = self.worker.query(DCEMsgItem).filter(DCEMsgItem.date==date).group_by(DCEMsgItem.name).all()
        for group in groups:
            if group.name in ["total", "subtotal"]:
                continue
            # 根据分组的结果(品种)查询出这个品种的所有数据
            variety_all = self.worker.query(DCEMsgItem).filter(DCEMsgItem.date == date, DCEMsgItem.name == group.name).all()
            holding_price = 0
            sum_holdings = 0
            sum_volume = 0
            # 记录第一个持仓量和第一个价格
            holdings = int(float(variety_all[0].holdings))
            contract_price = float(variety_all[0].close_price)
            for item in variety_all:
                if item.contract in ["subtotal", "total"]:  # 保存msg数据的时候将小计合计名称保存在contract字段，剔除这些数据（区别于其他交易所）
                    continue
                # 计算品种权重价格指数
                holding_price += float(item.close_price) * int(float(item.holdings))
                sum_volume += int(float(item.volume))
                h = int(float(item.holdings))
                sum_holdings += h
                # 判断主力合约
                if h > holdings:
                    holdings = h
                    contract_price = float('%.4f' % float(item.close_price))
            # 总数持仓量依然为0
            if not sum_holdings:
                variety_price = 0.0
                contract_price = 0.0
                print("大商所{}.{}没有持仓量,".format(date, group.name))
                with open("log/dce_not_holdings.log", "a+", encoding="utf-8") as f:
                    f.write("大商所{}.{}没有持仓量,权重价格指数、主力合约价格指数均为0.0\n".format(date, group.name))
            else:
                variety_price = float('%.4f' % (holding_price / sum_holdings))
            # 检测当日数据是否存在
            exist_today = self.worker.query(DCEPrice).filter(DCEPrice.date == date).first()
            if exist_today:
                # 检测当前数据是否存在
                exist_record = self.worker.query(DCEPrice).filter(DCEPrice.date == date, DCEPrice.name == group.name).first()
                if exist_record:
                    print("{}的{}记录已经存在".format(date, group.name))
                    continue
            # 将相应的数据入库
            price_item = DCEPrice(
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
        print("大商所{}的数据保存完成，大小：{}个".format(date, len(price_daily)))
