# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

""" Crawling and analyzing and saving """
import json
import requests
import re
import time
from lxml import etree
import copy

from sql.shfe import SHFEWorker
from sql.dce import DCEWorker
from sql.cffex import CFFEXWorker
from sql.czce import CZCEWorker
from items import Item
from settings import SHFE_PRODUCT_NAMES, DCE_PRODUCT_NAMES, CZCE_PRODUCT_NAMES


class SHFESpider(object):
    def __init__(self):
        """ initialization """
        # 连接数据库
        self.writer = SHFEWorker()

    def update_data(self, date):
        """
        获取解析对应时间的数据
        :param times: 时间生成器
        :return: None
        """
        # 1.请求msg数据
        daily_detail, daily_total, daily_metal = self.get_msg_day(date)
        print('上期所{}快讯数据{}个！'.format(date, len(daily_detail)))
        print('上期所{}总计数据{}个！'.format(date, len(daily_total)))
        print('上期所{}金属数据{}个！'.format(date, len(daily_metal)))
        # 1.1 保存msg数据
        self.writer.save_msg(daily_detail)
        self.writer.save_total(daily_total)
        self.writer.save_metal(daily_metal)

        # 2. 请求rkg数据
        daily_rkg = self.get_rkg_day(date)
        print('上期所{}排行数据{}个！'.format(date, len(daily_rkg)))
        # 2.2 保存rkg数据
        self.writer.save_rkg(daily_rkg)
        if not daily_detail or not daily_rkg:
            print('上期所{}msg或rkg其中一项没有数据'.format(date))
            return

        # 3. 合并目标结果
        daily_target = self._merge_target_day(daily_detail, daily_rkg)
        print('上期所{}目标数据{}个！'.format(date, len(daily_target)))
        # 3.1 保存目标结果
        self.writer.save_target(daily_target)
        # 关闭数据库连接
        self.writer.close()

    def get_msg_day(self, date):
        daily_detail, daily_total, daily_metal = [], [], []
        url = 'http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat?isAjax=true'.format(date)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.get(url=url, headers=headers)
            print('响应码：', rep.status_code)
        except Exception as e:
            print('{}发起请求上期所MSG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求上期所MSG失败{}！！！'.format(ct, date, e))
            return daily_detail, daily_total, daily_metal
        if rep.status_code == 404:
            print('获取上期所{}快讯请求响应404->今日没有数据！'.format(date))
        if rep.status_code == 200:
            print('获取上期所{}交易快讯数据成功！'.format(date))
            msg_data = rep.content.decode('utf-8')  # json
            daily_detail, daily_total, daily_metal = self._parse_daily_msg(msg_data, date)
            if not daily_detail:
                print('没有解析到上期所{}的交易快讯数据'.format(date))
            if not daily_total:
                print('没有解析到上期所{}的总计信息'.format(date))
            if not daily_metal:
                print('没有解析到上期所{}的有色金属信息'.format(date))

            # # 保存原始数据文件
            # date_dir = os.path.join(SHFE_FILE_ROOT_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'MSG.json'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(msg_data)

        return daily_detail, daily_total, daily_metal

    def get_rkg_day(self, date):
        """
        获取相应日期的日交易排行数据
        :param date: 日期
        :return: 日排行数据对象集合
        """
        daily_rkg = []
        url = 'http://www.shfe.com.cn/data/dailydata/kx/pm{}.dat'.format(date)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.get(url=url, headers=headers)
        except Exception as e:
            print('{}发起请求上期所RKG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求上期所RKG失败{}！！！'.format(ct, date, e))
            return daily_rkg
        if rep.status_code == 404:
            print('获取上期所{}排行请求响应404！->今日没有数据！'.format(date))
        if rep.status_code == 200:
            print('获取{}上期所排行数据成功！'.format(date))
            rkg_data = rep.content.decode('utf-8')  # json
            # 解析日排行数据
            daily_rkg = self._parse_daily_rkg(rkg_data, date)
            if not daily_rkg:
                print('没有解析到上期所{}的排行数据'.format(date))

            # # 保存原始数据文件
            # date_dir = os.path.join(SHFE_FILE_ROOT_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'RKG.json'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(rkg_data)
        return daily_rkg

    @staticmethod
    def _parse_daily_msg(data, date):
        """
        解析日交易快讯数据
        :param data: 日交易快讯原始json数据
        :param date: 日期
        :return: 详情、合计、有色金属数据对象集合
        """
        detail_list = []
        total_list = []
        metal_list = []
        # 将json转为字典类型
        json_dict = json.loads(data)
        # 取出想要的数据
        datas = json_dict.get('o_curinstrument', [])  # 结果仍是一个个字典的列表
        if not datas:
            print('o_curinstrument没有')
        for data_dict in datas:
            # 实例化数据对象
            item = Item()
            # 取值
            item.date = date  # 日期
            product_name = data_dict.get('PRODUCTNAME').strip(' ')  # 商品名称-中文

            # # 新品种自动更新加入
            # if product_name not in SHFE_PRODUCT_NAMES:
            #     if product_name not in GOODS_LIB["shfe"]:
            #         GOODS_LIB["shfe"].append(product_name)
            #         # 修改文件
            #         goods_lib_filename = os.path.join(FILE_DIR, 'libs.dat')
            #         with open(goods_lib_filename, 'w', encoding="utf-8") as f:
            #             f.write(str(GOODS_LIB))
            #     # 获取英文名称
            #     en_name = data_dict.get("PRODUCTID").strip(' ')
            #     SHFE_PRODUCT_NAMES[product_name] = en_name[0:2]
            #     # 修改文件
            #     product_filename = os.path.join(FILE_DIR, "shfe.p.dat")
            #     with open(product_filename, 'w', encoding='utf-8') as f:
            #         f.write(str(SHFE_PRODUCT_NAMES))
            item.product_name = SHFE_PRODUCT_NAMES.get(product_name, '')  # 商品名称-英文（20201012默认''防止没有的品种后续strip报错）
            delivery_month = data_dict.get('DELIVERYMONTH').strip(' ')  # 合约代码
            if delivery_month == '小计':
                delivery_month = 'subtotal'
            if delivery_month == '合计':
                delivery_month = 'total'
            item.delivery_month = delivery_month
            item.presettlement_price = data_dict.get('PRESETTLEMENTPRICE') if data_dict.get('PRESETTLEMENTPRICE') or data_dict.get('PRESETTLEMENTPRICE') == 0 else None  # 前结算
            item.open_price = data_dict.get('OPENPRICE') if data_dict.get('OPENPRICE') or data_dict.get('OPENPRICE') == 0 else None  # 开盘价
            item.highest_price = data_dict.get('HIGHESTPRICE') if data_dict.get('HIGHESTPRICE') or data_dict.get('HIGHESTPRICE') == 0 else None  # 最高价
            item.lowest_price = data_dict.get('LOWESTPRICE') if data_dict.get('LOWESTPRICE') or data_dict.get('LOWESTPRICE') == 0 else None  # 最低价
            item.close_price = data_dict.get('CLOSEPRICE') if data_dict.get('CLOSEPRICE') or data_dict.get('CLOSEPRICE') == 0 else None  # 收盘价
            item.settlement_price = data_dict.get('SETTLEMENTPRICE') if data_dict.get('SETTLEMENTPRICE') or data_dict.get('SETTLEMENTPRICE') == 0 else None  # 结算参考价
            item.zd1_chg = data_dict.get('ZD1_CHG') if data_dict.get('ZD1_CHG') or data_dict.get('ZD1_CHG') == 0 else None  # 涨跌1
            item.zd2_chg = data_dict.get('ZD2_CHG') if data_dict.get('ZD2_CHG') or data_dict.get('ZD2_CHG') == 0 else None  # 涨跌2
            item.volume = data_dict.get('VOLUME') if data_dict.get('VOLUME') or data_dict.get('VOLUME') == 0 else None  # 成交手
            item.open_interest = data_dict.get('OPENINTEREST') if data_dict.get('OPENINTEREST') or data_dict.get('OPENINTEREST') == 0 else None  # 持仓手
            item.open_interest_chg = data_dict.get('OPENINTERESTCHG') if data_dict.get('OPENINTERESTCHG') or data_dict.get('OPENINTERESTCHG') == 0 else None  # 变化
            """ 2020-10-16修改: 出现的原油TAS数据持仓量为None导致后面更新价格指数错误 
                剔除持仓量为None的数据
            """
            if item.open_interest is not None:
                detail_list.append(item)
        # 各商品合计信息
        total_data = json_dict.get('o_curproduct', [])  # 结果也是字典列表
        if not total_data:
            print('o_curproduct没有')
        for product_dict in total_data:
            # 实例化
            total_item = Item()
            total_item.date = date
            product_name = product_dict.get('PRODUCTNAME')
            if product_name:
                total_item.product_name = SHFE_PRODUCT_NAMES.get(product_name.strip(' '))
            else:
                total_item.product_name = product_dict.get('PRODUCTID')[:2]  # 商品名称-英文
            total_item.highest_price = product_dict.get('HIGHESTPRICE') if product_dict.get('HIGHESTPRICE') or product_dict.get('HIGHESTPRICE') == 0 else None  # 最高价
            total_item.lowest_price = product_dict.get('LOWESTPRICE') if product_dict.get('LOWESTPRICE') or product_dict.get('LOWESTPRICE') == 0 else None  # 最低价
            total_item.avg_price = product_dict.get('AVGPRICE') if product_dict.get('AVGPRICE') or product_dict.get('AVGPRICE') == 0 else None
            total_item.volume = product_dict.get('VOLUME') if product_dict.get('VOLUME') or product_dict.get('VOLUME') == 0 else None
            total_item.turnover = product_dict.get('TURNOVER') if product_dict.get('TURNOVER') or product_dict.get('TURNOVER') == 0 else None
            total_item.year_volume = product_dict.get('YEARVOLUME') if product_dict.get('YEARVOLUME') or product_dict.get('YEARVOLUME') == 0 else None
            total_item.year_turnover = product_dict.get('YEARTURNOVER') if product_dict.get('YEARTURNOVER') or product_dict.get('YEARTURNOVER') == 0 else None

            total_list.append(total_item)

        # 有色金属指数信息
        metal_index = json_dict.get('o_curmetalindex', [])
        if not metal_index:
            print('o_curmetalindex没有')
        for metal_dict in metal_index:
            metal_item = Item()
            metal_item.date = date
            metal_item.last_price = metal_dict.get('LASTPRICE') if metal_dict.get('LASTPRICE') or metal_dict.get('LASTPRICE') == 0 else None  # 最新价
            metal_item.open_price = metal_dict.get('OPENPRICE') if metal_dict.get('OPENPRICE') or metal_dict.get('OPENPRICE') == 0 else None  # 开盘价
            metal_item.highest_price = metal_dict.get('HIGHESTPRICE') if metal_dict.get('HIGHESTPRICE') or metal_dict.get('HIGHESTPRICE') == 0 else None  # 最高价
            metal_item.lowest_price = metal_dict.get('LOWESTPRICE') if metal_dict.get('LOWESTPRICE') or metal_dict.get('LOWESTPRICE') == 0 else None  # 最低价
            metal_item.avg_price = metal_dict.get('AVGPRICE') if metal_dict.get('AVGPRICE') or metal_dict.get('AVGPRICE') == 0 else None  # 平均价
            metal_item.close_price = metal_dict.get('CLOSEPRICE') if metal_dict.get('CLOSEPRICE') or metal_dict.get('CLOSEPRICE') == 0 else None  # 今收盘价
            metal_item.pre_close_price = metal_dict.get('PRECLOSEPRICE') if metal_dict.get('PRECLOSEPRICE') or metal_dict.get('PRECLOSEPRICE') == 0 else None  # 昨收盘价
            metal_item.up_down = metal_dict.get('UPDOWN') if metal_dict.get('UPDOWN') or metal_dict.get('UPDOWN') == 0 else None  # 涨跌
            metal_item.zd1 = metal_dict.get('UPDOWN1') if metal_dict.get('UPDOWN1') or metal_dict.get('UPDOWN1') == 0 else None  # 涨跌1
            metal_item.zd2 = metal_dict.get('UPDOWN2') if metal_dict.get('UPDOWN2') or metal_dict.get('UPDOWN2') == 0 else None  # 涨跌2
            metal_item.settlement_price = metal_dict.get('SETTLEMENTPRICE') if metal_dict.get('SETTLEMENTPRICE') or metal_dict.get('SETTLEMENTPRICE') == 0 else None  # 结算参考价

            metal_list.append(metal_item)

        return detail_list, total_list, metal_list

    @staticmethod
    def _parse_daily_rkg(data, date):
        """
        解析日交易快讯数据
        :param data: 日交易排行原始json数据
        :param date: 日期
        :return: 排行数据对象列表
        """
        rkg_list = []
        # 将json转为字典类型
        json_dict = json.loads(data)
        # 取出想要的数据
        datas = json_dict.get('o_cursor', [])
        # 遍历字典
        for data_dict in datas:
            item = Item()
            item.date = date  # 日期
            product_name = data_dict.get('PRODUCTNAME')  # 商品名称-中文
            if product_name:
                item.product_name = SHFE_PRODUCT_NAMES.get(product_name.strip(' '))
            else:
                item.product_name = data_dict.get('INSTRUMENTID')[:2]

            item.contract = data_dict.get('INSTRUMENTID')[2:].strip(' ')  # 合约代码
            item.rank = data_dict.get('RANK')  # 名次
            item.company1 = data_dict.get('PARTICIPANTABBR1') if data_dict.get('PARTICIPANTABBR1') else None  # 期货公司会员名称1
            item.company1_id = data_dict.get('PARTICIPANTID1') if data_dict.get('PARTICIPANTID1') else None  # 期货公司会员id
            item.cj1 = data_dict.get('CJ1') if data_dict.get('CJ1') or data_dict.get('CJ1') == 0 else None  # 成交量
            item.cj1_chg = data_dict.get('CJ1_CHG') if data_dict.get('CJ1_CHG') or data_dict.get('CJ1_CHG') == 0 else None  # 跟上一交易日变化

            item.company2 = data_dict.get('PARTICIPANTABBR2') if data_dict.get('PARTICIPANTABBR2') else None  # 期货公司会员名称2
            item.company2_id = data_dict.get('PARTICIPANTID2') if data_dict.get('PARTICIPANTID2') else None  # 期货公司会员id
            item.cj2 = data_dict.get('CJ2') if data_dict.get('CJ2') or data_dict.get('CJ2') == 0 else None  # 持买单量
            item.cj2_chg = data_dict.get('CJ2_CHG') if data_dict.get('CJ2_CHG') or data_dict.get('CJ2_CHG') == 0 else None  # 买单量变化

            item.company3 = data_dict.get('PARTICIPANTABBR3') if data_dict.get('PARTICIPANTABBR3') else None # 期货公司会员名称3
            item.company3_id = data_dict.get('PARTICIPANTID3') if data_dict.get('PARTICIPANTID3') else None # 期货公司会员id
            item.cj3 = data_dict.get('CJ3') if data_dict.get('CJ3') or data_dict.get('CJ3') == 0 else None  # 卖单量
            item.cj3_chg = data_dict.get('CJ3_CHG') if data_dict.get('CJ3_CHG') or data_dict.get('CJ3_CHG') == 0 else None  # 卖单量变化

            # 数据进一步处理
            if item.rank == 999:
                item.company1 = '成交量合计'
                item.company2 = '买单量合计'
                item.company3 = '卖单量合计'
            if item.rank == 999 and not item.cj1:  # 没有成交量合计数
                item.cj1 = 0
            if item.rank == 999 and not item.cj2:
                item.cj2 = 0
            if item.rank == 999 and not item.cj3:
                item.cj3 = 0

            rkg_list.append(item)
        return rkg_list

    @staticmethod
    def _merge_target_day(msg_items, rkg_items):
        """
        合并数据产生新对象
        :return: 新对象的集合列表
        """
        target_list = []
        for msg_item in msg_items:
            for rkg_item in rkg_items:
                if msg_item.delivery_month.strip(' ') == rkg_item.contract.strip(' ') and msg_item.product_name.strip(' ') == rkg_item.product_name.strip(
                        ' ') and msg_item.date == rkg_item.date and rkg_item.rank == 999:
                    target_item = Item()
                    target_item.date = rkg_item.date  # 日期
                    target_item.product_name = msg_item.product_name.strip(' ')  # 商品名称
                    target_item.contract_id = rkg_item.contract.strip(' ')  # 合约代码
                    target_item.price = msg_item.close_price  # 价格
                    target_item.holdings = msg_item.open_interest  # 持仓手
                    target_item.b_volume = rkg_item.cj2  # 持买单量（净多）
                    target_item.s_volume = rkg_item.cj3  # 持卖单量（净空）
                    target_item.net_holding = target_item.b_volume - target_item.s_volume  # 净持仓
                    if target_item.holdings == 0:  # 持仓手为0，净持率为0
                        target_item.holding_rate = str(0)
                    else:
                        target_item.holding_rate = str((target_item.net_holding / msg_item.open_interest) * 100) + '%'  # 净持率

                    target_list.append(target_item)
        return target_list


class DCESpider(object):
    # 保存当日的日详情数据
    msg_daily = []

    def __init__(self):
        self.writer = DCEWorker()

    def update_data(self, date):
        # 1. 请求msg数据
        daily_msg = self.get_msg_day(date)
        print('大商所{}的行情数据{}个'.format(date, len(daily_msg)))
        # 1.1 保存msg数据
        self.writer.save_msg(daily_msg)

        # 2. 请求rkg数据
        daily_rkg = self.get_rkg_day(date)
        print('大商所{}的排行数据{}个'.format(date, len(daily_rkg)))

        # 2.1 保存rkg数据
        self.writer.save_rkg(daily_rkg)
        if not daily_msg or not daily_rkg:
            print('大商所{}msg或rkg其中一项没有数据'.format(date))
            return

        # 3. 合并目标结果
        daily_target = self._merge_target_day(daily_msg, daily_rkg)
        print('大商所{}的目标数据{}个'.format(date, len(daily_target)))
        # 3.1 保存目标结果
        self.writer.save_target(daily_target)
        # 关闭数据库连接
        self.writer.close()

    def get_msg_day(self, date):
        daily_msg = []
        year = date[0:4]
        month = date[4:6]
        day = date[6:]
        msg_params = {
            "dayQuotes.variety": 'all',
            'dayQuotes.trade_type': 0,
            'year': int(year),
            'month': int(month) - 1,
            'day': int(day)
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.post('http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html', data=msg_params, headers=headers)
        except Exception as e:
            print('{}发起请求大商所MSG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求大商所MSG失败{}！！！'.format(ct, date, e))
            return daily_msg
        if rep.status_code == 404:
            print('获取大商所{}行情请求错误！404!'.format(date))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求大商所MSG响应404！！！'.format(ct, date))
        if rep.status_code == 200:
            print('获取大商所{}行情数据成功！'.format(date))
            msg_html = rep.content.decode('utf-8')
            daily_msg = self._parse_daily_msg(msg_html, date)
            if not daily_msg:
                print('没有解析到大商所{}的日行情数据'.format(date))

            # # 保存原始数据文件
            # date_dir = os.path.join(DCE_FILE_ROOT_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'MSG.html'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(msg_html)
        self.msg_daily = daily_msg
        return daily_msg

    def get_rkg_day(self, date):
        daily_rkg = []
        year = date[0:4]
        month = date[4:6]
        day = date[6:]
        # rkg_params_all = {
        #     'memberDealPosiQuotes.variety': 'all',  # 分类代码
        #     'memberDealPosiQuotes.trade_type': 0,
        #     'year': int(year),
        #     'month': int(month) - 1,
        #     'day': int(day),
        #     'contract.contract_id': 'all',  # 合约分类  汇总为'all'
        #     'contract.variety_id': 'all'  # 合约  汇总为 分类的代码
        # }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        # # 先请求总的合约
        # try:
        #     rep_all = requests.post(url='http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html', params=rkg_params_all, headers=headers)
        # except Exception as e:
        #     print('{}发起请求大商所RKG汇总失败{}！！！'.format(date, e))
        #     ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
        #     with open('log/miss.log', 'a+', encoding='utf-8') as f:
        #         f.write('\n{}  {}发起请求大商所RKG汇总失败{}！！！'.format(ct, date, e))
        #     return daily_rkg
        # if rep_all.status_code == 404:
        #     print('请求大商所{}的总合约响应404！'.format(date))
        #     ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
        #     with open('log/miss.log', 'a+', encoding='utf-8') as f:
        #         f.write('\n{}  请求大商所{}的总合约响应404！'.format(ct, date))
        # if rep_all.status_code == 200:
        #     print('获取{}日排行总数据信息成功!'.format(date))
        #     html_all = rep_all.content.decode('utf-8')
        # 获取具有排行的所有合约
        daily_contract = self._parse_daily_contracts(date)
        if not daily_contract:
            print('没有解析到{}排行合约号！！！'.format(date))
            return daily_rkg
        # 获取各合约号的详情排行信息
        for contract in daily_contract:
            if len(contract) > 5:
                product_name = contract[0:2]
            else:
                product_name = contract[0:1]

            params_detail = {
                'memberDealPosiQuotes.variety': product_name,  # 分类代码
                'memberDealPosiQuotes.trade_type': 0,
                'year': int(year),
                'month': int(month) - 1,
                'day': int(day),
                'contract.contract_id': contract,  # 合约分类  汇总为'all'
                'contract.variety_id': product_name  # 合约  汇总为 分类的代码
            }
            try:
                rep_detail = requests.post(url='http://www.dce.com.cn/publicweb/quotesdata/memberDealPosiQuotes.html', params=params_detail, headers=headers)
            except Exception as e:
                print('大商所{}的{}合约排行获取失败！{}'.format(date, contract, str(e)))
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  大商所{}的{}合约排行获取失败！{}'.format(ct, date, contract, str(e)))
                continue
            if rep_detail.status_code == 404:
                print('获取大商所{}的{}合约响应404！'.format(date, contract))
            if rep_detail.status_code == 200:
                html_detail = rep_detail.content.decode('utf-8')
                print('大商所{}的{}合约排行数据获取成功!'.format(date, contract))
                # 解析出详情数据, 结构 [item, item, item, ...]
                contract_detail_list = self._parse_contract_detail(html_page=html_detail, date=date, contract=contract, product_name=product_name)
                if not contract_detail_list:
                    print('没有解析到大商所{}的{}排行的详情信息'.format(date, contract))
                for item in contract_detail_list:
                    daily_rkg.append(item)  # 添加到日排行数据对象列表

                # # 保存到文件
                # date_dir = os.path.join(DCE_FILE_ROOT_DIR, date[0:4])
                # if not os.path.exists(date_dir):
                #     os.mkdir(date_dir)
                # file_dir = os.path.join(date_dir, date[4:])
                # if not os.path.exists(file_dir):
                #     os.mkdir(file_dir)
                # file_name = file_dir + '/' + '_' + contract + 'RKG.html'
                # with open(file_name, 'w', encoding='utf-8') as f:
                #     f.write(html_detail)
        return daily_rkg

    @staticmethod
    def _parse_daily_msg(html, date):
        # print(html)
        msg_list = []
        html = etree.HTML(html)
        # print(html.xpath('//*[@id="dayQuotesForm"]/div/div[2]/p[2]/span/text()'))
        has_data = html.xpath('//*[@id="dayQuotesForm"]/div/div[2]/p[2]/span/text()')[0]

        if re.match('.*暂无数据.*', has_data):
            print('大商所{}没有行情数据'.format(date))
            return msg_list
        tr_list = html.xpath('//div[@id="printData"]/div/table//tr')
        # 删除表头
        tr_list.pop(0)
        for tr in tr_list:
            item = Item()
            item.date = date
            product_name = tr.xpath('./td[1]/text()')[0].strip()
            if re.match('.*小计.*', product_name):
                item.delivery_month = 'subtotal'
            elif re.match('.*总计.*', product_name):
                item.delivery_month = 'total'
            else:
                delivery_month = tr.xpath('./td[2]/text()')[0].strip()  # 合约(交割月份)
                if len(delivery_month) > 4:
                    delivery_month = delivery_month[-4:]
                item.delivery_month = delivery_month
            item.product_name = DCE_PRODUCT_NAMES.get(product_name)  # 商品名称-英文
            item.open_price = tr.xpath('./td[3]/text()')[0].strip()  # 开盘价
            item.highest_price = tr.xpath('./td[4]/text()')[0].strip()  # 最高价
            item.lowest_price = tr.xpath('./td[5]/text()')[0].strip()  # 最低价
            item.close_price = tr.xpath('./td[6]/text()')[0].strip()  # 收盘价
            item.presettlement_price = tr.xpath('./td[7]/text()')[0].strip()  # 前结算价
            item.settlement_price = tr.xpath('./td[8]/text()')[0].strip()  # 结算价
            item.zd = tr.xpath('./td[9]/text()')[0].strip()  # 涨跌
            item.zd1 = tr.xpath('./td[10]/text()')[0].strip()  # 涨跌1
            item.volume = tr.xpath('./td[11]/text()')[0].strip()  # 成交量
            item.holdings = tr.xpath('./td[12]/text()')[0].strip()  # 持仓量
            item.holdings_chg = tr.xpath('./td[13]/text()')[0].strip()  # 持仓量变化
            item.volume_price = tr.xpath('./td[14]/text()')[0].strip()  # 成交额
            msg_list.append(item)
        return msg_list

    def _parse_daily_contracts(self, date):
        """ 解析当天所有的合约号 """
        daily_contract_list = []
        # 遍历当日的日详情数据，持仓量大于2w的取出
        for item in self.msg_daily:
            if item.date == date and int(float(item.holdings)) >= 20000:
                if re.match(".*total", item.delivery_month):
                    continue
                # print(item.product_name, item.delivery_month, item.close_price, item.holdings)
                contract = item.product_name+item.delivery_month
                # print(item.date, contract)
                daily_contract_list.append(contract)
        # print(html_page)
        # daily_contract_list = []
        # html = etree.HTML(html_page)
        #
        # has_data = html.xpath('//*[@id="memberDealPosiQuotesForm"]/div/div[2]/p[2]/span/text()')[0]
        # if re.match('.*无数据.*', has_data):
        #     print('大商所{}没有排行数据...'.format(date))
        #     return daily_contract_list
        # varieties_li = html.xpath('//*[@id="memberDealPosiQuotesForm"]/div/div[1]/div[3]/div/ul[2]/li')
        # for li in varieties_li:
        #     contract_code = li.xpath('./text()')[1].strip()
        #     daily_contract_list.append(contract_code)
        return daily_contract_list

    @staticmethod
    def _parse_contract_detail(html_page, date, contract, product_name):
        """ 解析每日详情合约排行数据 """
        contract_item_list = []
        html_contract = etree.HTML(html_page)
        tr_list = html_contract.xpath('//*[@id="printData"]/div/table[2]/tr')
        tr_list.pop(0)
        for tr in tr_list:
            item = Item()
            item.date = date
            item.product_name = product_name
            item.contract_id = contract[-4:]
            rank = tr.xpath('./td[1]/text()')[0].strip()
            if rank == '总计':
                item.rank = 'total'
            else:
                item.rank = rank
            item.company1 = tr.xpath('./td[2]/text()')[0].strip()
            item.cj1 = tr.xpath('./td[3]/text()')[0].strip()
            item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()
            item.company2 = tr.xpath('./td[6]/text()')[0].strip()
            item.cj2 = tr.xpath('./td[7]/text()')[0].strip()
            item.cj2_chg = tr.xpath('./td[8]/text()')[0].strip()
            item.company3 = tr.xpath('./td[10]/text()')[0].strip()
            item.cj3 = tr.xpath('./td[11]/text()')[0].strip()
            item.cj3_chg = tr.xpath('./td[12]/text()')[0].strip()
            contract_item_list.append(item)
        return contract_item_list

    @staticmethod
    def _merge_target_day(msg_items, rkg_items):
        target_list = []
        for msg_item in msg_items:
            for rkg_item in rkg_items:
                if msg_item.date == rkg_item.date and msg_item.product_name == rkg_item.product_name and msg_item.delivery_month == rkg_item.contract_id and rkg_item.rank == 'total':
                    target_item = Item()
                    target_item.date = rkg_item.date  # 日期
                    target_item.product_name = msg_item.product_name  # 商品名称
                    target_item.contract_id = rkg_item.contract_id  # 合约代码
                    target_item.price = msg_item.close_price  # 价格
                    target_item.holdings = int(msg_item.holdings.replace(',', ''))  # 持仓手
                    target_item.b_volume = int(rkg_item.cj2.replace(',', ''))  # 持买单量（净多）
                    target_item.s_volume = int(rkg_item.cj3.replace(',', ''))  # 持卖单量（净空）
                    target_item.net_holding = target_item.b_volume - target_item.s_volume  # 净持仓
                    if target_item.holdings == 0:  # 持仓手为0，净持率为0
                        target_item.holding_rate = str(0)
                    else:
                        target_item.holding_rate = str((target_item.net_holding / target_item.holdings) * 100) + '%'  # 净持率
                    target_list.append(target_item)
        return target_list


class CFFEXSpider(object):
    def __init__(self):
        self.writer = CFFEXWorker()

    def update_data(self, date):
        # 1. 请求msg数据
        daily_msg = self.get_msg_day(date)
        print('中金所{}统计数据{}个'.format(date, len(daily_msg)))
        # 1.1 保存msg数据
        self.writer.save_msg(daily_msg)

        # 2. 请求rkg数据
        daily_rkg = self.get_rkg_day(date)
        print('中金所{}排行数据{}个'.format(date, len(daily_rkg)))
        # 2.1 保存rkg数据
        self.writer.save_rkg(daily_rkg)
        print('msg:', daily_msg)
        print('rkg:', daily_rkg)
        # 判断是否都有数据
        if not daily_msg or not daily_rkg:
            print('中金所{}msg或rkg有一项没有数据...'.format(date))
            return

        # 3. 合并目标结果数据
        daily_target = self._merge_target_day(daily_msg)
        print('中金所{}的目标数据{}个'.format(date, len(daily_target)))
        # 3.1 保存目标数据
        self.writer.save_target(daily_target)

        # 关闭数据库连接
        self.writer.close()

    def get_msg_day(self, date):
        daily_msg = []
        year_month = date[:6]
        day = date[6:]
        url = 'http://www.cffex.com.cn/sj/hqsj/rtj/{}/{}/index.xml'.format(year_month, day)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.get(url=url, headers=headers)
        except Exception as e:
            print('{}发起请求中金所MSG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求中金所MSG失败{}！！！'.format(ct, date, e))
            return daily_msg
        if rep.status_code == 404:
            print('获取中金所{}统计请求错误！'.format(date))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  获取中金所{}统计请求错误！响应404'.format(ct, date))
        if rep.status_code == 200:
            print('获取中金所{}统计数据成功！'.format(date))
            # http://www.cffex.com.cn/error_page/error_404.html
            request_url = rep.request.url
            if re.match(r'http://www.cffex.com.cn/error_page/error_404.*', request_url):
                print('中金所{}没有统计数据'.format(date))
                return daily_msg
            xml_page = rep.content
            # 解析数据
            daily_msg = self._parse_daily_msg(xml_page, date)
            if not daily_msg:
                print('没有解析到{}中金所统计数据'.format(date))
            #
            # # 保存原始数据文件
            # date_dir = os.path.join(CFFEX_FILE_ROO_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'MSG.xml'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(xml_page.decode('utf-8'))

        return daily_msg

    def get_rkg_day(self, date):
        daily_rkg = []
        goods = ['IF', 'IC', 'IH', 'TS', 'TF', 'T']
        year_month = date[:6]
        day = date[6:]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        # http://www.cffex.com.cn/sj/ccpm/201002/04/IF.xml
        for good in goods:
            url = 'http://www.cffex.com.cn/sj/ccpm/{}/{}/{}.xml'.format(year_month, day, good)
            try:
                rep = requests.get(url=url, headers=headers)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  {}获取中金所商品{}数据时出错！{}'.format(ct, date, good, e))
            request_url = rep.request.url
            if re.match(r'http://www.cffex.com.cn/error_page/error_404.*', request_url):
                print('中金所{}没有{}的排行数据'.format(date, good))
                continue
            xml_page = rep.content
            # 解析数据
            good_rkg = self._parse_good_rkg(xml_page, date)
            for good_item in good_rkg:
                daily_rkg.append(good_item)  # 加入日汇总

            # # 文件保存原始数据
            # date_dir = os.path.join(CFFEX_FILE_ROO_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + '_' + good + 'RKG.xml'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(xml_page.decode('utf-8'))
        return daily_rkg

    @staticmethod
    def _parse_daily_msg(xml_page, date):
        msg_list = []
        tree = etree.XML(xml_page)
        for daily_data in tree:
            item = Item()
            item.date = date
            product_name = daily_data.xpath('./productid/text()')
            item.product_name = product_name[0].strip() if product_name else ''
            instrument_id = daily_data.xpath('./instrumentid/text()')
            item.instrument_id = instrument_id[0].strip()[-4:] if instrument_id else ''
            open_price = daily_data.xpath('./openprice/text()')  # 今开盘
            item.open_price = open_price[0].strip() if open_price else ''
            highest_price = daily_data.xpath('./highestprice/text()')  # 最高价
            item.highest_price = highest_price[0].strip() if highest_price else ''
            lowest_price = daily_data.xpath('./lowestprice/text()')  # 最低价
            item.lowest_price = lowest_price[0].strip() if lowest_price else ''
            volume = daily_data.xpath('./volume/text()')  # 成交量
            item.volume = volume[0].strip() if volume else ''
            turnover = daily_data.xpath('./turnover/text()')  # 成交额
            item.turnover = turnover[0].strip() if turnover else ''
            holdings = daily_data.xpath('./openinterest/text()')  # 持仓量
            item.holdings = holdings[0].strip() if holdings else ''
            close_price = daily_data.xpath('./closeprice/text()')  # 收盘价
            item.close_price = close_price[0].strip() if close_price else ''
            settlement_price = daily_data.xpath('./settlementprice/text()')  # 今结算
            item.settlement_price = settlement_price[0].strip() if settlement_price else ''
            item.zd1 = ''  # 涨跌1
            item.zd2 = ''  # 涨跌2
            msg_list.append(item)

        return msg_list

    @staticmethod
    def _parse_good_rkg(xml_page, date):
        source_items = []
        tree = etree.XML(xml_page)
        for data in tree:
            value = data.get('Value')
            if value:
                value = value.strip()
                item = Item()
                item.value = value
                item.date = date
                instrument_id1 = data.xpath('./instrumentid/text()')  # 新旧网站大小写未统一
                instrument_id2 = data.xpath('./instrumentId/text()')
                item.product_name = instrument_id1[0].strip()[:2] if instrument_id1 else instrument_id2[0].strip()[:2]  # 商品
                item.instrument_id = instrument_id1[0].strip()[-4:] if instrument_id1 else instrument_id2[0].strip()[-4:]  # 合约
                rank = data.xpath('./rank/text()')
                item.rank = rank[0].strip() if rank else ''  # 排名
                shortname = data.xpath('./shortname/text()')
                item.shortname = shortname[0].strip() if shortname else ''  # 公司名称
                partyid = data.xpath('./partyid/text()')
                item.partyid = partyid[0].strip() if partyid else ''  # 公司代号
                volume = data.xpath('./volume/text()')
                item.volume = volume[0].strip() if volume else ''  # 数量
                varvolume1 = data.xpath('./varvolume/text()')
                varvolume2 = data.xpath('./varVolume/text()')
                item.varvolume = varvolume1[0].strip() if varvolume1 else varvolume2[0].strip()
                source_items.append(item)

        return source_items

    def _merge_target_day(self, daily_msg):
        target_list = []
        for msg_item in daily_msg:
            date = msg_item.date
            name = msg_item.product_name
            contract = msg_item.instrument_id
            # 查询数据库计算总成交量(此数据无用，为提高性能，这里不查询)
            # volume = self.writer.calculate_volume(date=date, name=name, contract=contract, data_type='0')
            # 查询数据库计算总买单量
            b_volume = self.writer.calculate_volume(date=date, name=name, contract=contract, data_type='1')
            # 查询数据库计算总卖单量
            s_volume = self.writer.calculate_volume(date=date, name=name, contract=contract, data_type='2')
            if not b_volume or not s_volume:
                continue
            target_item = Item()
            target_item.date = date
            target_item.product_name = name
            target_item.contract_id = contract
            target_item.price = float(msg_item.close_price)
            target_item.holdings = int(float(msg_item.holdings))
            target_item.b_volume = b_volume
            target_item.s_volume = s_volume
            target_item.net_holding = b_volume - s_volume
            if target_item.holdings == 0:  # 持仓手为0，净持率为0
                target_item.holding_rate = str(0)
            else:
                target_item.holding_rate = str((target_item.net_holding / target_item.holdings) * 100) + '%'  # 净持率
            target_list.append(target_item)
        return target_list


class CZCESpider(object):
    def __init__(self):
        self.writer = CZCEWorker()

    def update_data(self, date):
        # 判断当前时间：
        if date <= '20100824':  # earliest
            print('时间在20100824及之前')
            msg_url = 'http://www.czce.com.cn/cn/exchange/jyxx/hq/hq{}.html'.format(date)
            rkg_url = 'http://www.czce.com.cn/cn/exchange/jyxx/pm/pm{}.html'.format(date)
        elif '20100824' < date <= '20150930':  # earlier
            print('时间在20100825~20150930')
            year = date[:4]
            msg_url = 'http://www.czce.com.cn/cn/exchange/{}/datadaily/{}.htm'.format(year, date)
            rkg_url = 'http://www.czce.com.cn/cn/exchange/{}/datatradeholding/{}.htm'.format(year, date)
        else:
            year = date[:4]
            msg_url = 'http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataDaily.htm'.format(year, date)
            rkg_url = 'http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataHolding.htm'.format(year, date)
            print('时间在20151001及之后')
        # 1. 请求msg数据
        daily_msg = self.get_msg_day(date, msg_url)
        print('郑商所{}行情数据{}个'.format(date, len(daily_msg)))
        # 1.1 保存msg数据
        self.writer.save_msg(daily_msg)

        # 2. 请求rkg数据
        daily_rkg = self.get_rkg_day(date, rkg_url)
        print('郑商所{}排行数据{}个'.format(date, len(daily_rkg)))
        # 2.1 保存rkg数据
        self.writer.save_rkg(daily_rkg)
        # 判断是否都有数据
        if not daily_msg or not daily_rkg:
            print('郑商所{}msg或rkg有一项没有数据...'.format(date))
            return
        # 3. 合并目标结果数据
        daily_target = self._merge_target_day(daily_msg, daily_rkg, date)
        print('郑商所{}的目标数据{}个'.format(date, len(daily_target)))
        # 3.1 保存目标数据
        self.writer.save_target(daily_target)

        # 关闭数据库连接
        self.writer.close()

    def get_msg_day(self, date, url):
        daily_msg = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.get(url=url, headers=headers)
        except Exception as e:
            print('{}发起请求郑商所MSG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求郑商所MSG失败{}！！！'.format(ct, date, e))
            return daily_msg
        if rep.status_code == 404:
            print('获取郑商所{}日行情请求错误！'.format(date))
            print('郑商所{}没有行情数据'.format(date))
        if rep.status_code == 200:
            print('获取郑商所{}日行情数据成功！'.format(date))
            request_url = rep.request.url
            if re.match(r'http://www.czce.com.cn/.*/404.*', request_url):
                print('郑商所{}没有行情数据'.format(date))
                return daily_msg
            html_page = rep.content.decode('utf-8')

            # 解析页面
            if date <= '20100824':
                daily_msg = self._parse_earliest_msg(html_page, date)
            elif '20100824' < date <= '20171227':
                daily_msg = self._parse_earlier_msg(html_page, date)
            else:
                daily_msg = self._parse_daily_msg(html_page, date)

            if not daily_msg:
                print('没有解析到{}郑商所行情数据'.format(date))
            #
            # # 保存原始数据文件
            # date_dir = os.path.join(CZCE_FILE_ROO_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'MSG.html'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(html_page)

        return daily_msg

    def get_rkg_day(self, date, url):
        daily_rkg = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
        }
        try:
            rep = requests.get(url=url, headers=headers)
        except Exception as e:
            print('{}发起请求郑商所RKG失败{}！！！'.format(date, e))
            ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
            with open('log/miss.log', 'a+', encoding='utf-8') as f:
                f.write('\n{}  {}发起请求郑商所RKG失败{}！！！'.format(ct, date, e))
            return daily_rkg
        if rep.status_code == 404:
            print('获取郑商所{}持仓排名请求错误！'.format(date))
            print('郑商所{}没有排行数据'.format(date))
        if rep.status_code == 200:
            print('获取郑商所{}持仓排名数据成功！'.format(date))
            request_url = rep.request.url
            if re.match(r'http://www.czce.com.cn/.*/404.*', request_url):
                print('郑商所{}没有排行数据'.format(date))
                return daily_rkg
            html_page = rep.content.decode('utf-8')
            # 解析页面
            if date <= '20100824':
                daily_rkg = self._parse_earliest_rkg(html_page, date)
            elif '20100824' < date <= '20150930':
                daily_rkg = self._parse_earlier_rkg(html_page, date)
            elif '20150930' < date <= '20171227':
                daily_rkg = self._parse_earlier2_rkg(html_page, date)
            else:
                daily_rkg = self._parse_daily_rkg(html_page, date)

            # 原始页面数据保存
            if not daily_rkg:
                print('没有解析到{}郑商所排行数据'.format(date))

            # # 保存原始数据文件
            # date_dir = os.path.join(CZCE_FILE_ROO_DIR, date[0:4])
            # if not os.path.exists(date_dir):
            #     os.mkdir(date_dir)
            # file_dir = os.path.join(date_dir, date[4:])
            # if not os.path.exists(file_dir):
            #     os.mkdir(file_dir)
            # file_name = file_dir + '/' + 'RKG.html'
            # with open(file_name, 'w', encoding='utf-8') as f:
            #     f.write(html_page)
        return daily_rkg

    @staticmethod
    def _parse_earlier_msg(html_page, date):
        msg_list = []
        html = etree.HTML(html_page)
        # //*[@id="senfe"]/tbody/tr[2]
        tr_list = html.xpath('//*[@id="senfe"]/tr')
        if tr_list:
            tr_list.pop(0)
            for tr in tr_list:
                item = Item()
                item.date = date
                instrument_id = tr.xpath('./td[1]/text()')[0].strip()
                if instrument_id == '小计':
                    item.product_name = 'subtotal'
                elif instrument_id == '总计':
                    item.product_name = 'total'
                else:
                    item.product_name = instrument_id[:2]
                item.contract = instrument_id[2:]
                pre_settlement = tr.xpath('./td[2]/text()')  # 昨结算
                item.pre_settlement = pre_settlement[0].strip() if pre_settlement else ''
                open_price = tr.xpath('./td[3]/text()')  # 今开盘
                item.open_price = open_price[0].strip() if open_price else ''
                highest_price = tr.xpath('./td[4]/text()')  # 最高价
                item.highest_price = highest_price[0].strip() if highest_price else ''
                lowest_price = tr.xpath('./td[5]/text()')  # 最低价
                item.lowest_price = lowest_price[0].strip() if lowest_price else ''
                close_price = tr.xpath('./td[6]/text()')  # 昨收盘价
                item.close_price = close_price[0].strip() if close_price else ''
                settlement = tr.xpath('./td[7]/text()')  # 今结算
                item.settlement = settlement[0].strip() if settlement else ''
                zd1 = tr.xpath('./td[8]/text()')  # 涨跌1
                item.zd1 = zd1[0].strip() if zd1 else ''
                zd2 = tr.xpath('./td[9]/text()')  # 涨跌2
                item.zd2 = zd2[0].strip() if zd2 else ''
                volume = tr.xpath('./td[10]/text()')  # 成交量（手）
                item.volume = volume[0].strip() if volume else ''
                open_interest = tr.xpath('./td[11]/text()')  # 空盘量
                item.open_interest = open_interest[0].strip() if open_interest else ''
                decrease = tr.xpath('./td[12]/text()')  # 增减量
                item.decrease = decrease[0].strip() if decrease else ''
                turnover = tr.xpath('./td[13]/text()')  # 成交额
                item.turnover = turnover[0].strip() if turnover else ''
                settlement_price = tr.xpath('./td[14]/text()')  # 交割结算价
                item.settlement_price = settlement_price[0].strip() if settlement_price else ''

                msg_list.append(item)

        return msg_list

    @staticmethod
    def _parse_earliest_msg(html_page, date):
        msg_list = []
        html = etree.HTML(html_page)
        # /html/body/table/tbody/tr/td/table[2]/tbody/tr[2]
        tr_list = html.xpath('/html/body/table/tr/td/table//tr')
        if tr_list:
            # 去掉表头
            tr_list.pop(0)
            if '20080319' <= date < '20080710':  # 在20080319~20080709之后都有一个涨跌1和涨跌2的注释
                tr_list.pop()  # 去掉涨跌1和涨跌2的注释
            for tr in tr_list:
                item = Item()
                item.date = date
                instrument_id = tr.xpath('./td[1]/text()')[0].strip()
                if instrument_id == '小计':
                    item.product_name = 'subtotal'
                elif instrument_id == '总计':
                    item.product_name = 'total'
                else:
                    item.product_name = instrument_id[:2]

                item.contract = instrument_id[2:]
                pre_settlement = tr.xpath('./td[2]/text()')  # 昨结算
                item.pre_settlement = pre_settlement[0].strip() if pre_settlement else ''
                open_price = tr.xpath('./td[3]/text()')  # 今开盘
                item.open_price = open_price[0].strip() if open_price else ''
                highest_price = tr.xpath('./td[4]/text()')  # 最高价
                item.highest_price = highest_price[0].strip() if highest_price else ''
                lowest_price = tr.xpath('./td[5]/text()')  # 最低价
                item.lowest_price = lowest_price[0].strip() if lowest_price else ''
                close_price = tr.xpath('./td[6]/text()')  # 收盘价
                item.close_price = close_price[0].strip() if close_price else ''
                settlement = tr.xpath('./td[7]/text()')  # 今结算
                item.settlement = settlement[0].strip() if settlement else ''
                zd1 = tr.xpath('./td[8]/text()')  # 涨跌1
                item.zd1 = zd1[0].strip() if zd1 else ''
                if '20080319' <= date < '20080710':
                    item.zd2 = tr.xpath('./td[9]/text()')[0].strip()  # 涨跌2
                    volume = tr.xpath('./td[10]/text()')  # 成交量（手）
                    item.volume = volume[0].strip() if volume else ''
                    open_interest = tr.xpath('./td[11]/text()')  # 空盘量
                    item.open_interest = open_interest[0].strip() if open_interest else ''
                    decrease = tr.xpath('./td[12]/text()')  # 增减量
                    item.decrease = decrease[0].strip() if decrease else ''
                    turnover = tr.xpath('./td[13]/text()')  # 成交额
                    item.turnover = turnover[0].strip() if turnover else ''
                    settlement_price = tr.xpath('./td[14]/text()')  # 交割结算价
                    item.settlement_price = settlement_price[0].strip() if settlement_price else ''
                else:
                    item.zd2 = ''
                    volume = tr.xpath('./td[9]/text()')  # 成交量（手）
                    item.volume = volume[0].strip() if volume else ''
                    open_interest = tr.xpath('./td[10]/text()')  # 空盘量
                    item.open_interest = open_interest[0].strip() if open_interest else ''
                    decrease = tr.xpath('./td[11]/text()')  # 增减量
                    item.decrease = decrease[0].strip() if decrease else ''
                    turnover = tr.xpath('./td[12]/text()')  # 成交额
                    item.turnover = turnover[0].strip() if turnover else ''
                    settlement_price = tr.xpath('./td[13]/text()')  # 交割结算价
                    item.settlement_price = settlement_price[0].strip() if settlement_price else ''

                msg_list.append(item)

        return msg_list

    @staticmethod
    def _parse_daily_msg(html_page, date):
        # print(html_page)
        msg_list = []
        html = etree.HTML(html_page)
        # //*[@id="tab1"]/tbody/tr
        tr_list = html.xpath('//*[@id="tab1"]//tr')
        # 删掉最后一个(表尾)
        tr_list.pop()
        for tr in tr_list:
            item = Item()
            item.date = date
            instrument_id = tr.xpath('./td[1]/text()')[0].strip()  # 产品名和合约号
            if instrument_id == '小计':
                item.product_name = 'subtotal'
            elif instrument_id == '总计':
                item.product_name = 'total'
            else:
                item.product_name = instrument_id[:2]
            item.contract = instrument_id[2:]
            pre_settlement = tr.xpath('./td[2]/text()')  # 昨结算
            item.pre_settlement = pre_settlement[0].strip() if pre_settlement else ''
            open_price = tr.xpath('./td[3]/text()')  # 今开盘
            item.open_price = open_price[0].strip() if open_price else ''
            highest_price = tr.xpath('./td[4]/text()')  # 最高价
            item.highest_price = highest_price[0].strip() if highest_price else ''
            lowest_price = tr.xpath('./td[5]/text()')  # 最低价
            item.lowest_price = lowest_price[0].strip() if lowest_price else ''
            close_price = tr.xpath('./td[6]/text()')  # 昨收盘价
            item.close_price = close_price[0].strip() if close_price else ''
            settlement = tr.xpath('./td[7]/text()')  # 今结算
            item.settlement = settlement[0].strip() if settlement else ''
            zd1 = tr.xpath('./td[8]/text()')  # 涨跌1
            item.zd1 = zd1[0].strip() if zd1 else ''
            zd2 = tr.xpath('./td[9]/text()')  # 涨跌2
            item.zd2 = zd2[0].strip() if zd2 else ''
            volume = tr.xpath('./td[10]/text()')  # 成交量（手）
            item.volume = volume[0].strip() if volume else ''
            open_interest = tr.xpath('./td[11]/text()')  # 空盘量
            item.open_interest = open_interest[0].strip() if open_interest else ''
            decrease = tr.xpath('./td[12]/text()')  # 增减量
            item.decrease = decrease[0].strip() if decrease else ''
            turnover = tr.xpath('./td[13]/text()')  # 成交额
            item.turnover = turnover[0].strip() if turnover else ''
            settlement_price = tr.xpath('./td[14]/text()')  # 交割结算价
            item.settlement_price = settlement_price[0].strip() if settlement_price else ''

            msg_list.append(item)
        return msg_list

    @staticmethod
    def _parse_earlier_rkg(html_page, date):
        print('第二种解析方法')
        # //*[@id="toexcel"]/table/tbody/tr/td/table
        # './tr'
        rkg_list = []
        html = etree.HTML(html_page)
        # //*[@id="senfe"]/tbody/tr[3]
        table_list = html.xpath('//*[@id="toexcel"]/table/tr/td/table')
        print('table_个数', len(table_list))
        # 遍历table,找出每个table的tr(就是一个品种或者合约)
        for table in table_list:
            tr_list = table.xpath('./tr')
            print('tr_list个数', len(tr_list))
            item = Item()
            item.date = date
            # 解析品种合约号等信息
            source_name = tr_list[0].xpath('./td[1]/b/text()')[0].strip()  # 合约或者品种
            print(source_name)
            # 正则匹配
            r1 = re.match(r'.*品种.*', source_name)
            r2 = re.search(r'.*合约.*(\w{2}\d{3}).*', source_name)
            if r1:  # 品种信息
                # 取出品种
                good_name = source_name[3:5]
                item.name = CZCE_PRODUCT_NAMES.get(good_name)
                item.contract = ''
            if r2:  # 合约信息
                # 取出合约
                instrument_id = r2.group(1)
                item.name = instrument_id[:2]
                item.contract = instrument_id[2:]
            # 删除品种合约的tr
            tr_list.pop(0)
            # 删掉表头
            tr_list.pop(0)
            print('删除表头后', len(tr_list))
            for tr in tr_list:
                rank = tr.xpath('./td[1]/text()')
                if rank[0].strip() == '合计':
                    rank = 'total'
                    company1 = ''
                    company2 = ''
                    company3 = ''
                else:
                    rank = rank[0].strip()
                    company1 = tr.xpath('./td[2]/text()')
                    company2 = tr.xpath('./td[5]/text()')
                    company3 = tr.xpath('./td[8]/text()')
                item.rank = rank
                item.company1 = company1[0].strip() if company1 else ''
                cj1 = tr.xpath('./td[3]/text()')
                item.cj1 = cj1[0].strip() if cj1 else ''  # 成交量（手）
                cj1_chg = tr.xpath('./td[4]/text()') # 成交量变化
                item.cj1_chg = cj1_chg[0].strip() if cj1_chg else ''

                item.company2 = company2[0].strip() if company2 else ''
                source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                try:
                    cj2 = int(source_cj2.replace(',', ''))
                except Exception as e:
                    cj2 = 0
                item.cj2 = cj2
                cj2_chg = tr.xpath('./td[7]/text()') # 买单量变化
                item.cj2_chg = cj2_chg[0].strip() if cj2_chg else ''

                item.company3 = company3[0].strip() if company3 else ''
                source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                try:
                    cj3 = int(source_cj3.replace(',', ''))
                except Exception as e:
                    cj3 = 0
                item.cj3 = cj3
                cj3_chg = tr.xpath('./td[10]/text()')  # 卖单量变化
                item.cj3_chg = cj3_chg[0].strip() if cj3_chg else ''
                rkg_list.append(copy.deepcopy(item))
        return rkg_list

    @staticmethod
    def _parse_earliest_rkg(html_page, date):
        print('第一种解析方法')
        rkg_list = []
        html = etree.HTML(html_page)
        div_list = html.xpath('/html/body/table/tr/td[1]//div')
        # print('div个数', len(div_list))
        if div_list:
            div_list.pop(0)
            div_list.pop(0)
        table_list = html.xpath('/html/body/table/tr/td[1]//table')
        # print('table个数', len(table_list))
        if table_list:
            table_list.pop(0)  # 多余的table
        if len(div_list) == len(table_list):
            data = list(zip(div_list, table_list))
            # print('date组合个数', len(data))
            for data_item in data:
                item = Item()
                item.date = date
                product_div = data_item[0]  # 类别 div
                detail_table = data_item[1]  # 相应数据 table
                # 解析出品种，创建数据对象(品种对象？或者合约对象？)
                source_name = product_div.xpath('./b/font/text()')[0].strip()

                r1 = re.search(r'.*品种(.*)\s:.*', source_name)
                r2 = re.search(r'.*合约代码(.*)\s*日期.*', source_name)
                if r1:  # 品种
                    name = r1.group(1).strip()
                    # print('name名字:', name)
                    item.name = CZCE_PRODUCT_NAMES.get(name)
                    item.contract = ''
                if r2:  # 合约详情
                    instrument_id = r2.group(1).strip()
                    item.name = instrument_id[:2]
                    # print('合约:', item.name)
                    item.contract = instrument_id[2:]
                tr_list = detail_table.xpath('./tr')
                # print('原始tr个数', len(tr_list))
                tr_list.pop(0)  # 删掉表头
                tr_list.pop()  # 删掉表尾
                # print('详情tr_list个数', len(tr_list))
                for tr in tr_list:
                    rank_source1 = tr.xpath('./td[1]/text()')
                    rank_source2 = tr.xpath('./td[1]/b/text()')
                    if rank_source1:
                        rank = rank_source1[0].strip()
                        company1 = tr.xpath('./td[2]/text()')[0].strip()
                        company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                        company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    elif rank_source2 and rank_source2[0].strip() == '合计':
                        rank = 'total'
                        company1 = ''
                        company2 = ''
                        company3 = ''
                    else:
                        rank = ''
                        company1 = tr.xpath('./td[2]/text()')[0].strip()
                        company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                        company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    item.rank = rank
                    item.company1 = company1
                    item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手
                    item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                    item.company2 = company2
                    source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                    try:
                        cj2 = int(source_cj2.replace(',', ''))
                    except Exception as e:
                        cj2 = 0
                    item.cj2 = cj2
                    item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                    item.company3 = company3
                    source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                    try:
                        cj3 = int(source_cj3.replace(',', ''))
                    except Exception as e:
                        cj3 = 0
                    item.cj3 = cj3
                    item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                    rkg_list.append(copy.deepcopy(item))
        return rkg_list

    @staticmethod
    def _parse_earlier2_rkg(html_page, date):
        print('第2种中间的解析方法20150930~20171227')
        rkg_list = []
        # print(html_page)
        html = etree.HTML(html_page)
        # //*[@id="senfe"]/tbody/tr[1]
        tr_list = html.xpath('/html/body/table/tr/td/table/tr')
        # print('tr_个数', len(tr_list))
        mark = 0  # 判断是何种数据的标记
        for tr in tr_list:
            # 解析出第一个td
            flag = tr.xpath('./td[1]/text()')
            if not flag:
                flag = tr.xpath('./td[1]/b/text()')[0].strip()
            else:
                flag = flag[0].strip()

            r1 = re.search(r'品种.*[A-Z]+', flag)
            r2 = re.search(r'合约.*\w+\d+', flag)
            r3 = re.search(r'名次', flag)
            r4 = re.search(r'合计', flag)
            if r1:  # 以下信息是品种信息
                mark = 0
                variety_item = Item()
                name = re.search(r'[A-Z]+', flag).group()
                variety_item.date = date
                variety_item.name = name
                variety_item.contract = ''
                continue

            if r2:  # 以下信息是合约信息
                mark = 1
                contract_item = Item()
                name = re.search(r'\w+\d+', flag).group()
                contract_item.date = date
                contract_item.name = name[:2]
                contract_item.contract = name[2:]
                continue

            if r3:  # 表头
                continue
            if r4:  # 合计
                if mark == 0:
                    variety_item.rank = 'total'  # 名次
                    variety_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                    variety_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                    variety_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                    variety_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                    source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单
                    try:
                        cj2 = int(source_cj2.replace(',', ''))
                    except Exception as e:
                        cj2 = 0
                    variety_item.cj2 = cj2
                    variety_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                    variety_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                    try:
                        cj3 = int(source_cj3.replace(',', ''))
                    except Exception as e:
                        cj3 = 0
                    variety_item.cj3 = cj3
                    variety_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                    rkg_list.append(copy.deepcopy(variety_item))
                if mark == 1:
                    contract_item.rank = 'total'  # 名次
                    contract_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                    contract_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                    contract_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                    contract_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                    source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                    try:
                        cj2 = int(source_cj2.replace(',', ''))
                    except Exception as e:
                        cj2 = 0
                    contract_item.cj2 = cj2
                    contract_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                    contract_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                    try:
                        cj3 = int(source_cj3.replace(',', ''))
                    except Exception as e:
                        cj3 = 0
                    contract_item.cj3 = cj3
                    contract_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                    rkg_list.append(copy.deepcopy(contract_item))
                continue
            if mark == 0:
                variety_item.rank = tr.xpath('./td[1]/text()')[0].strip()  # 名次
                variety_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                variety_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                variety_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                variety_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                try:
                    cj2 = int(source_cj2.replace(',', ''))
                except Exception as e:
                    cj2 = 0
                variety_item.cj2 = cj2
                variety_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                variety_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                try:
                    cj3 = int(source_cj3.replace(',', ''))
                except Exception as e:
                    cj3 = 0
                variety_item.cj3 = cj3
                variety_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                rkg_list.append(copy.deepcopy(variety_item))
            if mark == 1:
                contract_item.rank = tr.xpath('./td[1]/text()')[0].strip()  # 名次
                contract_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                contract_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                contract_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                contract_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                try:
                    cj2 = source_cj2.replace(',', '')
                except Exception as e:
                    cj2 = 0
                contract_item.cj2 = cj2
                contract_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                contract_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                try:
                    cj3 = source_cj3.replace(',', '')
                except Exception as e:
                    cj3 = 0
                contract_item.cj3 = cj3
                contract_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                rkg_list.append(copy.deepcopy(contract_item))
        return rkg_list

    @staticmethod
    def _parse_daily_rkg(html_page, date):
        print('第三种解析方法')
        rkg_list = []
        html = etree.HTML(html_page)
        # /html/body/div/table/tbody/tr[939]
        tr_list = html.xpath('/html/body/div/table/tbody/tr')
        mark = 0  # 判断是何种数据的标记
        for tr in tr_list:
            # 解析出第一个td
            flag = tr.xpath('./td[1]/text()')
            if not flag:
                flag = tr.xpath('./td[1]/b/text()')[0].strip()
            else:
                flag = flag[0].strip()

            r1 = re.search(r'品种.*[A-Z]+', flag)
            r2 = re.search(r'合约.*\w+\d+', flag)
            r3 = re.search(r'名次', flag)
            r4 = re.search(r'合计', flag)
            if r1:  # 以下信息是品种信息
                mark = 0
                variety_item = Item()
                name = re.search(r'[A-Z]+', flag).group()
                variety_item.date = date
                variety_item.name = name
                variety_item.contract = ''
                continue

            if r2:  # 以下信息是合约信息
                mark = 1
                contract_item = Item()
                name = re.search(r'\w+\d+', flag).group()
                contract_item.date = date
                contract_item.name = name[:2]
                contract_item.contract = name[2:]
                continue

            if r3:  # 表头
                continue
            if r4:  # 合计
                if mark == 0:
                    variety_item.rank = 'total'  # 名次
                    variety_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                    variety_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                    variety_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                    variety_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                    source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单
                    try:
                        cj2 = int(source_cj2.replace(',', ''))
                    except Exception as e:
                        cj2 = 0
                    variety_item.cj2 = cj2
                    variety_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                    variety_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                    try:
                        cj3 = int(source_cj3.replace(',', ''))
                    except Exception as e:
                        cj3 = 0
                    variety_item.cj3 = cj3
                    variety_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                    rkg_list.append(copy.deepcopy(variety_item))
                if mark == 1:
                    contract_item.rank = 'total'  # 名次
                    contract_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                    contract_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                    contract_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                    contract_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                    source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                    try:
                        cj2 = int(source_cj2.replace(',', ''))
                    except Exception as e:
                        cj2 = 0
                    contract_item.cj2 = cj2
                    contract_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                    contract_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                    source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                    try:
                        cj3 = int(source_cj3.replace(',', ''))
                    except Exception as e:
                        cj3 = 0
                    contract_item.cj3 = cj3
                    contract_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                    rkg_list.append(copy.deepcopy(contract_item))
                continue
            if mark == 0:
                variety_item.rank = tr.xpath('./td[1]/text()')[0].strip()  # 名次
                variety_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                variety_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                variety_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                variety_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                try:
                    cj2 = int(source_cj2.replace(',', ''))
                except Exception as e:
                    cj2 = 0
                variety_item.cj2 = cj2
                variety_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                variety_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                try:
                    cj3 = int(source_cj3.replace(',', ''))
                except Exception as e:
                    cj3 = 0
                variety_item.cj3 = cj3
                variety_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                rkg_list.append(copy.deepcopy(variety_item))
            if mark == 1:
                contract_item.rank = tr.xpath('./td[1]/text()')[0].strip()  # 名次
                contract_item.company1 = tr.xpath('./td[2]/text()')[0].strip()  # 会员简称
                contract_item.cj1 = tr.xpath('./td[3]/text()')[0].strip()  # 成交量（手）
                contract_item.cj1_chg = tr.xpath('./td[4]/text()')[0].strip()  # 成交量变化
                contract_item.company2 = tr.xpath('./td[5]/text()')[0].strip()  # 会员简称2
                source_cj2 = tr.xpath('./td[6]/text()')[0].strip()  # 持买单量
                try:
                    cj2 = source_cj2.replace(',', '')
                except Exception as e:
                    cj2 = 0
                contract_item.cj2 = cj2
                contract_item.cj2_chg = tr.xpath('./td[7]/text()')[0].strip()  # 买单量变化
                contract_item.company3 = tr.xpath('./td[8]/text()')[0].strip()  # 会员简称3
                source_cj3 = tr.xpath('./td[9]/text()')[0].strip()  # 持卖单量
                try:
                    cj3 = source_cj3.replace(',', '')
                except Exception as e:
                    cj3 = 0
                contract_item.cj3 = cj3
                contract_item.cj3_chg = tr.xpath('./td[10]/text()')[0].strip()  # 卖单量变化
                rkg_list.append(copy.deepcopy(contract_item))

        return rkg_list

    def _merge_target_day(self, msg_items, rkg_items, date):
        target_list = []
        # 如果时间是在20100824及之前，买单量与卖单量合计需要自己查询获得
        if date <= '20100824':
            # 查询数据库
            for msg_item in msg_items:
                date = msg_item.date
                name = msg_item.product_name
                contract = msg_item.contract
                # 查询数据库计算总买单量
                b_volume, s_volume = self.writer.calculate_volume(date=date, name=name, contract=contract)
                if not b_volume or not s_volume:
                    continue
                target_item = Item()
                target_item.date = date
                target_item.name = name
                target_item.contract = contract
                target_item.price = msg_item.close_price.replace(',', '')
                target_item.holdings = int(msg_item.open_interest.replace(',', ''))
                target_item.b_volume = b_volume
                target_item.s_volume = s_volume
                target_item.net_holding = target_item.b_volume - target_item.s_volume
                if target_item.holdings == 0:  # 持仓手为0，净持率为0
                    target_item.holding_rate = str(0)
                else:
                    target_item.holding_rate = str((target_item.net_holding / target_item.holdings) * 100) + '%'  # 净持率
                target_list.append(target_item)
        else:
            for msg_item in msg_items:
                for rkg_item in rkg_items:
                    if msg_item.date == rkg_item.date and msg_item.product_name == rkg_item.name and msg_item.contract == rkg_item.contract and rkg_item.rank == 'total':
                        target_item = Item()
                        target_item.date = msg_item.date
                        target_item.name = rkg_item.name
                        target_item.contract = rkg_item.contract
                        target_item.price = msg_item.close_price.replace(',', '')
                        target_item.holdings = int(msg_item.open_interest.replace(',', ''))
                        target_item.b_volume = rkg_item.cj2
                        target_item.s_volume = rkg_item.cj3
                        target_item.net_holding = target_item.b_volume - target_item.s_volume
                        if target_item.holdings == 0:  # 持仓手为0，净持率为0
                            target_item.holding_rate = str(0)
                        else:
                            target_item.holding_rate = str((target_item.net_holding / target_item.holdings) * 100) + '%'  # 净持率
                        target_list.append(target_item)
        return target_list

#
# if __name__ == '__main__':
#     # s = CZCESpider()
#     # s.update_data('20081015')
#     d = DCESpider()
#     d.update_data('20181210')
#     # a = SHFESpider()
#     # a.update_data('20181031')
