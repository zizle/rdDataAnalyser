# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import os
import datetime
import time
import re
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtCore import pyqtSignal

from utils.generate_time import GenerateTime
from settings import BASE_DIR, GOODS_LIB, CFFEX_PRODUCT_NAMES, SHFE_PRODUCT_NAMES, DCE_PRODUCT_NAMES, CZCE_PRODUCT_NAMES, FILE_DIR
from update import SHFESpider, DCESpider, CFFEXSpider, CZCESpider
from sql.shfe import SHFEWorker
from sql.dce import DCEWorker
from sql.cffex import CFFEXWorker
from sql.czce import CZCEWorker
from windows.ancestor import AncestorWindow, AncestorThread, DialogInput


class SysWindow(AncestorWindow):
    def __init__(self, *args, **kwargs):
        super(SysWindow, self).__init__(*args, **kwargs)

    """更新品种价格指数据库表"""
    def update_price_index(self):
        self.timer.start(1000)
        self.process_signal.emit(["开始更新:", 0, 100])  # 进度显示0%
        # 数据库对象字典
        db_works = {
            "shfe": self.shfe_getter,
            "dce": self.dce_getter,
            "cffex": self.cffex_getter,
            "czce": self.czce_getter
        }
        self.update_variety_price_thread = UpdatePriceIndexThread(db_works=db_works)
        self.update_variety_price_thread.process_signal.connect(self.update_price_index_process)
        self.update_variety_price_thread.result_signal.connect(self.update_price_index_finish)
        self.update_variety_price_thread.start()

    def update_price_index_process(self, data):
        self.process_signal.emit(data)

    def update_price_index_finish(self):
        QMessageBox.information(self, "完成", "更新品种价格指数数据表完成！", QMessageBox.Ok)
        self.timer.stop()
        self.message_signal.emit("更新品种价格指数数据表完成！耗时{}秒".format(self.cost_time))
        self.cost_time = 0

    """查看当前系统最新数据时间"""
    def view_lasted(self):
        """查看最新数据, 线程执行"""
        self.timer.start(1000)
        self.process_signal.emit(["正在查询", 0, 4])
        db_works = {
            "shfe": self.shfe_getter,
            "dce": self.dce_getter,
            "cffex": self.cffex_getter,
            "czce": self.czce_getter
        }
        self.view_thread = ViewLastedThread(works=db_works)
        self.view_thread.result_signal.connect(self.view_lasted_thread_result)
        self.view_thread.process_signal.connect(self.view_lasted_thread_process)
        self.view_thread.run()

    def view_lasted_thread_result(self, data):
        self.timer.stop()
        self.cost_time = 0
        QMessageBox.information(self, '最新数据时间',
                                '上海期货交易所：' + data.get("shfe") +
                                '\n大连商品交易所：' + data.get("dce") +
                                '\n中金期货交易所：' + data.get("cffex") +
                                '\n郑州商品交易所：' + data.get("czce")
                                , QMessageBox.Ok)

    def view_lasted_thread_process(self, data):
        self.process_signal.emit(data)

    """一键更新"""
    def one_key_update(self):
        """一键更新, 线程执行"""
        self.timer.start(1000)
        self.process_signal.emit(["开始更新:", 0, 100])  # 进度显示0%
        # 数据库对象字典
        db_works = {
            "shfe": self.shfe_getter,
            "dce": self.dce_getter,
            "cffex": self.cffex_getter,
            "czce": self.czce_getter
        }
        self.one_key_update_thread = OneKeyUpdateThread(db_works=db_works, window=self)
        self.one_key_update_thread.process_signal.connect(self.one_key_update_process)
        self.one_key_update_thread.result_signal.connect(self.show_one_key_update_message)
        self.one_key_update_thread.error_signal.connect(self.error_show)
        self.one_key_update_thread.start()

    def error_show(self, data):
        QMessageBox.warning(self, '出错!', '{}更新出错！\n错误信息：{}'.format(data[0], data[1]))
        self.timer.stop()
        self.cost_time = 0

    def one_key_update_process(self, data):
        self.process_signal.emit(data)

    def show_one_key_update_message(self, data):
        QMessageBox.information(self, "完成", "一键更新数据完毕！", QMessageBox.Ok)
        self.timer.stop()
        self.cost_time = 0

    """指定更新"""
    def point_update(self):
        # 获取输入的开始和结束时间
        dialog_input = DialogInput(text1='请输入起始日期(格式年月日如：20181020):', text2='请输入结束日期(格式年月日如：20181030):', title='时间输入')
        dialog_input.exec_()
        begin, end, lib = dialog_input.get_data()
        # 转为datetime.datetime实例
        try:
            begin_date = datetime.datetime.strptime(begin, "%Y%m%d")
            end_date = datetime.datetime.strptime(end, "%Y%m%d")
        except Exception as e:
            QMessageBox.warning(self, "错误", "时间格式输入错误！", QMessageBox.Ok)
            return
        spider = None
        if lib == "中国金融期货交易所":
            spider = CFFEXSpider()
        elif lib == "上海期货交易所":
            spider = SHFESpider()
        elif lib == "大连商品交易所":
            spider = DCESpider()
        elif lib == "郑州商品交易所":
            spider = CZCESpider()
        else:
            QMessageBox.warning(self,"错误", "交易所选择有误!", QMessageBox.Ok)
        if not spider:
            QMessageBox.warning(self, "错误", "交易所选择有误!", QMessageBox.Ok)
        # 开启计时器
        self.timer.start(1000)
        self.process_signal.emit(["开始更新:", 1, 100])  # 进度显示1%
        # 时间迭代器
        time_iterator = GenerateTime(begin=begin_date, end=end_date)
        # 线程执行更新
        self.point_update_thread = PointUpdateThread(spider=spider, iterator=time_iterator, lib=lib)
        self.point_update_thread.result_signal.connect(self.point_update_over)
        self.point_update_thread.process_signal.connect(self.point_update_process)
        self.point_update_thread.error_signal.connect(self.point_update_error)
        self.point_update_thread.start()

    def point_update_error(self, data):
        QMessageBox.warning(self, '出错!', '{}更新出错！\n错误信息：{}'.format(data[0], data[1]))
        self.timer.stop()
        self.cost_time = 0

    def point_update_over(self, data):
        QMessageBox.information(self, "完成", "指定更新"+data[0]+"数据完毕！", QMessageBox.Ok)
        self.timer.stop()
        self.cost_time = 0

    def point_update_process(self, data):
        self.process_signal.emit(data)

    """添加品种"""
    def add_product(self):
        # 弹出输入框
        dialog_input = DialogInput(text1="请输入商品名称：", text2="请输入商品交易代码(英文代号)", title="信息输入")
        dialog_input.exec_()
        product_name, product_code, lib = dialog_input.get_data()
        if not lib:
            QMessageBox.information(self, "错误", "请选择交易所...")
            return
        if not product_name:
            QMessageBox.information(self, "错误", "请输入商品名称...")
            return
        if not product_code:
            QMessageBox.information(self, "错误", "请输入商品交易代码...")
            return
        add_p = re.sub(r" ", "", product_name)
        add_c = re.sub(r" ", "", product_code)
        if lib == "中国金融期货交易所":
            # 转为大写
            add_p = add_p.upper()
            add_c = add_c.upper()
            # 保存
            if add_p not in GOODS_LIB["cffex"]:
                GOODS_LIB["cffex"].append(add_p)
                CFFEX_PRODUCT_NAMES[add_p] = add_c
                file_name_p = os.path.join(FILE_DIR, "goodsLib.dat")
                file_name_c = os.path.join(FILE_DIR, "cffex.p.dat")
                with open(file_name_p, 'w', encoding="utf-8") as f:
                    f.write(str(GOODS_LIB))
                with open(file_name_c, "w", encoding="utf-8") as f:
                    f.write(str(CFFEX_PRODUCT_NAMES))
                QMessageBox.information(self, lib, "新增品种" + add_p + "成功！", QMessageBox.Ok)
            else:
                QMessageBox.information(self, lib, "品种" + add_p + "已存在，无需添加！", QMessageBox.Ok)

        elif lib == "上海期货交易所":
            add_p = add_p
            add_c = add_c.lower()
            if add_p not in GOODS_LIB["shfe"]:
                GOODS_LIB["shfe"].append(add_p)
                SHFE_PRODUCT_NAMES[add_p] = add_c
                file_name_p = os.path.join(FILE_DIR, "goodsLib.dat")
                file_name_c = os.path.join(FILE_DIR, "shfe.p.dat")
                with open(file_name_p, 'w', encoding="utf-8") as f:
                    f.write(str(GOODS_LIB))
                with open(file_name_c, "w", encoding="utf-8") as f:
                    f.write(str(SHFE_PRODUCT_NAMES))
                QMessageBox.information(self, lib, "新增品种" + add_p + "成功！", QMessageBox.Ok)
            else:
                QMessageBox.information(self, lib, "品种" + add_p + "已存在，无需添加！", QMessageBox.Ok)

        elif lib == "大连商品交易所":
            add_p = add_p
            add_c = add_c.lower()
            if add_p not in GOODS_LIB["dce"]:
                GOODS_LIB["dce"].append(add_p)
                DCE_PRODUCT_NAMES[add_p] = add_c
                subtotal = add_p + "小计"
                DCE_PRODUCT_NAMES[subtotal] = add_c
                file_name_p = os.path.join(FILE_DIR, "goodsLib.dat")
                file_name_c = os.path.join(FILE_DIR, "dce.p.dat")
                with open(file_name_p, 'w', encoding="utf-8") as f:
                    f.write(str(GOODS_LIB))
                with open(file_name_c, "w", encoding="utf-8") as f:
                    f.write(str(DCE_PRODUCT_NAMES))
                QMessageBox.information(self, lib, "新增品种" + add_p + "成功！", QMessageBox.Ok)
            else:
                QMessageBox.information(self, lib, "品种" + add_p + "已存在，无需添加！", QMessageBox.Ok)

        elif lib == "郑州商品交易所":
            add_p = add_p
            add_c = add_c.lower()
            if add_p not in GOODS_LIB["czce"]:
                GOODS_LIB["czce"].append(add_p)
                CZCE_PRODUCT_NAMES[add_p] = add_c
                file_name_p = os.path.join(FILE_DIR, "goodsLib.dat")
                file_name_c = os.path.join(FILE_DIR, "czce.p.dat")
                with open(file_name_p, 'w', encoding="utf-8") as f:
                    f.write(str(GOODS_LIB))
                with open(file_name_c, "w", encoding="utf-8") as f:
                    f.write(str(CZCE_PRODUCT_NAMES))
                QMessageBox.information(self, lib, "新增品种" + add_p + "成功！", QMessageBox.Ok)
            else:
                QMessageBox.information(self, lib, "品种" + add_p + "已存在，无需添加！", QMessageBox.Ok)
        else:
            QMessageBox.warning(self, '错误', '请确认选择的交易所是否正确...')


class ViewLastedThread(AncestorThread):
    result_signal = pyqtSignal(dict)

    def __init__(self, works):
        super(ViewLastedThread, self).__init__()
        self.works = works

    def run(self):
        shfe_lasted = self.works.get("shfe").get_latest()
        self.process_signal.emit(["正在查询", 1, 4])
        cffex_lasted = self.works.get("cffex").get_latest()
        self.process_signal.emit(["正在查询", 2, 4])
        dce_lasted = self.works.get("dce").get_latest()
        self.process_signal.emit(["正在查询", 3, 4])
        czce_lasted = self.works.get("czce").get_latest()
        self.process_signal.emit(["正在查询", 4, 4])
        for getter in self.works.values():
            getter.close()
        self.result_signal.emit({"shfe": shfe_lasted, "cffex": cffex_lasted, "dce": dce_lasted, "czce": czce_lasted})


class OneKeyUpdateThread(AncestorThread):
    """一键更新线程"""
    error_signal = pyqtSignal(list)

    def __init__(self, db_works, window):
        super(OneKeyUpdateThread, self).__init__()
        self.works = db_works
        self.window = window

    def run(self):
        """更新"""
        shfe_getter = self.works.get("shfe")
        dce_getter = self.works.get("dce")
        cffex_getter = self.works.get("cffex")
        czce_getter = self.works.get("czce")

        # 获取当前系统数据最新时间
        shfe_latest = shfe_getter.get_latest()
        dce_latest = dce_getter.get_latest()
        cffex_latest = cffex_getter.get_latest()
        czce_latest = czce_getter.get_latest()
        # 关闭数据库连接
        for worker in self.works.values():
            worker.close()
        # 结束为昨天的时间
        # end_date = QDate.currentDate().addDays(-1).toPyDate()  # <class 'datetime.date'>
        end_date = datetime.datetime.now() + datetime.timedelta(days=-1)
        # end_date = datetime.date.strftime(current_date, '%Y%m%d')  # string
        # 生成时间
        # end = datetime.datetime.strptime(end_date, '%Y%m%d')
        shfe_begin_date = datetime.datetime.strptime(shfe_latest, '%Y%m%d')
        dce_begin_date = datetime.datetime.strptime(dce_latest, '%Y%m%d')
        cffex_begin_date = datetime.datetime.strptime(cffex_latest, '%Y%m%d')
        czce_begin_date = datetime.datetime.strptime(czce_latest, '%Y%m%d')

        """更新上期所"""
        shfe_spider = SHFESpider()
        shfe_time = GenerateTime(begin=shfe_begin_date, end=end_date)   # 时间生成器
        shfe_length = shfe_time.length()
        for index, date in enumerate(shfe_time.generate_time()):
            if shfe_getter.exist_today(date):
                print('{}的数据已经存在，无需重复更新！'.format(date))
                continue
            try:
                shfe_spider.update_data(date)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                log_file = os.path.join(BASE_DIR, "log/miss.log")
                with open(log_file, 'a+', encoding='utf-8') as f:
                    f.write('\n{}  一键更新上期所{}出现错误！{}'.format(ct, date, e))
                self.error_signal.emit([date, "更新上期所出错\n"+str(e)])
                break
            self.process_signal.emit(["更新上期所:", index + 1, shfe_length])

        """更新大商所"""
        dce_spider = DCESpider()
        dce_time = GenerateTime(begin=dce_begin_date, end=end_date)
        dce_length = dce_time.length()
        for index, date in enumerate(dce_time.generate_time()):
            if dce_getter.exist_today(date):
                print('{}的数据已经存在，无需重复更新！'.format(date))
                continue
            try:
                dce_spider.update_data(date)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  一键更新大商所{}出现错误！{}'.format(ct, date, e))
                self.error_signal.emit([date, "更新大商所出错\n" + str(e)])
                break
            self.process_signal.emit(["更新大商所:", index + 1, dce_length])

        # 更新中金所
        cffex_spider = CFFEXSpider()
        cffex_time = GenerateTime(begin=cffex_begin_date,end=end_date)
        cffex_length = cffex_time.length()
        for index, date in enumerate(cffex_time.generate_time()):
            if cffex_getter.exist_today(date):
                print('{}的数据已经存在，无需重复更新！'.format(date))
                continue
            try:
                cffex_spider.update_data(date)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  一键更新中金所{}出现错误！{}'.format(ct, date, e))
                self.error_signal.emit([date, "更新中金所出错\n" + str(e)])
                break
            self.process_signal.emit(["更新中金所:", index+1, cffex_length])

        # 更新郑商所
        czce_spider = CZCESpider()
        czce_time = GenerateTime(begin=czce_begin_date, end=end_date)
        czce_length = czce_time.length()
        for index,date in enumerate(czce_time.generate_time()):
            if czce_getter.exist_today(date):
                print('{}的数据已经存在，无需重复更新！'.format(date))
                continue
            try:
                czce_spider.update_data(date)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  一键更新郑商所所{}出现错误！{}'.format(ct, date, e))
                self.error_signal.emit([date, "更新郑商所出错\n" + str(e)])
                break
            self.process_signal.emit(["更新郑商所:", index+1, czce_length])
        # 关闭数据库连接
        for worker in self.works.values():
            worker.close()
        # 全部更新完毕发送线程执行完毕的信号
        self.result_signal.emit([])


class PointUpdateThread(AncestorThread):
    error_signal = pyqtSignal(list)

    def __init__(self, spider, iterator,lib):
        super(PointUpdateThread, self).__init__()
        self.spider = spider
        self.time_iterator = iterator
        self.lib = lib

    def run(self):
        time_length = self.time_iterator.length()
        for index, date in enumerate(self.time_iterator.generate_time()):
            try:
                self.spider.update_data(date)
            except Exception as e:
                ct = time.strftime('%Y.%m.%d %H:%M:%S', time.localtime(time.time()))
                with open('log/miss.log', 'a+', encoding='utf-8') as f:
                    f.write('\n{}  指定更新{}{}出现错误！{}'.format(ct, self.lib, date, e))
                self.error_signal.emit([date, "指定更新" + self.lib + "出错\n" + str(e)])
                break
            self.process_signal.emit(["正在更新:", index+1, time_length])
        self.result_signal.emit([self.lib])


class UpdatePriceIndexThread(AncestorThread):
    """更新品种权重价格指数数据库表"""
    def __init__(self, db_works):
        super(UpdatePriceIndexThread, self).__init__()
        self.works = db_works

    def run(self):
        # 关闭数据库连接
        for worker in self.works.values():
            worker.close()
        # 当前时间
        now = datetime.datetime.today()
        now_str = datetime.datetime.strftime(now, "%Y%m%d")
        now_date = datetime.datetime.strptime(now_str, "%Y%m%d")
        """遍历更新"""
        for worker in self.works.values():
            # 将时间转为时间类
            work_first = worker.variety_price_table_start()
            first_date = datetime.datetime.strptime(work_first, "%Y%m%d")
            if first_date == now_date:
                self.result_signal.emit([])
                return
            # 生成时间迭代器
            work_time = GenerateTime(first_date, now_date)
            time_iterator = work_time.generate_time()
            time_length = work_time.length()
            # 遍历时间更新数据
            for index, date in enumerate(time_iterator):
                worker.make_variety_price_table(date)
                # 设置进度条
                if isinstance(worker, SHFEWorker):
                    self.process_signal.emit(["正在更新上期所价格指数:", index + 1, time_length])
                elif isinstance(worker, DCEWorker):
                    self.process_signal.emit(["正在更新大商所价格指数:", index + 1, time_length])
                elif isinstance(worker, CFFEXWorker):
                    self.process_signal.emit(["正在更新中金所价格指数:", index + 1, time_length])
                elif isinstance(worker, CZCEWorker):
                    self.process_signal.emit(["正在更新郑商所价格指数:", index + 1, time_length])
        for worker in self.works.values():
            worker.close()
        self.result_signal.emit([])
