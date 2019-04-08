# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

import re
import datetime
from PyQt5.QtWidgets import (
    QLabel, QHBoxLayout, QVBoxLayout, QComboBox,
    QMessageBox, QDateEdit, QPushButton, QSplitter,
    )
from PyQt5.QtCore import Qt, QDate, pyqtSignal

from settings import RESEARCH_LIB, GOODS_LIB, CFFEX_PRODUCT_NAMES, SHFE_PRODUCT_NAMES, DCE_PRODUCT_NAMES, CZCE_PRODUCT_NAMES, BASE_DIR
from windows.ancestor import AncestorWindow, AncestorThread
from utils.generate_time import GenerateTime
from draw.net_holding import MapWidget, TableWidget


class NetHoldingWindow(AncestorWindow):
    def __init__(self, *args, **kwargs):
        super(NetHoldingWindow, self).__init__(*args, **kwargs)
        # """连接数据库"""
        # self.__connect_database()
        """绑定公用属性"""
        self.products = None
        self.db_worker = None
        # 线程是否结束标志位
        self.exchange_lib_thread_over = True
        self.selected_variety_thread_over = True
        self.selected_contract_thread_over = True
        """总布局"""
        vertical_layout = QVBoxLayout()  # 总纵向布局
        top_select = QHBoxLayout()  # 顶部横向条件选择栏
        """交易所选择下拉框"""
        exchange_label = QLabel('选择交易所:')
        self.exchange_lib = QComboBox()
        self.exchange_lib.activated[str].connect(self.exchange_lib_selected)  # 选择交易所调用的方法
        """品种选择下拉框"""
        variety_label = QLabel('选择品种:')
        self.variety_lib = QComboBox()
        self.variety_lib.setMinimumSize(80, 20)
        self.variety_lib.activated[str].connect(self.variety_lib_selected)  # 选择品种所调用的方法
        """合约代码下拉框"""
        contract_label = QLabel('合约代码:')
        self.contract_lib = QComboBox()
        self.contract_lib.activated[str].connect(self.contract_lib_selected)
        """时间选择"""
        begin_time_label = QLabel("起始日期:")
        self.begin_time = QDateEdit(QDate.currentDate().addDays(-2))  # 参数为设置的日期
        self.begin_time.setDisplayFormat('yyyy-MM-dd')  # 时间显示格式
        self.begin_time.setCalendarPopup(True)  # 使用日历选择时间
        end_time_label = QLabel("终止日期:")
        self.end_time = QDateEdit(QDate.currentDate().addDays(-1))  # 参数为设置的日期
        self.end_time.setDisplayFormat('yyyy-MM-dd')  # 时间显示格式
        self.end_time.setCalendarPopup(True)  # 使用日历选择时间
        """确定按钮"""
        self.confirm_button = QPushButton("确定")
        self.confirm_button.clicked.connect(self.confirm)
        """各控件加入水平布局"""
        top_select.addWidget(exchange_label, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.exchange_lib, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(variety_label, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.variety_lib, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(contract_label, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.contract_lib, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(begin_time_label, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.begin_time, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(end_time_label, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.end_time, alignment=Qt.AlignTop | Qt.AlignLeft)
        top_select.addWidget(self.confirm_button, alignment=Qt.AlignTop | Qt.AlignLeft)
        """自定义部件"""
        # 画布
        self.map_widget = MapWidget()
        # 表格,参数为列数
        self.table_widget = TableWidget(7)
        # 设置列名称
        self.table_widget.set_style(header_labels=['日期', '价格', '总持仓', '净多', '净空', '净持仓', '净持率'])
        """创建QSplitter"""
        show_splitter = QSplitter()
        show_splitter.setOrientation(Qt.Vertical)  # 垂直拉伸
        # 加入自定义控件
        show_splitter.addWidget(self.map_widget)
        show_splitter.addWidget(self.table_widget)
        """垂直布局添加布局和部件"""
        # vertical_layout.addLayout(self.title_hlayout)  # 窗口标题水平布局
        vertical_layout.addLayout(top_select)
        vertical_layout.addWidget(show_splitter)
        """设置总布局"""
        self.setLayout(vertical_layout)  # 总布局,只能设置一次

    def fill_init_data(self):
        """填充交易所"""
        self.exchange_lib.clear()
        for lib in RESEARCH_LIB:
            self.exchange_lib.addItem(lib)
        # 获取当前交易所的品种并填充
        self.exchange_lib_selected()

    def fill_contract(self, data):
        """根据品种选择线程执行返回的信号信息填充相应的品种"""
        data.reverse()
        # print("信号槽函数返回的信息", data)
        self.contract_lib.clear()
        for contract in data:
            self.contract_lib.addItem(contract)
        # 当合约填充完毕就解开交易所的选择锁
        self.exchange_lib_thread_over = True
        self.selected_variety_thread_over = True
        # 确定合约，显示时间
        self.contract_lib_selected()

    def change_time_interval(self, data):
        """根据合约选择线程返回的信号改变当前显示的时间"""
        current_date = datetime.datetime.today()
        min_time, max_time = data[0], data[1]
        if not min_time or not max_time:
            return
        # 转为时间类
        min_date = datetime.datetime.strptime(min_time, '%Y%m%d')
        max_date = datetime.datetime.strptime(max_time, '%Y%m%d')
        # 计算天数间隔
        big_time_interval = -(current_date - min_date).days  # 要往前取数，调整为负值
        small_time_interval = -(current_date - max_date).days
        # 设置时间
        self.begin_time.setDate(QDate.currentDate().addDays(big_time_interval))
        self.end_time.setDate(QDate.currentDate().addDays(small_time_interval))
        # 设置完时间就所有锁打开
        self.exchange_lib_thread_over = True
        self.selected_variety_thread_over = True
        self.selected_contract_thread_over = True
        # 设置完时间就设置确认按钮可用
        self.confirm_button.setEnabled(True)

    def exchange_lib_selected(self):
        """
        选择交易所调用的方法，线程执行：
        查询品种并填充
        """
        # 设置确认按钮不可用
        self.confirm_button.setEnabled(False)

        if not self.exchange_lib_thread_over:
            QMessageBox.warning(self, "错误", "您手速太快啦...", QMessageBox.Ok)
            return
        lib = self.exchange_lib.currentText()
        self.variety_lib.clear()
        if lib == "中国金融期货交易所":
            key = 'cffex'
            self.db_worker = self.cffex_getter
            self.products = CFFEX_PRODUCT_NAMES
        elif lib == "上海期货交易所":
            key = 'shfe'
            self.db_worker = self.shfe_getter
            self.products = SHFE_PRODUCT_NAMES
        elif lib == "大连商品交易所":
            key = 'dce'
            self.db_worker = self.dce_getter
            self.products = DCE_PRODUCT_NAMES
        elif lib == "郑州商品交易所":
            key = 'czce'
            self.db_worker = self.czce_getter
            self.products = CZCE_PRODUCT_NAMES
        else:
            self.db_worker = None
            self.products = None
            QMessageBox.information(self, '错误', "没有此项交易所..", QMessageBox.Ok)
            return
        for variety in GOODS_LIB.get(key):
            self.variety_lib.addItem(variety)

        self.exchange_lib_thread_over = False  # 选择完交易所就锁上不能再选择
        # 交易所确定，确定品种
        self.variety_lib_selected()

    def get_variety_en(self, variety):
        """获取相应的品种英文代号"""
        # 获取品种英文名称
        if not self.products:
            # print("没有品种库：", self.products)
            QMessageBox.warning(self, "错误", "内部发生未知错误")
            return ''
        return self.products.get(variety)

    def variety_lib_selected(self):
        """
        选择品种所调用的方法，线程执行：
        查询合约并填充合约
        """
        # 设置确认按钮不可用
        self.confirm_button.setEnabled(False)
        if not self.selected_variety_thread_over:
            QMessageBox.warning(self, "错误", "您手速太快啦...", QMessageBox.Ok)
            return
        variety = self.variety_lib.currentText()
        en_variety = self.get_variety_en(variety)
        # !!!(务必将线程绑在self对象上，否则无法实现多线程)
        self.selected_variety_thread_over = False
        if self.db_worker:
            self.variety_selected_thread = VarietySelectedThread(db_worker=self.db_worker, variety=en_variety)
            self.variety_selected_thread.result_signal.connect(self.fill_contract)
            self.variety_selected_thread.start()

    def contract_lib_selected(self):
        """
        选择了合约代码，查询本合约代码的时间周期，线程执行：
        信号返回结果，设置时间
        """
        # 设置确认按钮不可用
        self.confirm_button.setEnabled(False)

        if not self.selected_contract_thread_over:
            QMessageBox.warning(self, "错误", "您手速太快啦...", QMessageBox.Ok)
            return
        variety = self.variety_lib.currentText()
        variety_en = self.get_variety_en(variety)
        contract = self.contract_lib.currentText()
        # print("选择了合约代码:\n当前交易所:{}\n当前品种：{}\n当前合约:{}".format(lib, variety_en, contract))
        if self.db_worker:
            self.contract_selected_thread = ContractSelectedThread(db_worker=self.db_worker, variety=variety_en, contract=contract)
            self.contract_selected_thread.result_signal.connect(self.change_time_interval)
            self.contract_selected_thread.start()

    def confirm(self):
        """
        条件选择好了确认查询相应的数据进行图表生成，线程执行:
        信号返回结果设置状态
        """
        # 定时器计时
        self.timer.start(1000)
        # 设置确认按钮不可用，提示消息改为提交成功
        self.confirm_button.setText("提交成功")
        self.confirm_button.setEnabled(False)
        # 清除原图表并设置样式
        self.map_widget.delete()
        self.table_widget.clear()
        self.table_widget.set_style(header_labels=['日期', '价格', '总持仓', '净多', '净空', '净持仓', '净持率'])
        # 获取各数据
        lib = self.exchange_lib.currentText()
        variety = self.variety_lib.currentText()
        variety_en = self.get_variety_en(variety)
        contract = self.contract_lib.currentText()
        begin_time = re.sub('-', '', str(self.begin_time.date().toPyDate()))
        end_time = re.sub('-', '', str(self.end_time.date().toPyDate()))
        # print("确认提交:\n当前交易所：{}\n当前品种：{}：{}\n当前合约：{}\n起始时间：{}\n终止时间：{}".format(lib, variety, variety_en, contract, begin_time, end_time))
        # 转成可计算的datetime.datetime时间对象
        begin_time_datetime_cls = datetime.datetime.strptime(begin_time, "%Y%m%d")
        end_time_datetime_cls = datetime.datetime.strptime(end_time, "%Y%m%d")
        # 起止时间应早于终止时间
        if begin_time_datetime_cls >= end_time_datetime_cls:
            QMessageBox.warning(self, "错误", "起止时间应早于终止时间", QMessageBox.Ok)
            self.message_signal.emit("时间选择有误！")
            self.timer.stop()
            self.cost_time = 0
            # 设置确认按钮可用
            self.confirm_button.setText("确定")
            self.confirm_button.setEnabled(True)
            return
        # 生成时间可迭代对象
        iterator = GenerateTime(begin=begin_time_datetime_cls, end=end_time_datetime_cls)
        # 线程执行查询目标数据
        self.confirm_thread = ConfirmQueryThread(
                            db_worker=self.db_worker,
                            variety=variety_en,
                            contract=contract,
                            iterator=iterator,
                            lib=lib
                        )
        self.confirm_thread.result_signal.connect(self.draw_map_table)
        self.confirm_thread.process_signal.connect(self.show_process)
        self.confirm_thread.start()

    def show_process(self, data):
        self.process_signal.emit(data)

    def draw_map_table(self, data):
        prices = data.get("prices")
        rates = data.get("rates")
        times = data.get("times")
        items = data.get("items")
        message = data.get("message")
        # 设置样式
        self.map_widget.set_style()
        # 重新设置标题
        self.map_widget.axes1.set_title(message + "合约价格-净持率趋势图")
        # 进行画图
        self.map_widget.net_holding_map(y_left=prices, y_right=rates, x=times)
        # 填充表格
        self.table_widget.net_holding_table(items)
        # 结果执行完程序
        self.message_signal.emit("处理完毕，生成图表成功！")
        self.timer.stop()
        self.cost_time = 0
        # 设置确认按钮可用
        self.confirm_button.setText("确定")
        self.confirm_button.setEnabled(True)


class VarietySelectedThread(AncestorThread):
    """查询品种合约线程"""
    def __init__(self, db_worker, variety):
        super(VarietySelectedThread, self).__init__()
        self.db_worker = db_worker
        self.variety = variety

    def run(self):
        """"选择品种后查询所有合约通过信号传出"""
        data = self.db_worker.get_contracts(good=self.variety)
        # print('data:', data, self.db_worker)
        self.result_signal.emit(data)
        self.db_worker.close()  # 断开连接，不然下次线程来就无法连接


class ContractSelectedThread(AncestorThread):
    """查询当前合约的时间区间线程"""
    def __init__(self, db_worker, variety, contract):
        super(ContractSelectedThread, self).__init__()
        self.db_worker = db_worker
        self.variety = variety
        self.contract = contract

    def run(self):
        # 查询合约时间区间，返回最小及最大的时间
        min_time, max_time = self.db_worker.get_times(good=self.variety, contract=self.contract)
        # 通过信号返回结果
        self.result_signal.emit([min_time, max_time])
        # 断开数据库连接
        self.db_worker.close()


class ConfirmQueryThread(AncestorThread):
    """确认查询线程，结果通过信号传出"""
    result_signal = pyqtSignal(dict)

    def __init__(self, db_worker, variety, contract, iterator, lib):
        super(ConfirmQueryThread, self).__init__()
        self.db_worker = db_worker
        self.variety = variety
        self.contract = contract
        self.time_iterator = iterator.generate_time()
        self.time_length = iterator.length()
        self.lib = lib

    def run(self):
        """查询数据库，信号返回相应的结果"""
        prices = []
        rates = []
        times = []
        items = []
        for index, date in enumerate(self.time_iterator):
            data_item = self.db_worker.net_holding(date=date, name=self.variety, contract=self.contract)
            if not data_item:
                continue
            prices.append(int(float(data_item.price)))
            rates.append(float(data_item.holding_rate.strip('%')))
            times.append(data_item.date)
            items.append(data_item)
            # 设置进度条
            self.process_signal.emit(["处理进度:", index + 1, self.time_length])
        # prices, rates, times, items = self.db_worker.get_sources(time_intervals=self.time_iterator, good=self.variety, contract=self.contract)
        self.result_signal.emit({"prices": prices, "rates": rates, "times": times, "items": items, "message": self.lib + self.variety + self.contract})
        # 断开数据库连接
        self.db_worker.close()
