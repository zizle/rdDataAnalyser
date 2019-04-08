# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import datetime
import re
from collections import OrderedDict
from PyQt5.QtWidgets import (
    QVBoxLayout, QHBoxLayout, QLabel, QComboBox, QSplitter, QDateEdit, QPushButton, QMessageBox
)
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWebChannel import QWebChannel
from PyQt5.QtCore import Qt, QDate, pyqtSignal, QUrl

from windows.ancestor import AncestorWindow, AncestorThread
from draw.variety_price import MapWidget, TableWidget
from settings import RESEARCH_LIB, GOODS_LIB, SHFE_PRODUCT_NAMES, CFFEX_PRODUCT_NAMES, DCE_PRODUCT_NAMES, CZCE_PRODUCT_NAMES
from utils.generate_time import GenerateTime
from widgets import ToolWidget


class VarietyPriceWindow(AncestorWindow):
    def __init__(self, *args, **kwargs):
        super(VarietyPriceWindow, self).__init__(*args, **kwargs)
        """绑定公用属性"""
        self.products = None
        self.db_worker = None
        # 线程是否结束标志位
        self.exchange_lib_thread_over = True
        self.selected_variety_thread_over = True
        """总布局"""
        self.vertical_layout = QVBoxLayout()  # 总纵向布局
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
        """时间选择"""
        begin_time_label = QLabel("起始日期:")
        self.begin_time = QDateEdit(QDate.currentDate().addDays(-2))  # 参数为设置的日期
        self.begin_time.setDisplayFormat('yyyy-MM-dd')  # 时间显示格式
        self.begin_time.setCalendarPopup(True)  # 使用日历选择时间
        end_time_label = QLabel("终止日期:")
        self.end_time = QDateEdit(QDate.currentDate().addDays(-1))  # 参数为设置的日期
        self.end_time.setDisplayFormat('yyyy-MM-dd')  # 时间显示格式
        self.end_time.setCalendarPopup(True)  # 使用日历选择时间
        """确认按钮"""
        self.confirm_button = QPushButton("确定")
        self.confirm_button.clicked.connect(self.confirm)

        """水平布局添加控件"""
        top_select.addWidget(exchange_label)
        top_select.addWidget(self.exchange_lib)
        top_select.addWidget(variety_label)
        top_select.addWidget(self.variety_lib)
        top_select.addWidget(begin_time_label)
        top_select.addWidget(self.begin_time)
        top_select.addWidget(end_time_label)
        top_select.addWidget(self.end_time)
        top_select.addWidget(self.confirm_button)
        """自定义部件"""
        # 工具栏
        self.tools = ToolWidget()
        tool_btn = QPushButton("季节图表")
        self.tool_view_all = QPushButton("返回总览")
        self.tool_view_all.setEnabled(False)
        self.tool_view_all.clicked.connect(self.return_view_all)
        self.tools.addTool(tool_btn)
        self.tools.addTool(self.tool_view_all)
        self.tools.addSpacer()
        tool_btn.clicked.connect(self.season_table)
        # 画布
        self.map_widget = MapWidget()
        # 表格
        self.table_widget = TableWidget(4)
        # 设置格式
        self.table_widget.set_style(header_labels=['日期', '价格', '成交量合计', '持仓量合计'])
        """创建QSplitter"""
        self.show_splitter = QSplitter()
        self.show_splitter.setOrientation(Qt.Vertical)  # 垂直拉伸
        # 加入自定义控件
        self.show_splitter.addWidget(self.map_widget)
        self.show_splitter.addWidget(self.table_widget)
        """垂直布局添加布局和控件并设置到窗口"""
        self.vertical_layout.addLayout(top_select)
        self.vertical_layout.addWidget(self.tools)
        self.vertical_layout.addWidget(self.show_splitter)
        # 加入控件
        self.web_view = QWebEngineView()
        self.web_view.load(QUrl("file:///static/html/variety_price.html"))
        self.show_splitter.addWidget(self.web_view)
        self.web_view.hide()  # 隐藏，刚开始不可见
        """js交互通道"""
        web_channel = QWebChannel(self.web_view.page())
        self.web_view.page().setWebChannel(web_channel)  # 网页设置信息通道
        web_channel.registerObject("season_table", self.tools)

        self.setLayout(self.vertical_layout)

    def fill_init_data(self):
        """填充交易所"""
        self.exchange_lib.clear()
        for lib in RESEARCH_LIB:
            self.exchange_lib.addItem(lib)
        # 获取当前交易所的品种并填充
        self.exchange_lib_selected()

    def exchange_lib_selected(self):
        """交易所选择"""
        if not self.exchange_lib_thread_over:
            QMessageBox.information(self, "错误", "您的手速太快啦...", QMessageBox.Ok)
            return
        self.confirm_button.setEnabled(False)
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

        # 关闭交易所选择
        self.exchange_lib_thread_over = False
        # 品种选择
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
        查询当前品种上市的日期
        """
        # 设置确认按钮不可用
        self.confirm_button.setEnabled(False)
        if not self.selected_variety_thread_over:
            QMessageBox.warning(self, "错误", "您手速太快啦...", QMessageBox.Ok)
            return
        # 状态显示
        variety = self.variety_lib.currentText()
        self.message_signal.emit("正在查询"+variety+"的数据时间区间...")
        en_variety = self.get_variety_en(variety)
        self.selected_variety_thread_over = False  # 设置品种选择关闭
        if self.db_worker:
            self.variety_selected_thread = VarietySelectedThread(db_worker=self.db_worker, variety=en_variety)
            self.variety_selected_thread.result_signal.connect(self.change_time_interval)
            self.variety_selected_thread.start()

    def change_time_interval(self, data):
        """根据品种选择线程的信号改变当前时间显示区间"""
        current_date = datetime.datetime.today()
        min_date = datetime.datetime.strptime(data[0], "%Y%m%d")
        max_date = datetime.datetime.strptime(data[1], "%Y%m%d")
        # 计算天数间隔
        big_time_interval = -(current_date - min_date).days  # 要往前取数，调整为负值
        small_time_interval = -(current_date - max_date).days
        # 设置时间
        self.begin_time.setDate(QDate.currentDate().addDays(big_time_interval))
        self.end_time.setDate(QDate.currentDate().addDays(small_time_interval))
        self.confirm_button.setEnabled(True)  # 设置提交按钮为可用
        # 设置完时间就所有锁打开
        self.exchange_lib_thread_over = True
        self.selected_variety_thread_over = True
        # 状态显示时间区间查询完毕
        self.message_signal.emit("时间区间查询完毕！")

    def clear_map_table(self):
        self.map_widget.delete()

    def confirm(self):
        """确认按钮点击"""
        # 开启定时器
        self.timer.start(1000)
        # 改变确认按钮的状态
        self.confirm_button.setText("提交成功")
        self.confirm_button.setEnabled(False)
        # 清除原图表并设置样式
        self.map_widget.delete()
        self.table_widget.clear()
        self.table_widget.set_style(header_labels=['日期', '价格', '成交量合计', '持仓量合计'])

        # 根据时间段取得时间生成器类，查询相关数据，计算结果，返回时间列表及对应的价格列表
        lib = self.exchange_lib.currentText()
        variety = self.variety_lib.currentText()
        variety_en = self.get_variety_en(variety)
        begin_time = re.sub('-', '', str(self.begin_time.date().toPyDate()))
        end_time = re.sub('-', '', str(self.end_time.date().toPyDate()))
        print("确认提交:\n当前交易所：{}\n当前品种：{},英文{}\n起始时间：{}\n终止时间：{}\n".format(lib, variety,variety_en, begin_time, end_time))
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
        # 生成时间器对象
        iterator_cls = GenerateTime(begin=begin_time_datetime_cls, end=end_time_datetime_cls)
        print("执行线程")
        if self.db_worker:
            # 线程执行查询目标数据
            self.confirm_thread = ConfirmQueryThread(db_worker=self.db_worker, iterator=iterator_cls, variety=variety_en, lib=lib)
            self.confirm_thread.result_signal.connect(self.draw_map_table)
            self.confirm_thread.process_signal.connect(self.show_process)
            self.confirm_thread.start()

    def show_process(self, data):
        self.process_signal.emit(data)

    def draw_map_table(self, data):
        """数据展示"""
        print("结果", data)
        # 设置样式
        self.map_widget.set_style()
        # 画图
        x_data = []
        y_data = []
        tuple_data_list = data.get("data")
        message = data.get("message")
        for item in tuple_data_list:
            x_data.append(item[0])
            y_data.append(item[1])
        self.map_widget.axes.set_title(message)
        self.map_widget.map(x_data=x_data, y_data=y_data)
        # 表格数据填充
        self.table_widget.table(data=tuple_data_list)
        self.message_signal.emit("处理完毕，生成图表成功！")
        self.timer.stop()
        self.cost_time = 0
        self.confirm_button.setText("确定")
        self.confirm_button.setEnabled(True)

    def season_table(self):
        """处理展示季节图表"""
        # 获取表格的行数和列数 col-列，row-行
        col_count = self.table_widget.columnCount()
        row_count = self.table_widget.rowCount()
        if not row_count:
            return
        # 隐藏与可见的处理
        self.confirm_button.setEnabled(False)
        self.map_widget.hide()
        self.table_widget.hide()
        self.web_view.show()
        self.tool_view_all.setEnabled(True)
        # 获取数据，处理数据，将结果数据传入页面展示
        print("列数", col_count)
        print("行数", row_count)
        # 遍历获取数据
        data = OrderedDict()
        for row in range(row_count):
            date = self.table_widget.item(row_count - 1, 0).text()
            price = self.table_widget.item(row_count - 1, 1).text()
            if date[:4] not in data.keys():
                data[date[:4]] = []
            else:
                data[date[:4]].append([date, price])
            # data.append((date, price))
            # for col in range(col_count):
            #     print(self.table_widget.item(row_count-1, col).text(), ' ', end='')
            row_count -= 1

        import json
        data = json.dumps(data)
        print(data, type(data))
        self.tools.tool_click_signal.emit(data)

    def return_view_all(self):
        # 加入自定义控件
        self.confirm_button.setEnabled(True)
        self.web_view.hide()
        self.map_widget.show()
        self.table_widget.show()
        self.tool_view_all.setEnabled(False)


class ConfirmQueryThread(AncestorThread):
    """确定查询线程"""
    result_signal = pyqtSignal(dict)

    def __init__(self, db_worker, variety, iterator, lib):
        super(ConfirmQueryThread, self).__init__()
        self.db_worker = db_worker
        self.time_iterator = iterator.generate_time()
        self.time_length = iterator.length()
        self.variety = variety
        self.lib = lib

    def run(self):
        """根据时间查询出相应的数据，并计算返回目标结果数据"""
        # (列表嵌套元组的方式返回, [("时间": "价格")]使得数据不会发生串位错乱)
        data = []
        for index, date in enumerate(self.time_iterator):
            data_couple = self.db_worker.variety_price(date=date, variety=self.variety)  # 方法返回一个元组
            if not data_couple:
                continue
            data.append(data_couple)
            # 设置进度条
            self.process_signal.emit(["处理进度:", index + 1, self.time_length])
        self.result_signal.emit({"data": data, "message": self.lib + self.variety + "权重价格指数"})
        self.db_worker.close()


class VarietySelectedThread(AncestorThread):
    """查询品种日期线程"""
    def __init__(self, db_worker, variety):
        super(VarietySelectedThread, self).__init__()
        self.db_worker = db_worker
        self.variety = variety

    def run(self):
        """"选择品种后查询当前品种上市及退市日期"""
        items = self.db_worker.price_index_time_interval(variety=self.variety)
        self.result_signal.emit(items)  # 时间区间信号返回，设置
        self.db_worker.close()
