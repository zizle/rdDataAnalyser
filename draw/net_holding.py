# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

import datetime
import matplotlib
matplotlib.use('Qt5Agg')
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from pylab import mpl
from PyQt5.QtWidgets import QSizePolicy, QTableWidget, QHeaderView, QAbstractItemView, QTableWidgetItem
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor

from settings import COLUMN_NAMES


class MapWidget(FigureCanvas):
    """图像窗口部件"""
    def __init__(self, parent=None, width=12, height=3, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes1 = fig.add_subplot(111)
        self.axes2 = self.axes1.twinx()

        super(MapWidget, self).__init__(fig)
        self.setParent(parent)  # 我也不知道这行有什么用,看别人这么写,我也写上
        # 画布随着窗口的大小变化
        FigureCanvas.setSizePolicy(self, QSizePolicy.Expanding, QSizePolicy.Expanding)
        # 更新大小
        FigureCanvas.updateGeometry(self)
        mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体, 显示中文
        mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

        self.init_map()  # 初始化风格

    def init_map(self):
        """初始化"""
        self.set_style()

    def set_style(self):
        """设置样式"""
        self.axes1.cla()
        self.axes2.cla()
        # 只显示横向网格
        self.axes1.grid(axis='y')
        # 去掉边框
        self.axes1.spines['top'].set_visible(False)
        self.axes1.spines['right'].set_visible(False)
        self.axes1.spines['bottom'].set_visible(False)
        self.axes1.spines['left'].set_visible(False)

        self.axes2.spines['top'].set_visible(False)
        self.axes2.spines['right'].set_visible(False)
        # self.axes2.spines['bottom'].set_visible(False)
        self.axes2.spines['left'].set_visible(False)

        # 改变刻度样式
        self.axes1.tick_params(axis='x', width=1, length=1.5,  labelrotation=45, labelsize=7, )  # 设置的坐标轴， 刻度线大小， 旋转， 字体大小
        self.axes1.tick_params(axis='y', width=0, labelsize=8)
        self.axes2.tick_params(axis='y', width=0, labelsize=8)

        self.axes1.set_xlabel('时间', fontdict={'fontsize': 10, 'horizontalalignment': 'left'})
        self.axes1.set_ylabel('价格')
        self.axes2.set_ylabel('净持率')

        # 设置axes2的Y轴显示格式
        y_ticks = mtick.FormatStrFormatter('%.2f%%')
        self.axes2.yaxis.set_major_formatter(y_ticks)

    def _data_handler(self, y_left, y_right, x):
        """
        画图前的数据处理
        :param y_left: 左y轴
        :param y_right: 右y轴
        :param x: 横轴
        :return: y_left, y_right, x
        """
        # print('数据处理前:')
        # print('price:', len(y_left), y_left, type(y_left[0]))
        # print('rates:', len(y_right), y_right)
        # print('times:', len(x), x)
        if not all([y_left, y_right, x]):
            print('数据不全')
            return
        if len(y_left) != len(x) or len(y_right) != len(x):
            print('数据有误!')
            return
        # 计算价格显示
        min_price = min(y_left)
        max_price = max(y_left)
        if min_price == max_price:
            print('最大最小相等', len(str(max_price)))
            # step_price = 10
            zone = max_price
        else:
            zone = max_price - min_price
        zone_price = zone // 10
        # print('zone', zone_price, len(str(zone_price)))
        if zone_price == 0:
            step_price = 1
        elif len(str(zone_price)) == 5:
            step_price = int(str(zone_price)[0]) * 10000
        elif len(str(zone_price)) == 4:
            step_price = int(str(zone_price)[0]) * 1000
        elif len(str(zone_price)) == 3:
            step_price = int(str(zone_price)[0]) * 100
        elif len(str(zone_price)) == 2:
            step_price = int(str(zone_price)[0]) * 10
        elif len(str(zone_price)) == 1:
            step_price = zone_price
        else:
            step_price = 500

        # 计算净持率显示
        min_rate = min(y_right)
        max_rate = max(y_right)
        zone_rate = max_rate - min_rate
        if zone_rate >= 20:
            step_rate = 1.5
        elif zone_rate <= 7:
            step_rate = 0.5
        else:
            step_rate = 1
        # x 轴转为时间
        # x_times = pd.to_datetime(x)
        # print('时间转换前:', len(x), x)
        # x_times = [datetime.datetime.strptime(time, '%Y%m%d') for time in x]
        x_times = x
        # 计算时间显示
        # print('时间转换后:', len(x_times), x_times)
        count_time = len(x_times)
        # print("count_time:", count_time)
        if count_time <= 20:
            step_time = 1
        else:
            step_time = count_time // 20

        # 调整参数
        self._adjust_params(step_price, step_rate, step_time)
        return y_left, y_right, x_times

    def _adjust_params(self, step_price, step_rate, step_time):
        self.axes1.yaxis.set_major_locator(mtick.MultipleLocator(step_price))  # y 轴刻度设置
        self.axes2.yaxis.set_major_locator(mtick.MultipleLocator(step_rate))
        # x轴为时间显示，及设置间隔
        # self.axes1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        # self.axes1.xaxis.set_major_locator(mdates.DayLocator(interval=step_time))
        self.axes1.xaxis.set_major_locator(mtick.MultipleLocator(step_time))
        # self.axes2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        self.axes2.xaxis.set_major_locator(mtick.MultipleLocator(step_time))

    def net_holding_map(self, y_left, y_right, x):
        """drawing the maps"""
        # 数据处理
        y_left, y_right, x_times = self._data_handler(y_left=y_left, y_right=y_right, x=x)
        # print('数据处理后:')
        # print('price:', len(y_left), y_left)
        # print('rates:', len(y_right), y_right)
        # print('times:', len(x_times), x_times)
        # 画图
        price, = self.axes1.plot(x_times, y_left, 'b', label='价格')
        holding, = self.axes2.plot(x_times, y_right, 'r', label='净持率')
        self.axes1.legend((price, holding), ('价格', '净持率'), frameon=False, fontsize=6.5)
        self.draw()

    def delete(self):
        self.set_style()
        self.draw()


class TableWidget(QTableWidget):
    def __init__(self, column):
        super(TableWidget, self).__init__()
        self.setColumnCount(column)  # 列数
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)  # 自适应大小
        self.setSelectionBehavior(QAbstractItemView.SelectRows)  # 一次选中一行

    def set_style(self, header_labels):
        # 设置列名称
        self.setHorizontalHeaderLabels(header_labels)
        self.setRowCount(0)
        self.verticalHeader().setVisible(False)  # 隐藏垂直表头

    def net_holding_table(self, items):
        """表格显示数据"""
        items = items[::-1]
        # 行数
        self.setRowCount(len(items))
        for row in range(len(items)):
            for vol in range(7):
                if vol == 0:
                    date = QTableWidgetItem(str(items[row].date))
                    date.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        date.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, date)
                if vol == 1:
                    price = QTableWidgetItem(str(items[row].price))
                    price.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        price.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, price)
                if vol == 2:
                    holdings = QTableWidgetItem(str(items[row].holdings))
                    holdings.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        holdings.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, holdings)
                if vol == 3:
                    b_volume = QTableWidgetItem(str(items[row].b_volume))
                    b_volume.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        b_volume.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, b_volume)
                if vol == 4:
                    s_volume = QTableWidgetItem(str(items[row].s_volume))
                    s_volume.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        s_volume.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, s_volume)
                if vol == 5:
                    net_holding = QTableWidgetItem(str(items[row].net_holding))
                    net_holding.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        net_holding.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, net_holding)
                if vol == 6:
                    rate = '%.4f' % float(items[row].holding_rate.strip('%'))
                    holding_rate = QTableWidgetItem(str(rate))
                    holding_rate.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        holding_rate.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, holding_rate)



