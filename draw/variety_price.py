# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import matplotlib.ticker as mtick
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from pylab import mpl
from PyQt5.QtWidgets import QTableWidget, QSizePolicy, QAbstractItemView, QHeaderView, QTableWidgetItem
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor


class MapWidget(FigureCanvas):
    def __init__(self, parent=None, width=12, height=3, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)

        super(MapWidget, self).__init__(fig)
        self.setParent(parent)  # 我也不知道这行有什么用,看别人这么写,我也写上
        # 画布随着窗口的大小变化
        FigureCanvas.setSizePolicy(self, QSizePolicy.Expanding, QSizePolicy.Expanding)
        # 更新大小
        FigureCanvas.updateGeometry(self)
        mpl.rcParams['font.sans-serif'] = ['SimHei']  # 指定默认字体, 显示中文
        mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

        self.set_style()

    def delete(self):
        self.set_style()
        self.draw()

    def set_style(self):
        self.axes.cla()
        self.axes.grid(axis='y')
        self.axes.tick_params(axis='x', width=1, length=1.5, labelrotation=45, labelsize=7, )  # 设置的坐标轴， 刻度线大小， 旋转， 字体大小
        self.axes.tick_params(axis='y', width=0, labelsize=8)
        self.axes.set_xlabel('时间', fontdict={'fontsize': 10, 'horizontalalignment': 'left'})
        self.axes.set_ylabel('价格')

    def _data_handler(self, x_data, y_data):
        """数据显示合适参数处理"""
        if not [x_data, y_data]:
            print("数据不全")
            return
        if len(x_data) != len(y_data):
            print("数据有误")
            return
        # 计算价格显示
        min_price = int(float(min(y_data)))
        max_price = int(float(max(y_data)))
        if min_price == max_price:
            print('最大最小相等', len(str(max_price)))
            # step_price = 10
            zone = max_price
        else:
            zone = max_price - min_price
        zone_price = zone // 10
        print("zone_price", zone_price, 111111111111111)
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

        # 计算时间显示
        print('时间转换后:', len(x_data), x_data)
        count_time = len(x_data)
        print("count_time:", count_time)
        if count_time <= 20:
            step_time = 1
        else:
            step_time = count_time // 20
        # 调整参数
        self._adjust_params(step_price, step_time)

    def _adjust_params(self, step_price, step_time):
        self.axes.yaxis.set_major_locator(mtick.MultipleLocator(step_price))  # y 轴刻度设置
        self.axes.xaxis.set_major_locator(mtick.MultipleLocator(step_time))

    def map(self, x_data, y_data):
        """画图"""
        print(x_data)
        print(y_data,type(y_data[0]))
        try:
            self._data_handler(x_data, y_data)
            self.axes.plot(x_data, y_data, 'r', label='价格')
            print("画图完成")
            self.draw()
        except Exception as e:
            import traceback
            traceback.print_exc()


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

    def table(self, data):
        """表格数据填充"""
        data = data[::-1]
        self.setRowCount(len(data))
        for row in range(len(data)):  # 行
            for vol in range(4):  # 列
                if vol == 0:
                    date = QTableWidgetItem(str(data[row][0]))
                    date.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        date.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, date)
                if vol == 1:
                    price = QTableWidgetItem(str(data[row][1]))
                    price.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        price.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, price)
                if vol == 2:
                    volume = QTableWidgetItem(str(data[row][2]))
                    volume.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        volume.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, volume)
                if vol == 3:
                    holdings = QTableWidgetItem(str(data[row][3]))
                    holdings.setTextAlignment(Qt.AlignCenter)
                    if row % 2 == 1:
                        holdings.setBackground(QColor(220, 220, 220))
                    self.setItem(row, vol, holdings)

