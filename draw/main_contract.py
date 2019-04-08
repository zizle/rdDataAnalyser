# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from pylab import mpl
from PyQt5.QtWidgets import QTableWidget, QSizePolicy, QAbstractItemView, QHeaderView


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

        self.__init_style()

    def __init_style(self):
        self.axes.set_xlabel('时间', fontdict={'fontsize': 10, 'horizontalalignment': 'left'})
        self.axes.set_ylabel('价格')


class TableWidget(QTableWidget):
    def __init__(self, column):
        super(TableWidget, self).__init__()
        self.setColumnCount(column)  # 列数
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)  # 自适应大小
        self.setSelectionBehavior(QAbstractItemView.SelectRows)  # 一次选中一行
