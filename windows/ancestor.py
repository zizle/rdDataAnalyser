# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
from PyQt5.QtWidgets import (
    QWidget, QLabel, QHBoxLayout, QDialog, QComboBox, QLineEdit, QVBoxLayout,QDialogButtonBox
)
from PyQt5.QtCore import pyqtSignal, Qt, QTimer, QThread
from PyQt5.QtGui import QFont,QIcon

from sql.shfe import SHFEWorker
from sql.dce import DCEWorker
from sql.cffex import CFFEXWorker
from sql.czce import CZCEWorker
from settings import RESEARCH_LIB


class DialogInput(QDialog):
    def __init__(self, text1, text2, title):
        super(DialogInput, self).__init__()
        # 交易所选择
        self.select_lib = QComboBox()
        # 填充
        for item in RESEARCH_LIB:
            self.select_lib.addItem(item)
        s_label = QLabel(text1)
        self.s_input = QLineEdit()
        f_label = QLabel(text2)
        self.f_input = QLineEdit()
        v_box = QVBoxLayout()
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        v_box.addWidget(self.select_lib, alignment=Qt.AlignCenter)
        v_box.addWidget(s_label, alignment=Qt.AlignCenter)
        v_box.addWidget(self.s_input, alignment=Qt.AlignCenter)
        v_box.addWidget(f_label, alignment=Qt.AlignCenter)
        v_box.addWidget(self.f_input, alignment=Qt.AlignCenter)
        v_box.addWidget(buttons)
        self.setLayout(v_box)
        self.setWindowIcon(QIcon('static/Icon.png'))
        self.setWindowTitle(title)
        self.adjustSize()

    def get_data(self):
        return self.s_input.text(), self.f_input.text(), self.select_lib.currentText()


class AncestorWindow(QWidget):
    """窗口主抽象类"""
    # 自定义信号
    message_signal = pyqtSignal(str)  # 信息显示
    process_signal = pyqtSignal(list)  # 进度显示

    def __init__(self, title):
        super(AncestorWindow, self).__init__()
        """连接数据库"""
        self.__connect_database()
        self.cost_time = 1
        # 标题横向布局
        self.title_hlayout = QHBoxLayout()
        title_label = QLabel(title)
        title_label.setFont(QFont("SimHei", 12,))
        title_label.setContentsMargins(0, 0, 0, 10)
        self.title_hlayout.addWidget(title_label, alignment=Qt.AlignTop | Qt.AlignCenter)

        """设置定时器"""
        self.timer = QTimer()
        self.timer.timeout.connect(self.count_time)

    def count_time(self):
        # print("计时1秒结束")
        self.cost_time += 1
        self.message_signal.emit("系统正在处理,耗时{}秒...".format(self.cost_time))

    def __connect_database(self):
        """初始化连接数据库"""
        self.shfe_getter = SHFEWorker()
        self.dce_getter = DCEWorker()
        self.cffex_getter = CFFEXWorker()
        self.czce_getter = CZCEWorker()


class AncestorThread(QThread):
    """线程主抽象类"""
    # 自定义执行结束后返回的信号：返回查询结果
    process_signal = pyqtSignal(list)
    result_signal = pyqtSignal(list)

    def __init__(self):
        super(AncestorThread, self).__init__()

    def __del__(self):
        self.wait()
        # self.exit()





