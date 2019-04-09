# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle


from PyQt5.QtWidgets import QWidget, QHBoxLayout, QPushButton, QSpacerItem, QSizePolicy
from PyQt5.QtCore import Qt, pyqtSignal


class ToolWidget(QWidget):
    tool_click_signal = pyqtSignal(str)

    def __init__(self):
        super(ToolWidget, self).__init__()
        self.__init_ui()

    def __init_ui(self):
        style_sheet = """
            ToolWidget {
                background-color: rgb(240,240,240);
            }
            QPushButton{
                background-color: #FFFFFF;
                margin: 0 5px;
                padding: 3px 5px;
                font-size:12px;
            }
        """
        self.setAttribute(Qt.WA_StyledBackground, True)  # 设置了才能使用qss设置布局背景色
        self.layout = QHBoxLayout(spacing=0, margin=0)
        self.setLayout(self.layout)
        self.setStyleSheet(style_sheet)

    def addTool(self, tool):
        if not isinstance(tool, QPushButton):
            return
        self.layout.addWidget(tool, alignment=Qt.AlignTop)

    def addSpacer(self):
        self.layout.addSpacerItem(QSpacerItem(
            40, 10, QSizePolicy.Expanding, QSizePolicy.Minimum))