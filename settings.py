# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 文件保存路径设置
SHFE_FILE_ROOT_DIR = os.path.join(BASE_DIR, 'data/shfe')  # 上期所数据文件保存路径
# SHFE_FILE_ROOT_DIR = os.path.join(BASE_DIR, 'data/shfe_test')  # 上期所数据文件保存路径
DCE_FILE_ROOT_DIR = os.path.join(BASE_DIR, 'data/dce')
# DCE_FILE_ROOT_DIR = os.path.join(BASE_DIR, 'data/dce_test')
CFFEX_FILE_ROO_DIR = os.path.join(BASE_DIR, 'data/cffex')
# CFFEX_FILE_ROO_DIR = os.path.join(BASE_DIR, 'data/cffex_test')
CZCE_FILE_ROO_DIR = os.path.join(BASE_DIR, 'data/czce')
# CZCE_FILE_ROO_DIR = os.path.join(BASE_DIR, 'data/czce_test')
# 数据库路径
# DB_DIR = os.path.join(BASE_DIR, 'data/rd_test.db')
DB_DIR = os.path.join(BASE_DIR, 'data/rd_dev.db')
# DB_DIR = os.path.join(BASE_DIR, 'data/rd_pro.db')
FILE_DIR = os.path.join(BASE_DIR, "data")

VERSION = '2.2.1'
RESEARCH_LIB = ['中国金融期货交易所', '上海期货交易所', '大连商品交易所', '郑州商品交易所']  # 研究所

with open("data/goodsLib.dat", 'r', encoding="utf-8") as f:
    GOODS_LIB = eval(f.read())

COLUMN_NAMES = ['日期', '价格', '总持仓', '净多', '净空', '净持仓', '净持率']  # 表格列名称

with open("data/cffex.p.dat", "r", encoding="utf-8") as f:
    CFFEX_PRODUCT_NAMES = eval(f.read())

with open("data/shfe.p.dat", "r", encoding="utf-8") as f:
    SHFE_PRODUCT_NAMES = eval(f.read())

with open("data/dce.p.dat", "r", encoding="utf-8") as f:
    DCE_PRODUCT_NAMES = eval(f.read())

with open("data/czce.p.dat", "r", encoding="utf-8") as f:
    CZCE_PRODUCT_NAMES = eval(f.read())

