# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle
"""创建数据库并创建表, 打包前需先运行"""
import os

from sqlalchemy import Column, Integer, String, FLOAT, create_engine, Table, MetaData

from settings import DB_DIR


def create_table():
    db = 'sqlite:///' + DB_DIR
    engine = create_engine(db, echo=True)
    conn = engine.connect()

    metadata = MetaData()
    Table(
        'shfe_msg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('presettlement_price', Integer),
        Column('open_price', Integer),
        Column('highest_price', Integer),
        Column('lowest_price', Integer),
        Column('close_price', Integer),
        Column('settlement_price', Integer),
        Column('zd1_chg', Integer),
        Column('zd2_chg', Integer),
        Column('volume', Integer),
        Column('open_interest', Integer),
        Column('open_interest_chg', Integer)
    )

    Table(
        'shfe_total', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('highest_price', Integer),
        Column('lowest_price', Integer),
        Column('avg_price', FLOAT),
        Column('volume', Integer),
        Column('turnover', FLOAT),
        Column('year_volume', FLOAT),
        Column('year_turnover', FLOAT)
    )

    Table(
        'shfe_metal', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('last_price', FLOAT),
        Column('open_price', FLOAT),
        Column('highest_price', FLOAT),
        Column('lowest_price', FLOAT),
        Column('avg_price', FLOAT),
        Column('close_price', FLOAT),
        Column('pre_close_price', FLOAT),
        Column('up_down', FLOAT),
        Column('zd1', FLOAT),
        Column('zd2', FLOAT),
        Column('settlement_price', FLOAT)
    )

    Table(
        'shfe_rkg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('rank', Integer),
        Column('company1', String),
        Column('company1_id', String),
        Column('cj1', Integer),
        Column('cj1_chg', Integer),
        Column('company2', String),
        Column('company2_id', String),
        Column('cj2', Integer),
        Column('cj2_chg', Integer),
        Column('company3', String),
        Column('company3_id', String),
        Column('cj3', Integer),
        Column('cj3_chg', Integer)
    )

    Table(
        'shfe_target', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('price', Integer),
        Column('holdings', Integer),
        Column('b_volume', Integer),
        Column('s_volume', Integer),
        Column('net_holding', Integer),
        Column('holding_rate', String)
    )

    Table(
        'dce_msg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('open_price', String),
        Column('highest_price', String),
        Column('lowest_price', String),
        Column('close_price', String),
        Column('presettlement_price', String),
        Column('settlement_price', String),
        Column('zd', String),
        Column('zd1', String),
        Column('volume', String),
        Column('holdings', String),
        Column('holdings_chg', String),
        Column('volume_price', String),
    )

    Table(
        'dce_rkg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('rank', String),
        Column('company1', String),
        Column('cj1', String),
        Column('cj1_chg', String),
        Column('company2', String),
        Column('cj2', String),
        Column('cj2_chg', String),
        Column('company3', String),
        Column('cj3', String),
        Column('cj3_chg', String)
    )

    Table(
        'dce_target', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('price', String),
        Column('holdings', Integer),
        Column('b_volume', Integer),
        Column('s_volume', Integer),
        Column('net_holding', Integer),
        Column('holding_rate', String)
    )

    Table(
        'cffex_msg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('open_price', String),
        Column('highest_price', String),
        Column('lowest_price', String),
        Column('volume', String),
        Column('turnover', String),
        Column('holdings', String),
        Column('close_price', String),
        Column('settlement_price', String),
        Column('zd1', String),
        Column('zd2', String)
    )

    Table(
        'cffex_rkg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('data_type', String),
        Column('rank', Integer),
        Column('company', String),
        Column('company_id', String),
        Column('volume', Integer),
        Column('volume_chg', Integer)
    )

    Table(
        'cffex_target', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('price', String),
        Column('holdings', Integer),
        Column('b_volume', Integer),
        Column('s_volume', Integer),
        Column('net_holding', Integer),
        Column('holding_rate', String)
    )

    Table(
        'czce_msg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('pre_settlement', String),
        Column('open_price', String),
        Column('highest_price', String),
        Column('lowest_price', String),
        Column('close_price', String),
        Column('settlement', String),
        Column('zd1', String),
        Column('zd2', String),
        Column('volume', String),
        Column('open_interest', String),
        Column('decrease', String),
        Column('turnover', String),
        Column('settlement_price', String)
    )

    Table(
        'czce_rkg', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('rank', String),
        Column('company1', String),
        Column('cj1', String),
        Column('cj1_chg', String),
        Column('company2', String),
        Column('cj2', Integer),
        Column('cj2_chg', String),
        Column('company3', String),
        Column('cj3', Integer),
        Column('cj3_chg', String)
    )

    Table(
        'czce_target', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('contract', String),
        Column('price', String),
        Column('holdings', Integer),
        Column('b_volume', Integer),
        Column('s_volume', Integer),
        Column('net_holding', Integer),
        Column('holding_rate', String)
    )

    # 上期所品种价格指数表
    Table(
        "shfe_price", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('volume', Integer),
        Column('holdings', Integer),
        Column('variety_price', FLOAT),
        Column('contract_price', FLOAT)
    )
    # 大商所品种权重价格指数表
    Table(
        "dce_price", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('volume', Integer),
        Column('holdings', Integer),
        Column('variety_price', FLOAT),
        Column('contract_price', FLOAT)
    )
    # 中金所品种权重价格指数表
    Table(
        "cffex_price", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('volume', Integer),
        Column('holdings', Integer),
        Column('variety_price', FLOAT),
        Column('contract_price', FLOAT)
    )
    # 郑商所品种权重价格指数表
    Table(
        "czce_price", metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('date', String),
        Column('name', String),
        Column('volume', Integer),
        Column('holdings', Integer),
        Column('variety_price', FLOAT),
        Column('contract_price', FLOAT)
    )

    # 在数据库中创建真实表
    metadata.create_all(engine)
    conn.close()


def create_source_file():
    # 各研究所的商品
    goods_lib = {
        'cffex': ['IF', 'IC', 'IH', 'TS', 'TF', 'T'],
        'shfe': ['铜', '铝', '锌', '铅', '镍', '锡', '黄金', '白银', '螺纹钢', '线材', '热轧卷板', '原油', '燃料油', '石油沥青', '天然橡胶', '纸浆'],
        'dce': ['豆一', '豆二', '豆粕', '豆油', '棕榈油', '玉米', '玉米淀粉', '纤维板', '胶合板', '聚乙烯', '聚氯乙烯', '聚丙烯', '焦炭', '焦煤', '铁矿石'],
        'czce': ['棉花', '早籼ER', '菜油RO', '白糖', 'PTA', '强麦WS', '强麦WH', '硬麦', '甲醇ME', '甲醇MA', '菜油OI', '早籼RI', '玻璃', '普麦', '油菜籽', '菜籽粕', '动力煤TC', '动力煤ZC', '粳稻', '晚籼', '硅铁', '锰硅', '棉纱', '苹果']
    }
    shfe_product_names = {
        '铜': 'cu',
        '铝': 'al',
        '锌': 'zn',
        '铅': 'pb',
        '镍': 'ni',
        '锡': 'sn',
        '黄金': 'au',
        '白银': 'ag',
        '螺纹钢': 'rb',
        '线材': 'wr',
        '热轧卷板': 'hc',
        '原油': 'sc',
        '燃料油': 'fu',
        '石油沥青': 'bu',
        '天然橡胶': 'ru',
        '纸浆': 'sp',
        '总计': 'total',
        '总计1': 'total1',
        '总计2': 'total2'
    }
    dce_product_names = {
        '豆一': 'a',
        '豆一小计': 'a',
        '豆二': 'b',
        '豆二小计': 'b',
        '豆粕': 'm',
        '豆粕小计': 'm',
        '豆油': 'y',
        '豆油小计': 'y',
        '棕榈油': 'p',
        '棕榈油小计': 'p',
        '玉米': 'c',
        '玉米小计': 'c',
        '玉米淀粉': 'cs',
        '玉米淀粉小计': 'cs',
        '鸡蛋': 'jd',
        '鸡蛋小计': 'jd',
        '纤维板': 'fb',
        '纤维板小计': 'fb',
        '胶合板': 'bb',
        '胶合板小计': 'bb',
        '聚乙烯': 'l',
        '聚乙烯小计': 'l',
        '聚氯乙烯': 'v',
        '聚氯乙烯小计': 'v',
        '聚丙烯': 'pp',
        '聚丙烯小计': 'pp',
        '焦炭': 'j',
        '焦炭小计': 'j',
        '焦煤': 'jm',
        '焦煤小计': 'jm',
        '铁矿石': 'i',
        '铁矿石小计': 'i',
        '总计': 'total',
    }
    czce_product_names = {
        '棉花': 'CF',
        '早籼ER': 'ER',
        '早籼': 'ER',
        '菜油RO': 'RO',
        '菜油': 'RO',
        '白糖': 'SR',
        'PTA': 'PTA',
        '强麦WS': 'WS',
        '强麦': 'WS',
        '强麦WH': 'WH',
        '硬麦': 'WT',
        '甲醇ME': 'ME',
        '甲醇': 'ME',
        '甲醇MA': 'MA',
        '菜油OI': 'OI',
        '早籼RI': 'RI',
        '玻璃': 'FG',
        '普麦': 'PM',
        '油菜籽': 'RS',
        '菜籽粕': 'RM',
        '动力煤TC': 'TC',
        '动力煤': 'TC',
        '动力煤ZC': 'ZC',
        '粳稻': 'JR',
        '晚籼': 'LR',
        '硅铁': 'SF',
        '锰硅': 'SM',
        '棉纱': 'CY',
        '苹果': 'AP'
    }
    with open("data/goodsLib.dat", 'w', encoding="utf-8") as f:
        f.write(str(goods_lib))

    with open("data/shfe.p.dat", 'w', encoding="utf-8") as f:
        f.write(str(shfe_product_names))

    with open("data/dce.p.dat", 'w', encoding="utf-8") as f:
        f.write(str(dce_product_names))

    with open("data/czce.p.dat", 'w', encoding="utf-8") as f:
        f.write(str(czce_product_names))

#
# if __name__ == '__main__':
#     if os.path.exists("data/rd_pro.db"):
#         print('数据库已存在')
#     create_table()
#     # create_source_file()

