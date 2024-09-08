# -*- coding: utf-8 -*-
# %%
# http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20170524&type=ALLBUT0999
# %%
# !pip install psycopg2

# %%
import os
import re
import sys
import csv
import time
import string
import logging
import requests
import argparse
from typing import List
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pg import load_config, insert_data

# %%

# +
# from os import mkdir
# from os.path import isdir

# +
class Crawler():
    def __init__(self, prefix="data", logger: logging.Logger = None):
        """ Make directory if not exist when initialize """

        self.prefix = prefix
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        for sub_dir in ["tse", "otc"]:
            os.makedirs(os.path.join(self.prefix, sub_dir), exist_ok=True)

    def _clean_row(self, row):
        ''' Clean comma and spaces '''
        for index, content in enumerate(row):
            row[index] = re.sub(",", "", content.strip())
        return row

    def _record(self, stock_id, row):
        ''' Save row to csv file '''
        f = open('{}/{}.csv'.format(self.prefix, stock_id), 'a')
        cw = csv.writer(f, lineterminator='\n')
        cw.writerow(row)
        f.close()

    def _get_tse_data(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            # logging.error("Can not get TSE data at {}".format(date_str))
            self.logger.error("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()
        
        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])

        
        for data in content['data9']:  # json data key
            print(type(data), len(data))
            sign = '-' if data[9].find('green') > 0 else ''
            row = self._clean_row([
                date_str_mingguo, # 日期
                data[2], # 成交股數
                data[4], # 成交金額
                data[5], # 開盤價
                data[6], # 最高價
                data[7], # 最低價
                data[8], # 收盤價
                sign + data[10], # 漲跌價差
                data[3], # 成交筆數
            ])

            self._record(data[0].strip(), row)
            
    def get_tse_data(self, date_tuple):
        
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        date_Ymd = '{0}-{1:02d}-{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            # logging.error("Can not get TSE data at {}".format(date_str))
            self.logger.error("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()

        tse_data = pd.DataFrame(content["data9"],
                                columns=["證券代號",
                                         "證券名稱",
                                         "成交股數",
                                         "成交筆數",
                                         "成交金額",
                                         "開盤價",
                                         "最高價",
                                         "最低價",
                                         "收盤價",
                                         "漲跌(+/-)",
                                         "漲跌價差",
                                         "最後揭示買價",
                                         "最後揭示買量",
                                         "最後揭示賣價",
                                         "最後揭示賣量",
                                         "本益比"])

        tse_date = pd.DataFrame([date_Ymd for _ in range(tse_data.shape[0])], columns=["日期"])

        # 跌 => 漲跌價差補上負號
        diff_prices = tse_data["漲跌價差"].values
        for idx, up_down in enumerate(tse_data["漲跌(+/-)"].values):
            if re.search(".*color.green.*", up_down):
                diff_prices[idx] = "-" + diff_prices[idx]

        tse_data.drop(columns=["漲跌價差", "漲跌(+/-)"], inplace=True)
        tse_data["漲跌價差"] = diff_prices
        
        # string to float, (1) "," to ""; (2) "--" to -9999
        for col in tse_data.columns[2:]:
            try:
                tse_data[col] = [re.sub(",", "", s) for s in tse_data[col].values]
                tse_data[col] = [re.sub("--", "-9999", s) for s in tse_data[col].values]
                tse_data.astype({col: "float"})
            except Exception as e:
                self.logger.error(f"***E: assign {col} failed!\n{e}")
                # print(col, e)
        
        tse_data = pd.concat([tse_date, tse_data], axis=1)
        
        return tse_data

    def _get_otc_data(self, date_tuple):
        """
        reference url
            https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw&d=110/01/09
        """
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        print(url)
        page = requests.get(url)

        if not page.ok:
            # logging.error("Can not get OTC data at {}".format(date_str))
            self.logger.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()
        
        if result['reportDate'] != date_str:
            # logging.error("Get error date OTC data at {}".format(date_str))
            self.logger.error("Get error date OTC data at {}".format(date_str))
            return

        for table in [result['mmData'], result['aaData']]:
            for tr in table:
                row = self._clean_row([
                    date_str,
                    tr[8], # 成交股數
                    tr[9], # 成交金額
                    tr[4], # 開盤價
                    tr[5], # 最高價
                    tr[6], # 最低價
                    tr[2], # 收盤價
                    tr[3], # 漲跌價差
                    tr[10] # 成交筆數
                ])
                self._record(tr[0], row)
                
    def get_otc_data(self, date_tuple):
        """
        reference url
            https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw&d=110/01/09
        """
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
#         print(url)
        page = requests.get(url)

        if not page.ok:
            self.logger.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()
        
        if result['reportDate'] != date_str:
            self.logger.error("Get error date OTC data at {}".format(date_str))
            return
            
        cols = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價", "成交股數", "成交金額(元)", "成交筆數", "最後買價",
                "最後買量(千股)", "最後賣價", "最後賣量(千股)", "發行股數", "次日參考價", "次日漲停價", "次日跌停價"]
        
        otc_data = pd.DataFrame(result['aaData'], columns=cols)

        for col in cols[2:]:
#             print(col,  otc_data[col])
            otc_data[col] = [re.sub(",", "", s) for s in otc_data[col].values]
            otc_data[col] = [re.sub("---", "-9999", s) for s in otc_data[col].values]
            otc_data[col] = [re.sub("除息", "-9001", s) for s in otc_data[col].values]
            otc_data.astype({col: "float"})
        
        return otc_data

#     def get_data(self, date_tuple):
#         print('Crawling {}'.format(date_tuple))
#         self._get_tse_data(date_tuple)
#         self._get_otc_data(date_tuple)

    def get_data(self, start_date: List[int], end_date: List[int], to_csv: bool = False, to_db: str = None, wait_seconds: int = 1800):
        start_date = datetime(start_date[0], start_date[1], start_date[2])
        end_date = datetime(end_date[0], end_date[1], end_date[2])
        current = start_date
        while current <= end_date:
            print(current, end_date, "<<<<<<<<<<<<<")
            
            self.logger.info(">>> {}".format(current))

            try:
                tse_data = self.get_tse_data(current.timetuple()[0:3])
                if to_csv:
                    tse_data.to_csv(os.path.join(self.prefix, "tse", "tse_{}.csv".format(current.strftime("%Y%m%d"))), 
                                    index=False, 
                                    encoding="utf-8-sig")
                if to_db is not None:
                    insert_data(tse_data, tablename="listed_daily", config=to_db)

            except Exception as e:
                self.logger.error("  get tse data failed!\n  {}\n".format(e))
                tse_data = None

            try: 
                otc_data = self.get_otc_data(current.timetuple()[0:3])
                if to_csv:
                    otc_data.to_csv(os.path.join(self.prefix, "otc", "otc_{}.csv".format(current.strftime("%Y%m%d"))), 
                                    index=False, 
                                    encoding="utf-8-sig")
                # if to_db is not None:
                #     insert_data(otc_data, tablename="otc_daily", config=to_db)
            except Exception as e:
                self.logger.error("  get otc data failed!\n {}\n".format(e))
                otc_data = None

            if current == end_date:
                return {"tse": tse_data, "otc": otc_data}
# -
            current += timedelta(days=1)
            time.sleep(wait_seconds)

def main(args, log_dir: str = "log"):
    
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter(fmt="%(asctime)s [%(levelname)s] %(name)s (%(module)s-%(lineno)d): %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    logger = logging.getLogger("stock_data_crawler")
    logger.setLevel(logging.INFO)

    fileHandler = logging.FileHandler("{}/stock_data_crawler_{}.log".format(log_dir, datetime.now().strftime("%Y%m%d")),
                                      mode="w",
                                      encoding="utf_8_sig", 
                                      delay=False)
    
    fileHandler.setLevel(logging.INFO)
    fileHandler.setFormatter(fmt)
    logger.addHandler(fileHandler)

    start_date = datetime.strptime(str(args.start_date), "%Y%m%d")
    end_date = datetime.strptime(str(args.end_date), "%Y%m%d")
    crawler = Crawler(logger=logger)

    print((start_date.year, start_date.month, start_date.day), (end_date.year, end_date.month, end_date.day))
    
    # content = crawler.get_data((start_date.year, start_date.month, start_date.day), 
    #                            (end_date.year, end_date.month, end_date.day))
    
    # return content

    # sys.exit()

    # Day only accept 0 or 3 arguments
    # if len(args.day) == 0:
    #     first_day = datetime.today()
    # elif len(args.day) == 3:
    #     first_day = datetime(args.day[0], args.day[1], args.day[2])
    # else:
    #     parser.error('Date should be assigned with (YYYY MM DD) or none')
    #     return

    crawler = Crawler(logger=logger)

    first_day = start_date
    last_day = end_date

    # If back flag is on, crawl till 2004/2/11, else crawl one day
    # if args.back or args.check:
    print(args.back, "======================>>>")
    print(args, "======================>>>")
    # if args.back:
    #     # otc first day is 2007/04/20
    #     # tse first day is 2004/02/11

    #     # last_day = datetime(2004, 2, 11) if args.back else first_day - timedelta(10)
    #     start_date = datetime(2004, 2, 11) 
    #     max_error = 5
    #     error_times = 0

    #     while error_times < max_error and first_day >= last_day:
    #         try:
    #             # tse_data = crawler.get_data((first_day.year, first_day.month, first_day.day))
    #             tse_data = crawler.get_data(start_date=start_date, end_date=end_date, to_csv="C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini")
    #             error_times = 0
    #         except:
    #             date_str = first_day.strftime('%Y/%m/%d')
    #             logging.error('Crawl raise error {}'.format(date_str))
    #             error_times += 1
    #             continue
    #         finally:
    #             first_day -= timedelta(1)
    # else:
        # tse_data = crawler.get_data((first_day.year, first_day.month, first_day.day))
    tse_data = crawler.get_data(start_date=[start_date.year, start_date.month, start_date.day], 
                                end_date=[end_date.year, end_date.month, end_date.day], 
                                to_csv=args.to_csv,
                                to_db=args.to_db)

    return tse_data

# %%
        
@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False,
           match_args=True, kw_only=True, slots=False)
class TSECrawlerArguments:
    datetime_now: datetime = datetime.now()
    start_year: int = field(default=datetime_now.year)
    start_month: int = field(default=datetime_now.month)
    start_day: int = field(default=datetime_now.day)
    start_date: str = field(default=datetime_now.strftime("%Y%m%d"))
    end_year: int = field(default=datetime_now.year)
    end_month: int = field(default=datetime_now.month)
    end_day: int = field(default=datetime_now.day)
    end_date: str = field(default=datetime_now.strftime("%Y%m%d"))
    back: bool = False,
    to_csv: bool = True,
    to_db: str = "C:/Users/YuZhe/OneDrive - Foxconn/Project/tsec/pg/database.ini"



# %%
if __name__ == '__main__':

    dt_now = datetime.now()
    dt_now

    default_start_date = int("{0}{1:02d}{2:02d}".format(dt_now.year, dt_now.month, dt_now.day))
    default_end_date = default_start_date
    
    tesc_args = TSECrawlerArguments(start_date="20240111", end_date="20240408")
    content = main(tesc_args)
    sys.exit()

    # Get arguments
    parser = argparse.ArgumentParser(description='Crawl data at assigned day')
    parser.add_argument('-s', '--start_date', type=int, default=default_start_date, help='start date YYYYmmdd')
    parser.add_argument('-e', '--end_date', type=int, default=default_end_date, help='end date YYYYmmdd')

    parser.add_argument('day', type=int, nargs='*',
        help='assigned day (format: YYYY MM DD), default is today')
    parser.add_argument('-b', '--back', action='store_true',
        help='crawl back from assigned day until 2004/2/11')
    parser.add_argument('-c', '--check', action='store_true',
        help='crawl back 10 days for check data')

    args = parser.parse_args()
    print(args)
    main(args)

    sys.exit()
    

    crawl = Crawler("data")
#     crawl.get_data((2023, 1, 9))
    content = crawl.get_data((2013, 1, 1), (2023, 5, 2))
#     content = crawl.get_otc_data((2023, 4, 22))
#     content = crawl.get_tse_data((2023, 4, 25))
#     content = crawl.get_otc_data((2023, 4, 25))

    content

    cols = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價", "成交股數", "成交金額(元)", "成交筆數", "最後買價",
            "最後買量(千股)", "最後賣價", "最後賣量(千股)", "發行股數", "次日參考價", "次日漲停價", "次日跌停價"]
    otc_data = pd.DataFrame(content['aaData'], columns=cols)
    

    for col in cols[2:]:
        print(col)
        otc_data[col] = [re.sub(",", "", s) for s in otc_data[col].values]
        otc_data[col] = [re.sub("---", "-9999", s) for s in otc_data[col].values]
        otc_data.astype({col: "float"})

    [i for i in content.iloc[0]]

    for k, v in content.items():
#         print(k, v[0:10], type(v))
        print(k, type(v))
        if isinstance(v, list):
            print(v[0:5])
        elif isinstance(v, dict):
            print(v.keys())





# %%
