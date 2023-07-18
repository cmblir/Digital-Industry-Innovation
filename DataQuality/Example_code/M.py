import pandas as pd
import os
from tqdm import tqdm

class financial:
    def __init__(self):
        self.viet_stock_dict = {"Date":"stndd_dt",
                                "symbol":"lstng_cd",
                                "Open":"stock_start",
                                "Close":"stock_end",
                                "marketcap":"mrprc_ttamt"}
        self.global_stock_dict = {"Date":"stndd_dt",
                                "symbol":"lstng_cd",
                                "Open":"stock_start",
                                "Close":"stock_end",
                                "High":"stock_high",
                                "Low":"stock_low",
                                "marketcap":"mrprc_ttamt"}

    def dataframe(self, respond, plcfi_d, stock_d, egnin_d) -> pd.DataFrame:
        self.respond = respond
        self.plcfi_d = plcfi_d
        self.stock_d = stock_d
        self.egnin_d = egnin_d
        

    def make_ev_dict(self, df) -> pd.DataFrame:
        ev_dict = {}
        for key, value in zip(df["표준컬럼 영문명"], df["표준 데이터타입"]):
            ev_dict[key] = value
        columns = [x.lower() for x in df["표준컬럼 영문명"]]
        return ev_dict, pd.DataFrame(columns=columns)