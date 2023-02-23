import pandas as pd
import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from tqdm import tqdm

class DataExtract:
    """
    데이터베이스에서 데이터를 추출하는 모듈
    """
    def __init__(self, id, pw, ip, pt, db, table_name):
        """
        postgresql 아이디, 비밀번호, 접속ip, 데이터베이스명을 \n
        입력해주세요.
        """
        self.id = id
        self.pw = pw
        self.ip = ip
        self.pt = pt
        self.db = db
        self.table_name = table_name

    def connect(self):
        """
        데이터베이스에 접속하는 함수
        """
        url = f"postgresql://{self.id}:{self.pw}@{self.ip}:{self.pt}/{self.db}"
        self.engine = create_engine(url)
    
    def extract(self):
        """
        데이터베이스에서 데이터를 추출하는 함수
        """
        df = pd.read_sql_table(table_name = self.table_name, con=self.engine)
        return df.to_excel(f"{self.table_name}.xlsx")

class DataLoad:
    """
    데이터를 데이터베이스에 적재하는 모듈 
    사용할 데이터가 많을 시 many를 True로 설정해주세요.
    """
    def __init__(self, many=False):
        """
        다수의 파일을 적재할 시 many = True로 설정해주세요.
        """
        self.dtypesql_finan = {
        'keyval' : sqlalchemy.types.NUMERIC(),
        'stock_mrkt_cd' : sqlalchemy.types.VARCHAR(6),
        'acplc_lngg_stock_mrkt_nm' : sqlalchemy.types.VARCHAR(60),
        'engls_stock_mrkt_nm': sqlalchemy.types.VARCHAR(60),
        'hb_ntn_cd' : sqlalchemy.types.VARCHAR(3),
        'lstng_cd' : sqlalchemy.types.VARCHAR(12),
        'acplc_lngg_entrp_nm' : sqlalchemy.types.VARCHAR(100),
        'engls_entrp_nm' : sqlalchemy.types.VARCHAR(100),
        'ovrss_entrp_crprt_rgno' : sqlalchemy.types.VARCHAR(50),
        'ovrss_entrp_bsnsm_rgno' : sqlalchemy.types.VARCHAR(50),
        'fndtn_dt' : sqlalchemy.types.VARCHAR(8),
        'lstng_dt' : sqlalchemy.types.VARCHAR(8),
        'acplc_lngg_indst_gnnm' : sqlalchemy.types.VARCHAR(100),
        'engls_indstrsctrs_nm' : sqlalchemy.types.VARCHAR(100),
        'crrnc_sctin_cd' : sqlalchemy.types.VARCHAR(3),
        'accnn_yr' : sqlalchemy.types.VARCHAR(4),
        'reprt_kind_cd' : sqlalchemy.types.VARCHAR(1),
        'stacnt_dt' : sqlalchemy.types.VARCHAR(8),
        'cuass_amt' : sqlalchemy.types.NUMERIC(),
        'nncrrnt_assts_amt' : sqlalchemy.types.NUMERIC(),
        'cash_and_dpst_amt' : sqlalchemy.types.NUMERIC(),
        'scrts_amt' : sqlalchemy.types.NUMERIC(),
        'lon_bond_amt' : sqlalchemy.types.NUMERIC(),
        'instm_fnc_assts_amt' : sqlalchemy.types.NUMERIC(),
        'lease_assts_amt' : sqlalchemy.types.NUMERIC(),
        'tpe_assts_amt' : sqlalchemy.types.NUMERIC(),
        'etc_assts_amt' : sqlalchemy.types.NUMERIC(),
        'assts_summ' : sqlalchemy.types.NUMERIC(),
        'fltng_debt_amt' : sqlalchemy.types.NUMERIC(),
        'nncrrnt_debt_amt' : sqlalchemy.types.NUMERIC(),
        'cstdpslblits_amt' : sqlalchemy.types.NUMERIC(),
        'cstdbt_amt' : sqlalchemy.types.NUMERIC(),
        'etc_debt_amt' : sqlalchemy.types.NUMERIC(),
        'debt_summ' : sqlalchemy.types.NUMERIC(),
        'captl' : sqlalchemy.types.NUMERIC(),
        'captl_srpl' : sqlalchemy.types.NUMERIC(),
        'captl_mdtn_amt' : sqlalchemy.types.NUMERIC(),
        'etc_inclsn_prlss_acttl_amt' : sqlalchemy.types.NUMERIC(),
        'prft_srpl' : sqlalchemy.types.NUMERIC(),
        'captl_summ' : sqlalchemy.types.NUMERIC(),
        'debt_captl_summ_amt' : sqlalchemy.types.NUMERIC(),
        'prsls' : sqlalchemy.types.NUMERIC(),
        'sllng_prmpc_amt' : sqlalchemy.types.NUMERIC(),
        'bsn_cost_amt' : sqlalchemy.types.NUMERIC(),
        'bsn_prft_amt' : sqlalchemy.types.NUMERIC(),
        'bsn_else_cost_amt' : sqlalchemy.types.NUMERIC(),
        'ctax_cost_strbf_ntincmls_amt' : sqlalchemy.types.NUMERIC(),
        'ctax_cost_strbf_cntntbs_plamt' : sqlalchemy.types.NUMERIC(),
        'ctax_cost_amt' : sqlalchemy.types.NUMERIC(),
        'cntntbs_prlss_ctax_cost_amt' : sqlalchemy.types.NUMERIC(),
        'cntntbs_prft_amt' : sqlalchemy.types.NUMERIC(),
        'dscnt_bsnss_prlss_amt' : sqlalchemy.types.NUMERIC(),
        'thtrm_ntpf_amt' : sqlalchemy.types.NUMERIC(),
        'bsn_acti_csflw_amt' : sqlalchemy.types.NUMERIC(),
        'invsm_acti_cash_infl_amt' : sqlalchemy.types.NUMERIC(),
        'fnnr_acti_cash_infl_amt' : sqlalchemy.types.NUMERIC(),
        'cash_incre_amt' : sqlalchemy.types.NUMERIC(),
        'bsis_cash_amt' : sqlalchemy.types.NUMERIC(),
        'entrm_cash_amt' : sqlalchemy.types.NUMERIC(),
        'debt_rate' : sqlalchemy.types.NUMERIC(),
        'bsn_prft_rt_val' : sqlalchemy.types.VARCHAR(4),
        'prsls_incre_rt' : sqlalchemy.types.NUMERIC(),
        'bsn_prft_incre_rt_val' : sqlalchemy.types.VARCHAR(4),
        'thtrm_ntpf_incre_rt_val' : sqlalchemy.types.VARCHAR(4),
        'entrp_yrmn_grrt_val' : sqlalchemy.types.VARCHAR(4),
        'entrp_reltn_tdngs_dt' : sqlalchemy.types.VARCHAR(8),
        'entrp_reltn_tdngs_kind_cont' : sqlalchemy.types.VARCHAR(10),
        'entrp_reltn_tdngs_subjc' : sqlalchemy.types.VARCHAR(100),
        'entrp_reltn_tdngs_cont_smmr' : sqlalchemy.types.VARCHAR(1000),
        'entrp_reltn_tdngs_dtl_cont' : sqlalchemy.types.VARCHAR(4000),
        'info_orgin_cont' : sqlalchemy.types.VARCHAR(4000),
        'entrp_reltn_tdngs_url' : sqlalchemy.types.VARCHAR(4000),
        'cmp_insd_reltn_info' : sqlalchemy.types.VARCHAR(4000),
        'cstmr_reltn_info' : sqlalchemy.types.VARCHAR(4000),
        'sppl_reltn_info' : sqlalchemy.types.VARCHAR(4000),
        'cmptt_reltn_info' : sqlalchemy.types.VARCHAR(4000),
        'sbst_goods_reltn_info' : sqlalchemy.types.VARCHAR(4000),
        'opert_sctin_cd' : sqlalchemy.types.VARCHAR(1),
        'data_crtin_dt' : sqlalchemy.types.VARCHAR(8),
        'cntct_prces_stts_cd' : sqlalchemy.types.VARCHAR(1),
        'cntct_prces_dt' : sqlalchemy.types.VARCHAR(8)}
        self.definition_finan = {
        'STOCK_MRKT_CD' : 6,
        'ACPLC_LNGG_STOCK_MRKT_NM' : 60,
        'ENGLS_STOCK_MRKT_NM' : 60,
        'HB_NTN_CD' : 3,
        'LSTNG_CD' : 12,
        'ACPLC_LNGG_ENTRP_NM' : 100,
        'ENGLS_ENTRP_NM' : 100,
        'OVRSS_ENTRP_CRPRT_RGNO' : 50,
        'OVRSS_ENTRP_BSNSM_RGNO' : 50,
        'FNDTN_DT' : 8,
        'LSTNG_DT' : 8,
        'ACPLC_LNGG_INDST_GNNM' : 100,
        'ENGLS_INDSTRSCTRS_NM' : 100,
        'CRRNC_SCTIN_CD' : 3,
        'ACCNN_YR' : 4,
        'REPRT_KIND_CD' : 1,
        'STACNT_DT' : 8,
        'BSN_PRFT_RT_VAL' : 4,
        'BSN_PRFT_INCRE_RT_VAL' : 4,
        'THTRM_NTPF_INCRE_RT_VAL' : 4,
        'ENTRP_YRMN_GRRT_VAL' : 4,
        'ENTRP_RELTN_TDNGS_DT' : 8,
        'ENTRP_RELTN_TDNGS_KIND_CONT' : 10,
        'ENTRP_RELTN_TDNGS_SUBJC' : 100,
        'ENTRP_RELTN_TDNGS_CONT_SMMR' : 1000,
        'ENTRP_RELTN_TDNGS_DTL_CONT' : 4000,
        'INFO_ORGIN_CONT' : 4000,
        'ENTRP_RELTN_TDNGS_URL' : 1000,
        'CMP_INSD_RELTN_INFO' : 4000,
        'CSTMR_RELTN_INFO' : 4000,
        'SPPL_RELTN_INFO' : 4000,
        'CMPTT_RELTN_INFO' : 4000,
        'SBST_GOODS_RELTN_INFO' : 4000,
        'OPERT_SCTIN_CD' : 1,
        'DATA_CRTIN_DT' : 8,
        'CNTCT_PRCES_STTS_CD' : 1,
        'CNTCT_PRCES_DT': 8}
        self.dtypesql_info = {
        'keyval' : sqlalchemy.types.NUMERIC,
        'hb_ntn_cd' : sqlalchemy.types.VARCHAR(3),
        'acplc_lngg_ntn_nm' : sqlalchemy.types.VARCHAR(40),
        'engls_ntn_nm' : sqlalchemy.types.VARCHAR(40),
        'ntn_lngg_cd_val' : sqlalchemy.types.VARCHAR(3),
        'acplc_lngg_lngg_nm' : sqlalchemy.types.VARCHAR(12),
        'engls_lngg_nm' : sqlalchemy.types.VARCHAR(40),
        'acplc_lngg_entrp_nm' : sqlalchemy.types.VARCHAR(100),
        'engls_entrp_nm' : sqlalchemy.types.VARCHAR(100),
        'acplc_lngg_oln_intrd_cont' : sqlalchemy.types.VARCHAR(400),
        'acplc_lngg_entrp_intrd_cont' : sqlalchemy.types.VARCHAR(4000),
        'engls_oln_intrd_cont' : sqlalchemy.types.VARCHAR(400),
        'engls_entrp_intrd_cont' : sqlalchemy.types.VARCHAR(4000),
        'acplc_lngg_ceo_nm' : sqlalchemy.types.VARCHAR(100),
        'engls_ceo_nm' : sqlalchemy.types.VARCHAR(100),
        'entrp_rprsn_tlno' : sqlalchemy.types.VARCHAR(40),
        'rprsn_fxno' : sqlalchemy.types.VARCHAR(50),
        'rprsn_email' : sqlalchemy.types.VARCHAR(100),
        'entrp_hmpg_url' : sqlalchemy.types.VARCHAR(700),
        'facebook_url' : sqlalchemy.types.VARCHAR(300),
        'instgrm_url' : sqlalchemy.types.VARCHAR(300),
        'ytb_url' : sqlalchemy.types.VARCHAR(300),
        'lnkdn_url' : sqlalchemy.types.VARCHAR(300),
        'acplc_lngg_pinch_nm' : sqlalchemy.types.VARCHAR(100),
        'engls_pinch_nm' : sqlalchemy.types.VARCHAR(100),
        'acplc_lngg_ofpst_nm' : sqlalchemy.types.VARCHAR(40),
        'engls_ofpst_nm' : sqlalchemy.types.VARCHAR(40),
        'pinch_sub_sgnng' : sqlalchemy.types.VARCHAR(50),
        'entrp_pinch_tlno' : sqlalchemy.types.VARCHAR(40),
        'pinch_email' : sqlalchemy.types.VARCHAR(100),
        'pinch_fxno' : sqlalchemy.types.VARCHAR(50),
        'entrp_pinch_mtlno' : sqlalchemy.types.VARCHAR(40),
        'acplc_lngg_entrp_addr' : sqlalchemy.types.VARCHAR(200),
        'acplc_lngg_entrp_dtadd' : sqlalchemy.types.VARCHAR(200),
        'engls_entrp_addr' : sqlalchemy.types.VARCHAR(200),
        'engls_entrp_dtadd' : sqlalchemy.types.VARCHAR(200),
        'entrp_post_no' : sqlalchemy.types.VARCHAR(12),
        'acplc_lngg_indst_gnnm' : sqlalchemy.types.VARCHAR(100),
        'engls_indstrsctrs_nm' : sqlalchemy.types.VARCHAR(100),
        'acplc_lngg_main_prduc_cont' : sqlalchemy.types.VARCHAR(4000),
        'engls_main_prduc_cont' : sqlalchemy.types.VARCHAR(4000),
        'fndtn_dt' : sqlalchemy.types.VARCHAR(8),
        'entrp_emply_val' : sqlalchemy.types.VARCHAR(400),
        'extnladt_sctin_val' : sqlalchemy.types.VARCHAR(20),
        'accnn_yr' : sqlalchemy.types.VARCHAR(4),
        'prsls_val' : sqlalchemy.types.VARCHAR(400),
        'crrnc_sctin_cd' : sqlalchemy.types.VARCHAR(3),
        'currn_unit_nm' : sqlalchemy.types.VARCHAR(20),
        'stock_mrkt_cd' : sqlalchemy.types.VARCHAR(6),
        'acplc_lngg_stock_mrkt_nm' : sqlalchemy.types.VARCHAR(60),
        'engls_stock_mrkt_nm' : sqlalchemy.types.VARCHAR(60),
        'lstng_cd' : sqlalchemy.types.VARCHAR(12),
        'lstng_dt' : sqlalchemy.types.VARCHAR(8),
        'entrp_addtn_info_val' : sqlalchemy.types.VARCHAR(1000),
        'opert_sctin_cd' : sqlalchemy.types.VARCHAR(1),
        'data_crtin_dt' : sqlalchemy.types.VARCHAR(8),
        'cntct_prces_stts_cd' : sqlalchemy.types.VARCHAR(1),
        'cntct_prces_dt' : sqlalchemy.types.VARCHAR(8)}
        self.definition_info = {
        'hb_ntn_cd' : 3,
        'acplc_lngg_ntn_nm' : 40,
        'engls_ntn_nm' : 40,
        'ntn_lngg_cd_val' : 3,
        'acplc_lngg_lngg_nm' : 12,
        'engls_lngg_nm' : 40,
        'acplc_lngg_entrp_nm' : 100,
        'engls_entrp_nm' : 100,
        'acplc_lngg_oln_intrd_cont' : 400,
        'acplc_lngg_entrp_intrd_cont' : 4000,
        'engls_oln_intrd_cont' : 400,
        'engls_entrp_intrd_cont' : 4000,
        'acplc_lngg_ceo_nm' : 100,
        'engls_ceo_nm' : 100,
        'entrp_rprsn_tlno' : 40,
        'rprsn_fxno' : 50,
        'rprsn_email' : 100,
        'entrp_hmpg_url' : 700,
        'facebook_url' : 300,
        'instgrm_url' : 300,
        'ytb_url' : 300,
        'lnkdn_url' : 300,
        'acplc_lngg_pinch_nm' : 100,
        'engls_pinch_nm' : 100,
        'acplc_lngg_ofpst_nm' : 40,
        'engls_ofpst_nm' : 40,
        'pinch_sub_sgnng' : 50,
        'entrp_pinch_tlno' : 40,
        'pinch_email' : 100,
        'pinch_fxno' : 50,
        'entrp_pinch_mtlno' : 40,
        'acplc_lngg_entrp_addr' : 200,
        'acplc_lngg_entrp_dtadd' : 200,
        'engls_entrp_addr' : 200,
        'engls_entrp_dtadd' : 200,
        'entrp_post_no' : 12,
        'acplc_lngg_indst_gnnm' : 100,
        'engls_indstrsctrs_nm' : 100,
        'acplc_lngg_main_prduc_cont' : 4000,
        'engls_main_prduc_cont' : 4000,
        'fndtn_dt' : 8,
        'entrp_emply_val' : 400,
        'extnladt_sctin_val' : 20,
        'accnn_yr' : 4,
        'prsls_val' : 400,
        'crrnc_sctin_cd' : 3,
        'currn_unit_nm' : 20,
        'stock_mrkt_cd' : 6,
        'acplc_lngg_stock_mrkt_nm' : 60,
        'engls_stock_mrkt_nm' : 60,
        'lstng_cd' : 12,
        'lstng_dt' : 8,
        'entrp_addtn_info_val' : 1000,
        'opert_sctin_cd' : 1,
        'data_crtin_dt' : 8,
        'cntct_prces_stts_cd' : 1,
        'cntct_prces_dt' : 8}
        self.many = many
        self.url = None
        self.df = None
        self.table_name = None
        self.replace = False
        self.first = False
        self.table_nameList = []
        self.DataFrameList = []

    def information(self):
        print("""
        함수 설명은 아래와 같습니다. \n
        DataLoading : 데이터를 일괄로 불러오는 함수 \n
        CheckLength : 데이터내의 값의 최대 길이까지 제한을 두는 함수 \n
        Load : 데이터를 배치프로세스를 진행하여 적재하는 함수 \n
        Login : 데이터베이스에 로그인하는 함수 \n
        Connect_DB : 데이터베이스에 적재하기 위해 기본값을 설정하는 함수 \n
        """)

    def DataLoading(self, Path):
        """
        데이터를 2개 이상 적재할 시 Path를 설정하여 사용하는 함수
        """
        FilePath = os.listdir(Path)
        if self.many == True:
            for file in tqdm(FilePath):
                tmp_table_name = file.split(".")[0]
                self.table_nameList.append(tmp_table_name)
                filetype = file.split(".")[1]
                if filetype == "csv":
                    try:
                        tmp = pd.read_csv(Path+file, encoding = "cp949")
                    except:
                        tmp = pd.read_csv(Path+file)
                elif filetype == "xlsx":
                    tmp = pd.read_excel(Path+file)       
                self.DataFrameList.append(tmp)
        else:
            for file in tqdm(FilePath):
                self.table_name = file.split(".")[0]
                filetype = file.split(".")[1]
                if filetype == "csv":
                    try:
                        self.df = pd.read_csv(Path+file, encoding = "cp949")
                    except:
                        self.df = pd.read_csv(Path+file)
                elif filetype == "xlsx":
                    self.df = pd.read_excel(Path+file)       

    def CheckLength(self):
        '''
        데이터의 크기보다 클 경우 해당 데이터 크기까지 자르는 함수
        '''
        if self.many == False:
            if self.table_name.split("_")[-1] == "m":
                definition = self.definition_info
            elif self.table_name.split("_")[-1] == "d":
                definition = self.definition_finan 
            for key, value in tqdm(definition.items()):
                for Length in range(len(self.df)):
                    check = str(self.df[key.lower()][Length])
                    if check == "nan":
                        continue
                    elif len(check) > value:
                        self.df[key.lower()][Length] = str(self.df[key.lower()][Length])[:value]
                    else:
                        pass
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if self.table_nameList[length].split("_")[-1] == "m":
                    definition = self.definition_info
                elif self.table_nameList[length].split("_")[-1] == "d":
                    definition = self.definition_finan 
                for key, value in tqdm(definition.items()):
                    for Length in range(len(self.DataFrameList[length])):
                        check = str(self.DataFrameList[length][key.lower()][Length])
                        if check == "nan":
                            continue
                        elif len(check) > value:
                            self.DataFrameList[length][key.lower()][Length] = str(self.DataFrameList[length][key.lower()][Length])[:value]
                        else:
                            pass


    def Load(self):
        """
        배치 프로세스를 이용한다. \n
        이는 일괄 처리라고도 하는 과정으로서 실시간으로 요청에 의해 처리되는 \n
        방식이 아닌 일괄적으로 대량의 데이터를 처리해준다.
        """
        engine = create_engine(self.url, executemany_mode = "batch")
        if self.many == False:
            if self.table_name.split("_")[-1] == "m":
                dtypesql = self.dtypesql_info
            elif self.table_name.split("_")[-1] == "d":
                dtypesql = self.dtypesql_finan
            if self.replace == False:
                self.df.to_sql(name = self.table_name, con=engine, schema='public',chunksize= 10000,
                if_exists='append', index = False, dtype=dtypesql, method = 'multi')
            elif self.replace == True:
                self.df.to_sql(name = self.table_name, con=engine, schema='public',chunksize= 10000,
                if_exists='replace', index = False, dtype=dtypesql, method = 'multi')
            return f"{self.table_name}가 적재 완료되었습니다."
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if self.table_nameList[length].split("_")[-1] == "m":
                    dtypesql = self.dtypesql_info
                elif self.table_nameList[length].split("_")[-1] == "d":
                    dtypesql = self.dtypesql_finan
                if self.replace == False:
                    self.DataFrameList[length].to_sql(name = self.table_nameList[length], con=engine, schema='public',chunksize= 10000,
                    if_exists='append', index = False, dtype=dtypesql, method = 'multi')
                elif self.replace == True:
                    self.DataFrameList[length].to_sql(name = self.table_nameList[length], con=engine, schema='public',chunksize= 10000,
                    if_exists='replace', index = False, dtype=dtypesql, method = 'multi')
                print(f"{self.table_nameList[length]}가 적재 완료되었습니다.")


    def Login(self, user, password, host, port, dbname):
        self.url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    def Connect_DB(self, replace=False, first=False):
        """
        postgresql 접속에 필요한 \n
        id, password, host, port, dbname이다.\n
        replace는 업데이트 여부를 의미한다. \n
        업데이트를 진행할 시 선택해주면 된다. \n
        각 국가마다 테이블명이 다르므로 필요하다.
        """
        self.replace = replace
        self.first = first
        if self.many == False:
            if first == False:
                try:
                    conn = psycopg2.connect(self.url)
                    cur = conn.cursor()
                    cur.execute(f"select keyval from {self.table_name} order by keyval desc limit 1;")
                    rows = cur.fetchall()
                    NowKeyval = int(str(rows[0]).split("'")[1])
                except psycopg2.DatabaseError as db_err:
                    print(f"현재 발생한 에러는 {db_err} 입니다.")
                for keyval, idx in zip(range(NowKeyval, len(self.df)+NowKeyval), range(len(self.df))):
                    self.df["keyval"][idx] = int(keyval)
                self.df["keyval"] = self.df["keyval"].astype(int)
            else:
                for Length in tqdm(range(len(self.df))):
                    self.df["keyval"][Length] = int(Length)
                self.df["keyval"] = self.df["keyval"].astype(int)
        else:
            for length in tqdm(range(len(self.table_nameList))):
                if first == False:
                    try:
                        conn = psycopg2.connect(self.url)
                        cur = conn.cursor()
                        cur.execute(f"select keyval from {self.table_nameList[length]} order by keyval desc limit 1;")
                        rows = cur.fetchall()
                        NowKeyval = int(str(rows[0]).split("'")[1]) + 1
                    except psycopg2.DatabaseError as db_err:
                        print(f"현재 발생한 에러는 {db_err} 입니다.")
                    for keyval, idx in zip(range(NowKeyval, len(self.DataFrameList[length])+NowKeyval), range(len(self.DataFrameList[length]))):
                        self.DataFrameList[length]["keyval"][idx] = int(keyval)
                    self.DataFrameList[length]["keyval"] = self.DataFrameList[length]["keyval"].astype(int)
                else:
                    for Length in tqdm(range(len(self.DataFrameList[length]))):
                        self.DataFrameList[length]["keyval"][Length] = int(Length)
                    self.DataFrameList[length]["keyval"] = self.DataFrameList[length]["keyval"].astype(int)