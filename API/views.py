from flask import request, jsonify
from app import app, db
from models import NIA

def create_record(table):
    table_name = table.__tablename__

    @app.route(f'/{table_name}/create', methods=['POST'])
    def create_new_record():
        data = request.get_json()

        new_record = table(**data)
        db.session.add(new_record)
        db.session.commit()

        return jsonify({'message': 'New record created'}), 201

def get_records(table):
    table_name = table.__tablename__

    @app.route(f'/{table_name}', methods=['GET'])
    def get_all_records():
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)

        records = table.query.paginate(page=page, per_page=per_page)

        output = []
        for record in records.items:
            record_data = {}
            if record.keyval is not None:
                record_data['keyval'] = record.keyval
            if record.stock_mrkt_cd is not None:
                record_data['stock_mrkt_cd'] = record.stock_mrkt_cd
            if record.acplc_lngg_stock_mrkt_nm is not None:
                record_data['acplc_lngg_stock_mrkt_nm'] = record.acplc_lngg_stock_mrkt_nm
            if record.engls_stock_mrkt_nm is not None:
                record_data['engls_stock_mrkt_nm'] = record.engls_stock_mrkt_nm
            if record.hb_ntn_cd is not None:
                record_data['hb_ntn_cd'] = record.hb_ntn_cd
            if record.lstng_cd is not None:
                record_data['lstng_cd'] = record.lstng_cd
            if record.acplc_lngg_entrp_nm is not None:
                record_data['acplc_lngg_entrp_nm'] = record.acplc_lngg_entrp_nm
            if record.engls_entrp_nm is not None:
                record_data['engls_entrp_nm'] = record.engls_entrp_nm
            if record.ovrss_entrp_crprt_rgno is not None:
                record_data['ovrss_entrp_crprt_rgno'] = record.ovrss_entrp_crprt_rgno
            if record.ovrss_entrp_bsnsm_rgno is not None:
                record_data['ovrss_entrp_bsnsm_rgno'] = record.ovrss_entrp_bsnsm_rgno
            if record.fndtn_dt is not None:
                record_data['fndtn_dt'] = record.fndtn_dt
            if record.lstng_dt is not None:
                record_data['lstng_dt'] = record.lstng_dt
            if record.acplc_lngg_indst_gnnm is not None:
                record_data['acplc_lngg_indst_gnnm'] = record.acplc_lngg_indst_gnnm
            if record.engls_indstrsctrs_nm is not None:
                record_data['engls_indstrsctrs_nm'] = record.engls_indstrsctrs_nm
            if record.crrnc_sctin_cd is not None:
                record_data['crrnc_sctin_cd'] = record.crrnc_sctin_cd
            if record.accnn_yr is not None:
                record_data['accnn_yr'] = record.accnn_yr
            if record.reprt_kind_cd is not None:
                record_data['reprt_kind_cd'] = record.reprt_kind_cd
            if record.stacnt_dt is not None:
                record_data['stacnt_dt'] = record.stacnt_dt
            if record.cuass_amt is not None:
                record_data['cuass_amt'] = record.cuass_amt
            if record.nncrrnt_assts_amt is not None:
                record_data['nncrrnt_assts_amt'] = record.nncrrnt_assts_amt
            if record.cash_and_dpst_amt is not None:
                record_data['cash_and_dpst_amt'] = record.cash_and_dpst_amt
            if record.scrts_amt is not None:
                record_data['scrts_amt'] = record.scrts_amt
            if record.lon_bond_amt is not None:
                record_data['lon_bond_amt'] = record.lon_bond_amt
            if record.instm_fnc_assts_amt is not None:
                record_data['instm_fnc_assts_amt'] = record.instm_fnc_assts_amt
            if record.lease_assts_amt is not None:
                record_data['lease_assts_amt'] = record.lease_assts_amt
            if record.tpe_assts_amt is not None:
                record_data['tpe_assts_amt'] = record.tpe_assts_amt
            if record.etc_assts_amt is not None:
                record_data['etc_assts_amt'] = record.etc_assts_amt
            if record.assts_summ is not None:
                record_data['assts_summ'] = record.assts_summ
            if record.fltng_debt_amt is not None:
                record_data['fltng_debt_amt'] = record.fltng_debt_amt
            if record.nncrrnt_debt_amt is not None:
                record_data['nncrrnt_debt_amt'] = record.nncrrnt_debt_amt
            if record.cstdpslblits_amt is not None:
                record_data['cstdpslblits_amt'] = record.cstdpslblits_amt
            if record.cstdbt_amt is not None:
                record_data['cstdbt_amt'] = record.cstdbt_amt
            if record.etc_debt_amt is not None:
                record_data['etc_debt_amt'] = record.etc_debt_amt
            if record.debt_summ is not None:
                record_data['debt_summ'] = record.debt_summ
            if record.captl is not None:
                record_data['captl'] = record.captl
            if record.captl_srpl is not None:
                record_data['captl_srpl'] = record.captl_srpl
            if record.captl_mdtn_amt is not None:
                record_data['captl_mdtn_amt'] = record.captl_mdtn_amt
            if record.etc_inclsn_prlss_acttl_amt is not None:
                record_data['etc_inclsn_prlss_acttl_amt'] = record.etc_inclsn_prlss_acttl_amt
            if record.prft_srpl is not None:
                record_data['prft_srpl'] = record.prft_srpl
            if record.captl_summ is not None:
                record_data['captl_summ'] = record.captl_summ
            if record.debt_captl_summ_amt is not None:
                record_data['debt_captl_summ_amt'] = record.debt_captl_summ_amt
            if record.prsls is not None:
                record_data['prsls'] = record.prsls
            if record.sllng_prmpc_amt is not None:
                record_data['sllng_prmpc_amt'] = record.sllng_prmpc_amt
            if record.bsn_cost_amt is not None:
                record_data['bsn_cost_amt'] = record.bsn_cost_amt
            if record.bsn_prft_amt is not None:
                record_data['bsn_prft_amt'] = record.bsn_prft_amt
            if record.bsn_else_cost_amt is not None:
                record_data['bsn_else_cost_amt'] = record.bsn_else_cost_amt
            if record.ctax_cost_strbf_ntincmls_amt is not None:
                record_data['ctax_cost_strbf_ntincmls_amt'] = record.ctax_cost_strbf_ntincmls_amt
            if record.ctax_cost_strbf_cntntbs_plamt is not None:
                record_data['ctax_cost_strbf_cntntbs_plamt'] = record.ctax_cost_strbf_cntntbs_plamt
            if record.ctax_cost_amt is not None:
                record_data['ctax_cost_amt'] = record.ctax_cost_amt
            if record.cntntbs_prlss_ctax_cost_amt is not None:
                record_data['cntntbs_prlss_ctax_cost_amt'] = record.cntntbs_prlss_ctax_cost_amt
            if record.cntntbs_prft_amt is not None:
                record_data['cntntbs_prft_amt'] = record.cntntbs_prft_amt
            if record.dscnt_bsnss_prlss_amt is not None:
                record_data['dscnt_bsnss_prlss_amt'] = record.dscnt_bsnss_prlss_amt
            if record.thtrm_ntpf_amt is not None:
                record_data['thtrm_ntpf_amt'] = record.thtrm_ntpf_amt
            if record.bsn_acti_csflw_amt is not None:
                record_data['bsn_acti_csflw_amt'] = record.bsn_acti_csflw_amt
            if record.invsm_acti_cash_infl_amt is not None:
                record_data['invsm_acti_cash_infl_amt'] = record.invsm_acti_cash_infl_amt
            if record.fnnr_acti_cash_infl_amt is not None:
                record_data['fnnr_acti_cash_infl_amt'] = record.fnnr_acti_cash_infl_amt
            if record.cash_incre_amt is not None:
                record_data['cash_incre_amt'] = record.cash_incre_amt
            if record.bsis_cash_amt is not None:
                record_data['bsis_cash_amt'] = record.bsis_cash_amt
            if record.entrm_cash_amt is not None:
                record_data['entrm_cash_amt'] = record.entrm_cash_amt
            if record.debt_rate is not None:
                record_data['debt_rate'] = record.debt_rate
            if record.bsn_prft_rt_val is not None:
                record_data['bsn_prft_rt_val'] = record.bsn_prft_rt_val
            if record.prsls_incre_rt is not None:
                record_data['prsls_incre_rt'] = record.prsls_incre_rt
            if record.bsn_prft_incre_rt_val is not None:
                record_data['bsn_prft_incre_rt_val'] = record.bsn_prft_incre_rt_val
            if record.thtrm_ntpf_incre_rt_val is not None:
                record_data['thtrm_ntpf_incre_rt_val'] = record.thtrm_ntpf_incre_rt_val
            if record.entrp_yrmn_grrt_val is not None:
                record_data['entrp_yrmn_grrt_val'] = record.entrp_yrmn_grrt_val
            if record.entrp_reltn_tdngs_dt is not None:
                record_data['entrp_reltn_tdngs_dt'] = record.entrp_reltn_tdngs_dt
            if record.entrp_reltn_tdngs_kind_cont is not None:
                record_data['entrp_reltn_tdngs_kind_cont'] = record.entrp_reltn_tdngs_kind_cont
            if record.entrp_reltn_tdngs_subjc is not None:
                record_data['entrp_reltn_tdngs_subjc'] = record.entrp_reltn_tdngs_subjc
            if record.entrp_reltn_tdngs_cont_smmr is not None:
                record_data['entrp_reltn_tdngs_cont_smmr'] = record.entrp_reltn_tdngs_cont_smmr
            if record.entrp_reltn_tdngs_dtl_cont is not None:
                record_data['entrp_reltn_tdngs_dtl_cont'] = record.entrp_reltn_tdngs_dtl_cont
            if record.info_orgin_cont is not None:
                record_data['info_orgin_cont'] = record.info_orgin_cont
            if record.entrp_reltn_tdngs_url is not None:
                record_data['entrp_reltn_tdngs_url'] = record.entrp_reltn_tdngs_url
            if record.cmp_insd_reltn_info is not None:
                record_data['cmp_insd_reltn_info'] = record.cmp_insd_reltn_info
            if record.cstmr_reltn_info is not None:
                record_data['cstmr_reltn_info'] = record.cstmr_reltn_info
            if record.sppl_reltn_info is not None:
                record_data['sppl_reltn_info'] = record.sppl_reltn_info
            if record.cmptt_reltn_info is not None:
                record_data['cmptt_reltn_info'] = record.cmptt_reltn_info
            if record.sbst_goods_reltn_info is not None:
                record_data['sbst_goods_reltn_info'] = record.sbst_goods_reltn_info
            if record.opert_sctin_cd is not None:
                record_data['opert_sctin_cd'] = record.opert_sctin_cd
            if record.data_crtin_dt is not None:
                record_data['data_crtin_dt'] = record.data_crtin_dt
            if record.cntct_prces_stts_cd is not None:
                record_data['cntct_prces_stts_cd'] = record.cntct_prces_stts_cd
            if record.cntct_prces_dt is not None:
                record_data['cntct_prces_dt'] = record.cntct_prces_dt

            output.append(record_data)

        return jsonify(output)
    
create_record(NIA)
get_records(NIA)
