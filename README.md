# MHCM1-task 
import pandas as pd
import sys
import re
import warnings
warnings.filterwarnings('ignore')
from base import BaseSDQApi
import utils
import traceback
import tqdm
import numpy as np
from functools import partial
import logging

class MHCM1(BaseSDQApi):
    domain_list = ['MH', 'CM','DS']
    def execute(self):
        study = self.study_id
        sub_cat = 'MHCM1'
        fields_dict = self.get_field_dict(subcat=sub_cat)
        fields_labels = dict(self.get_field_labels(subcat=sub_cat))
        deeplink_template = self.get_deeplink(study)
        fn_config = self.get_fn_config(study=study, subcat=sub_cat)
        match_config = fn_config['match']
        
        check_cmdu_flag = False
        if "cmdu_match_term" in fn_config:
            check_cmdu_flag = True
            check_cmdu_value = fn_config['cmdu_match_term']
            
        subjects = self.get_subjects(study, domain_list=self.domain_list, per_page=10000)
        for subject in tqdm.tqdm(subjects):
            try:            
                flatten_data = self.get_flatten_data(study, subject, per_page=10000, 
                                                     domain_list=self.domain_list)
                cm_df = pd.DataFrame(flatten_data['CM'])
                mh_df = pd.DataFrame(flatten_data['MH'])
                ds_df = pd.DataFrame(flatten_data['DS'])
                
                for cols in ['CMINDC', 'CMSTDAT']:
                    cm_df = cm_df[cm_df[cols].notna()]                 
                for cols in ['MHSTDAT', 'MHTERM', 'MHCM']:
                    mh_df = mh_df[mh_df[cols].notna()]
                for cols in ['DSSTDAT']:
                    ds_df = ds_df[ds_df[cols].notna()]
                
                    
                for ind in range(mh_df.shape[0]):
                    try:
                        mh_record = mh_df.iloc[[ind]]
                        mhterm = mh_record['MHTERM'].values[0]
                        mhstdat = mh_record['MHSTDAT'].apply(utils.get_date)
                        mhcm = mh_record['MHCM'].values[0]
                    

                        match_flag, cm_match_df = utils.ae_cm_mapper(prim_rec=mh_record,
                                                           sec_df=cm_df,
                                                           subcat=sub_cat,
                                                           str_match='contains',
                                                           **match_config)
                        
                        if match_flag is False:
                            continue
                        
                        query_flag = False
                        for cm_ind in range(cm_match_df.shape[0]):
                            cm_record = cm_match_df.iloc[[cm_ind]]
                            cmstdat = cm_record['CMSTDAT'].apply(utils.get_date)
                            try:
                                ds_df = ds_df[ds_df['subjid']==cm_record['subjid'].values[0]].reset_index()
                            except:
                                ds_df = ds_df[ds_df['subjid']==cm_record['subjid'].values[0]]
                            ds_record = ds_df.iloc[[0]]
                            dsstdat = ds_record['DSSTDAT'].apply(utils.get_date)
                            cmindc = cm_match_df['CMINDC'].values[0]
                            cmtrt = cm_match_df['CMTRT'].values[0]
                            # cmdu = cm_match_df['CMDU'].values[0] if 'CMDU' in cm_match_df.columns.tolist() else ''
                            
                            # if utils.check_cmindc(cmindc) or utils.check_cmindc(cmtrt):
                            #     continue   
                            mhterm = new_ae_record['CMINDC'].values[0].upper()
                            if cmstdat.values[0] < dsstdat.values[0]:
                                print(mhstdat.values[0],cmstdat.values[0])
                                #(MHCM1 == 1 ):
                                if (mhstdat.values[0] > cmstdat.values[0]) and str(mhcm).lower() in ['1','1.0', 'yes']:       
                                        query_flag = True
                                        # if check_cmdu_flag is False:
                                        #     query_flag = True
                                        # else:
                                            # if (cmdu == check_cmdu_value):
                                            #     query_flag = True


                                        if query_flag is True:

                                            try:
                                                mh_record['MHSTDAT'] = mh_record['MHSTDAT'].apply(utils.format_datetime)
                                                cm_record['CMSTDAT'] = cm_record['CMSTDAT'].apply(utils.format_datetime)
                                            except:
                                                mh_record['MHSTDAT'] = utils.format_datetime(mh_record['MHSTDAT'])
                                                cm_record['CMSTDAT'] = utils.format_datetime(cm_record['CMSTDAT'])
                                            try:
                                                mh_record['MHENDAT'] = mh_record['MHENDAT'].apply(utils.format_datetime)
                                                cm_record['CMENDAT'] = cm_record['CMENDAT'].apply(utils.format_datetime)
                                            except:
                                                mh_record['MHENDAT'] = utils.format_datetime(mh_record['MHENDAT'])
                                                cm_record['CMENDAT'] = utils.format_datetime(cm_record['CMENDAT'])

                                            cm_record = cm_record.replace(np.nan, 'blank')
                                            mh_record = mh_record.replace(np.nan, 'blank')

                                            mh_record['MHCM'] = mh_record['MHCM'].apply(lambda x:'Yes' if x in [1,1.0,'1','1.0'] else 'No')

                                            piv_rec = {'MH': mh_record,
                                                        'CM': cm_record}

                                            cm_record['CMSTDAT'] = cm_record['CMSTDAT_RAW']
                                            cm_record['CMENDAT'] = cm_record['CMENDAT_RAW']
                                            mh_record['MHSTDAT'] = mh_record['MHSTDAT_RAW']
                                            mh_record['MHENDAT'] = mh_record['MHENDAT_RAW']
                                            cmstdat = cm_record['CMSTDAT']
                                            cmendat = cm_record['CMENDAT']
                                            mhstdat = mh_record['MHSTDAT']
                                            mhendat = mh_record['MHENDAT']

                                            report_dict = {}
                                            subcate_report_dict = {}
                                            for dom, cols in fields_dict.items():
                                                piv_df = piv_rec[dom]
                                                present_col = [col for col in cols if col in piv_df.columns.tolist()]
                                                rep_df = piv_df[present_col]
                                                rep_df['deeplink'] = utils.get_deeplink(deeplink_template, piv_df)
                                                rep_df = rep_df.rename(columns=fields_labels)
                                                report_dict[dom] = rep_df.to_json(orient='records')

                                            subcate_report_dict[sub_cat] = report_dict

                                            mhterm = mh_record['MHTERM'].values[0]
                                            cmtrt = cm_record['CMTRT'].values[0]
                                            mh_stdat = mh_record['MHSTDAT'].values[0]
                                            cm_stdat = cm_record['CMSTDAT'].values[0]

                                            mh_record = mh_record.iloc[0]

                                            query_text_param = {
                                                'MHTERM': mhterm,
                                                "CMTRT": cmtrt,
                                                "MHSTDAT": mh_stdat,
                                                "CMSTDAT": cm_stdat
                                            }
                                            payload = {
                                                "subcategory": sub_cat,
                                                "query_text": self.get_model_query_text_json(study, sub_cat, params= query_text_param),
                                                "form_index": str(mh_record['form_index']),
                                                "question_present": self.get_subcategory_json(study, sub_cat),
                                                "modif_dts": str(mh_record['modif_dts']),  
                                                "stg_ck_event_id": int(mh_record['ck_event_id']),
                                                "formrefname": str(mh_record['formrefname']),
                                                "report": subcate_report_dict,
                                                "confid_score": np.random.uniform(0.7, 0.97)
                                            }

                                            print(subject, payload)
                                            self.insert_query(study, subject, payload)

                    except:
                        print(traceback.format_exc())
                        continue
                
            except:
                print(traceback.format_exc())
                continue

if __name__ == '__main__':
    study_id = sys.argv[1]
    rule_id = sys.argv[2]
    version = sys.argv[3]
    rule = MHCM1(study_id, rule_id, version)
    rule.run()
