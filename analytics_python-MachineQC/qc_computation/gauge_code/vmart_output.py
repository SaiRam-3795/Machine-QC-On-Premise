''' This module contains function to create Vmart Stats to be added to Output JSON '''


def Vmart(input_json,subassembly_instance,ac_features_list, ac_key_indicator, xl_features_list, xl_key_indicator, vac_features_list,vac_key_indicator,
          ir_features_list, ir_key_indicator, hy_features_list, hy_key_indicator, bp_features_list, bp_key_indicator):
     
    try:
        if xl_key_indicator==1 and xl_features_list is not None:
            xl_stat_list_r={'RTC':xl_features_list['rtc_r'],
                            'ABSOLUTE_AREA_OF_ASYMMETRY':xl_features_list['abs_area_of_asymmetry_r'],
                            'Coeff_of_var':xl_features_list['mr_coeff_of_var_r'],
                            'MEAN':xl_features_list['summary_stats_called'][0]['mean'],
                            'MEDIAN':xl_features_list['summary_stats_called'][0]['median'],
                            'VARIANCE':xl_features_list['summary_stats_called'][0]['variance'],
                            'SKEWNESS':xl_features_list['summary_stats_called'][0]['skewness'],
                            'KURTOSIS':xl_features_list['summary_stats_called'][0]['kurtosis'],
                            'RMS':xl_features_list['summary_stats_called'][0]['rms'],
                            'PERCENTILE_5':xl_features_list['summary_stats_called'][0]['percentile_5'],
                            'PERCENTILE_95':xl_features_list['summary_stats_called'][0]['percentile_95'],
                            'PERCENTILE_25':xl_features_list['summary_stats_called'][0]['percentile_25'],
                            'PERCENTILE_75':xl_features_list['summary_stats_called'][0]['percentile_75'],
                            'MAX':xl_features_list['summary_stats_called'][0]['max'],
                            'MIN':xl_features_list['summary_stats_called'][0]['min'],
                            'CREST':xl_features_list['summary_stats_called'][0]['crest']
                            }
            
            xl_stat_list_theta={'RTC':xl_features_list['rtc_theta'],
                                'ABSOLUTE_AREA_OF_ASYMMETRY':xl_features_list['abs_area_of_asymmetry_theta'],
                                'Coeff_of_var':xl_features_list['mr_coeff_of_var_theta'],
                                'MEAN':xl_features_list['summary_stats_called'][1]['mean'],
                                'MEDIAN':xl_features_list['summary_stats_called'][1]['median'],
                                'VARIANCE':xl_features_list['summary_stats_called'][1]['variance'],
                                'SKEWNESS':xl_features_list['summary_stats_called'][1]['skewness'],
                                'KURTOSIS':xl_features_list['summary_stats_called'][1]['kurtosis'],
                                'RMS':xl_features_list['summary_stats_called'][1]['rms'],
                                'PERCENTILE_5':xl_features_list['summary_stats_called'][1]['percentile_5'],
                                'PERCENTILE_95':xl_features_list['summary_stats_called'][1]['percentile_95'],
                                'PERCENTILE_25':xl_features_list['summary_stats_called'][1]['percentile_25'],
                                'PERCENTILE_75':xl_features_list['summary_stats_called'][1]['percentile_75'],
                                'MAX':xl_features_list['summary_stats_called'][1]['max'],
                                'MIN':xl_features_list['summary_stats_called'][1]['min'],
                                'CREST':xl_features_list['summary_stats_called'][1]['crest']
                                }
            
            xl_stat_list_phi={'RTC':xl_features_list['rtc_phi'],
                              'ABSOLUTE_AREA_OF_ASYMMETRY':xl_features_list['abs_area_of_asymmetry_phi'],
                              'Coeff_of_var':xl_features_list['mr_coeff_of_var_theta'],
                              'MEAN':xl_features_list['summary_stats_called'][2]['mean'],
                              'MEDIAN':xl_features_list['summary_stats_called'][2]['median'],
                              'VARIANCE':xl_features_list['summary_stats_called'][2]['variance'],
                              'SKEWNESS':xl_features_list['summary_stats_called'][2]['skewness'],
                              'KURTOSIS':xl_features_list['summary_stats_called'][2]['kurtosis'],
                              'RMS':xl_features_list['summary_stats_called'][2]['rms'],
                              'PERCENTILE_5':xl_features_list['summary_stats_called'][2]['percentile_5'],
                              'PERCENTILE_95':xl_features_list['summary_stats_called'][2]['percentile_95'],
                              'PERCENTILE_25':xl_features_list['summary_stats_called'][2]['percentile_25'],
                              'PERCENTILE_75':xl_features_list['summary_stats_called'][2]['percentile_75'],
                              'MAX':xl_features_list['summary_stats_called'][2]['max'],
                              'MIN':xl_features_list['summary_stats_called'][2]['min'],
                              'CREST':xl_features_list['summary_stats_called'][2]['crest']
                              }
            
            
            stat_list_xl={'R':xl_stat_list_r,'THETA':xl_stat_list_theta,'PHI':xl_stat_list_phi}
            link_stats_xl={
                           'Dataloss_Percentage':xl_features_list['xl_data_loss_percent'] if xl_features_list['xl_data_loss_percent']>=0 else 0
                           }

            machine_util={'Mach_Util_10_Mins':xl_features_list['machine_running_time_percentage']}
            stat_list_xl["Link Stats"]=link_stats_xl
            stat_list_xl["Machine_Util"]=machine_util
            
            xl_combined_param_list=stat_list_xl



        else:
            xl_stat_list_r={}
            xl_stat_list_theta={}
            xl_stat_list_phi={}
            xl_stat_list_link_stats={}
            xl_stat_list_machine_utit_10_mins={}
            xl_combined_param_list=None
            

        if ac_key_indicator==1 and ac_features_list is not None:
            ac_stat_list_r={'RTC':ac_features_list['rtc_r'],
                            'ABSOLUTE_AREA_OF_ASYMMETRY':ac_features_list['abs_area_of_asymmetry_r'],
                            'Coeff_of_var':ac_features_list['mr_coeff_of_var_r'],
                            'MEAN':ac_features_list['summary_stats_called'][0]['mean'],
                            'MEDIAN':ac_features_list['summary_stats_called'][0]['median'],
                            'VARIANCE':ac_features_list['summary_stats_called'][0]['variance'],
                            'SKEWNESS':ac_features_list['summary_stats_called'][0]['skewness'],
                            'KURTOSIS':ac_features_list['summary_stats_called'][0]['kurtosis'],
                            'RMS':ac_features_list['summary_stats_called'][0]['rms'],
                            'PERCENTILE_5':ac_features_list['summary_stats_called'][0]['percentile_5'],
                            'PERCENTILE_95':ac_features_list['summary_stats_called'][0]['percentile_95'],
                            'PERCENTILE_25':ac_features_list['summary_stats_called'][0]['percentile_25'],
                            'PERCENTILE_75':ac_features_list['summary_stats_called'][0]['percentile_75'],
                            'MAX':ac_features_list['summary_stats_called'][0]['max'],
                            'MIN':ac_features_list['summary_stats_called'][0]['min'],
                            'CREST':ac_features_list['summary_stats_called'][0]['crest']
                            }
            
            ac_stat_list_theta={'RTC':ac_features_list['rtc_theta'],
                                'ABSOLUTE_AREA_OF_ASYMMETRY':ac_features_list['abs_area_of_asymmetry_theta'],
                                'Coeff_of_var':ac_features_list['mr_coeff_of_var_theta'],
                                'MEAN':ac_features_list['summary_stats_called'][1]['mean'],
                                'MEDIAN':ac_features_list['summary_stats_called'][1]['median'],
                                'VARIANCE':ac_features_list['summary_stats_called'][1]['variance'],
                                'SKEWNESS':ac_features_list['summary_stats_called'][1]['skewness'],
                                'KURTOSIS':ac_features_list['summary_stats_called'][1]['kurtosis'],
                                'RMS':ac_features_list['summary_stats_called'][1]['rms'],
                                'PERCENTILE_5':ac_features_list['summary_stats_called'][1]['percentile_5'],
                                'PERCENTILE_95':ac_features_list['summary_stats_called'][1]['percentile_95'],
                                'PERCENTILE_25':ac_features_list['summary_stats_called'][1]['percentile_25'],
                                'PERCENTILE_75':ac_features_list['summary_stats_called'][1]['percentile_75'],
                                'MAX':ac_features_list['summary_stats_called'][1]['max'],
                                'MIN':ac_features_list['summary_stats_called'][1]['min'],
                                'CREST':ac_features_list['summary_stats_called'][1]['crest']
                                }
            
            ac_stat_list_phi={'RTC':ac_features_list['rtc_phi'],
                              'ABSOLUTE_AREA_OF_ASYMMETRY':ac_features_list['abs_area_of_asymmetry_phi'],
                              'Coeff_of_var':ac_features_list['mr_coeff_of_var_theta'],
                              'MEAN':ac_features_list['summary_stats_called'][2]['mean'],
                              'MEDIAN':ac_features_list['summary_stats_called'][2]['median'],
                              'VARIANCE':ac_features_list['summary_stats_called'][2]['variance'],
                              'SKEWNESS':ac_features_list['summary_stats_called'][2]['skewness'],
                              'KURTOSIS':ac_features_list['summary_stats_called'][2]['kurtosis'],
                              'RMS':ac_features_list['summary_stats_called'][2]['rms'],
                              'PERCENTILE_5':ac_features_list['summary_stats_called'][2]['percentile_5'],
                              'PERCENTILE_95':ac_features_list['summary_stats_called'][2]['percentile_95'],
                              'PERCENTILE_25':ac_features_list['summary_stats_called'][2]['percentile_25'],
                              'PERCENTILE_75':ac_features_list['summary_stats_called'][2]['percentile_75'],
                              'MAX':ac_features_list['summary_stats_called'][2]['max'],
                              'MIN':ac_features_list['summary_stats_called'][2]['min'],
                              'CREST':ac_features_list['summary_stats_called'][2]['crest']
                              }
            
            
            stat_list_ac={'R':ac_stat_list_r,'THETA':ac_stat_list_theta,'PHI':ac_stat_list_phi}
            link_stats_ac={
                           'Dataloss_Percentage':ac_features_list['ac_data_loss_percent'] if ac_features_list['ac_data_loss_percent']>=0 else 0
                           }

            machine_util={'Mach_Util_10_Mins':ac_features_list['machine_running_time_percentage']}
            stat_list_ac["Link Stats"]=link_stats_ac
            stat_list_ac["Machine_Util"]=machine_util
            
            ac_combined_param_list=stat_list_ac
            
        else:
            ac_stat_list_r={}
            ac_stat_list_theta={}
            ac_stat_list_phi={}
            ac_stat_list_link_stats={}
            ac_stat_list_machine_utit_10_mins={}
            ac_combined_param_list=None
            
        
        if(  (hy_key_indicator == 1 ) & ( hy_features_list !=None  ) ):
          
            hy_stat_list_link_stats = {
                    'Dataloss_Percentage' :  0 if hy_features_list['hy_data_loss_percent'] < 0 else hy_features_list['hy_data_loss_percent'],
                                          
                                          }
            
            hy_combined_param_list = {'Link Stats' : hy_stat_list_link_stats}
          
        else:
            hy_stat_list_link_stats = {}
            hy_combined_param_list=None
            
            
        if( ( ir_key_indicator == 1 ) & ( ir_features_list!=None ) ):
          
            ir_stat_list_link_stats = {
                    'Dataloss_Percentage' :  0 if ir_features_list['ir_data_loss_percent'] < 0 else ir_features_list['ir_data_loss_percent'],
                                          
                                          }
            ir_combined_param_list = {'Link Stats' : ir_stat_list_link_stats}
            
            
        else:
            ir_stat_list_link_stats = {}
            ir_combined_param_list=None
        
        
        
        if( ( vac_key_indicator == 1 ) & ( vac_features_list!=None ) ):
            
            vac_features_list2=vac_features_list.copy()
          
            vac_stat_list_link_stats = {
                    'Dataloss_Percentage' :  0 if vac_features_list2['vac_data_loss_percent'] < 0 else vac_features_list2['vac_data_loss_percent'],
                                          
                                          }
            vac_stat_list_output = {'MEDIAN' : vac_features_list2['vac_median'] }
            
            
            vac_combined_param_list={'Cycles-Metadata' : vac_features_list2,
                                     'Link Stats' : vac_stat_list_link_stats,
                                     'VAC' : vac_stat_list_output}
            
            del vac_combined_param_list['Cycles-Metadata']['vac_median']
            del vac_combined_param_list['Cycles-Metadata']['vac_data_loss_percent']
            del vac_combined_param_list['Cycles-Metadata']['vac_data_status']
            del vac_combined_param_list['Cycles-Metadata']['vac_data']
            del vac_combined_param_list['Cycles-Metadata']['vac_timestamp']
            
        else:
            vac_stat_list_link_stats = {}
            vac_stat_list_output = {}
            vac_combined_param_list=None



        if( ( bp_key_indicator == 1 ) and bp_features_list!=None ):
     
            bp_stat_list_link_stats = {
                                        'Dataloss_Percentage' :  0 if bp_features_list['bp_data_loss_percent'] < 0 else bp_features_list['bp_data_loss_percent'],
                                         
                                    }
            bp_stat_list_output = {'MEDIAN' : bp_features_list['bp_median'] }
           
           
            bp_combined_param_list={
                                     'Link Stats' : bp_stat_list_link_stats,
                                     'Barometric Pressure' : bp_stat_list_output
                                }
        else:
            bp_stat_list_link_stats = {}
            bp_combined_param_list=None
        
        
        collector_list={}

        if xl_combined_param_list is not None:
            collector_list.update(xl1=xl_combined_param_list)
        
        if ac_combined_param_list is not None:
            collector_list.update(ac1=ac_combined_param_list)
        
        if vac_combined_param_list is not None:
            
            collector_list.update(vac1 = vac_combined_param_list)
        
        if ir_combined_param_list is not None:
            
            collector_list.update(ir1 = ir_combined_param_list)
            
        if hy_combined_param_list is not None:
            
            collector_list.update(hy1 = hy_combined_param_list)

        if bp_combined_param_list is not None:

            collector_list.update(bp1 = bp_combined_param_list)
            
        subassembly_instance_collector_list_0 = { 'collectors' : collector_list }
        
        subassembly_instance_collector_list = {subassembly_instance : subassembly_instance_collector_list_0}
        
        return subassembly_instance_collector_list
    
    except Exception as e:
        import sys
        exec_info=sys.exc_info()[2]
        line=exec_info.tb_lineno
        print("An Error has occured in Vmart at line {}: {}".format(line,str(e)))
        return {}
