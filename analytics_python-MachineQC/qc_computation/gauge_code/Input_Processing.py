# -*- coding: utf-8 -*-
"""
Created on Mon Aug 31 12:12:30 2020

@author: Biswadeep Ghoshal
"""

# -*- coding: utf-8 -*-
"""
This module contains functions needed to process raw input data
NOTE: PSI to inch Hg conversion is not done in this code while processing VAC Data. VAC adjustments with BP are also not made in this code. These are all done from the middleware side
"""

from statistics import mean, median 
from qc_computation.gauge_code.unseq_final import unseq
from numpy import array, less, sqrt, arccos, arctan, arange, hstack, row_stack, column_stack, std, var, nanvar, abs as abl, apply_along_axis, int0, nanmin, where, nanquantile, quantile, nanmean, nanmedian, greater, zeros
from math import pi
from traceback import format_exc

from qc_computation.gauge_code.additional_functions_ver_1_12 import RTC_And_Absolute_Area_Asymmetry_Calculation
from qc_computation.gauge_code.exception_handler import Numeric_Input_Type_Checking, Non_Negative_Out_Of_Range_Exception_Checking, VAC_Data_Out_Of_Range_Exception_Checking
from scipy.stats import skew, kurtosis
from qc_computation.gauge_code.kernel_density_user_defined_3 import kde

from qc_computation.gauge_code.log_info_updation import add_log_info


#... Generating r, theta and phi from x, y and z of ac data ...#


def center(a):
    a_mean=mean(a)
    a_centred=list(map(lambda b: b-a_mean,a))
    return a_centred

def Generate_r_theta_phi_xyz_AC(json_data,subassembly_instance,log_json,current_datetime_stamp):                #... Input is a formatted json ...#
    u=json_data["subassemblies"][subassembly_instance]["collectors"]["ac1"]["CSV"]
    try:
        x=list(map(lambda a:float(a[1]),u))
        y=list(map(lambda a:float(a[2]),u))
        z=list(map(lambda a:float(a[3]),u))
    except Exception as e:
        import sys
        line=sys.exc_info()[2].tb_lineno
        add_log_info(log_json,current_datetime_stamp,"Generate_r_theta_phi_xyz_AC","error","Data From All Axes are Not Present. Detailed error at line {}: {}".format(line,str(e)))
        return None

    #... Transforming to 0 means ...#
##    x=center(x)
##    y=center(y)
##    z=center(z)
    
    #... Storing values of x, y and z in an array ...#
    
    df_xyz=column_stack((x,y,z))
    
    #... Calculating r, theta and phi
    
    r=sqrt(array(x)**2+array(y)**2+array(z)**2)
    theta=arctan(array(y)/array(x))
    phi=arccos(array(z)/r)
    
    def theta_transform(theta_val):
        theta_val=theta_val-pi/2 if theta_val>pi/4 else theta_val
        theta_val=theta_val+pi/2 if theta_val<-pi/4 else theta_val
        return theta_val
    
    def phi_transform(phi_val):
        phi_val=phi_val-pi/2 if phi_val>pi/2 else phi_val
        phi_val=phi_val+pi/2 if phi_val<pi/4 else phi_val
        return phi_val
    
    #... Transforming phi and theta to their suitable ranges ...#
    
    theta_tr=list(map(theta_transform,theta))                       #... Values of theta greater than pi/4 now lie between -pi/4 and 0, and those less than -pi/4 now lie between 0 and pi/4
    phi_tr=list(map(phi_transform,phi))                             #... Values of phi greater than pi/2 are now greater than 0 and those less than pi/4 are now less than 3*pi/4
    
    #... Storing the values of r, theta and phi in an array ...#
    
    df_polar=column_stack((r,theta_tr,phi_tr))

    if 0 < (df_polar[:,0]>3).sum() < (df_polar[:,0]<=3).sum():        #... If no. of datapoints having r values > 3 is less than that of the rest, then these datapoints are considered outliers
    
        df_polar=df_polar[df_polar[:,0]<=3] ; print("Outlier Detected")                        #... removing outliers
    
    #... Giving the complete dictionary of x,y,z and r,theta,phi values
    
    return {'df_polar':df_polar,'df_xyz':df_xyz}


#... Extracting various features from json for AC ...#
    
def AC_Feature_Extractions_From_Input(json_data,subassembly_instance,gauge_rule_list,current_datetime_stamp,stored_object,log_json):
    try:
        ac_raw_data_extraction_called=Generate_r_theta_phi_xyz_AC(json_data,subassembly_instance,log_json,current_datetime_stamp)
        polar_coordinates_df=ac_raw_data_extraction_called['df_polar']                  #... Array containing r, theta and phi ...#
        xyz_df=ac_raw_data_extraction_called['df_xyz']                                  #... Array containing x, y and z ...#
        num_obs=xyz_df.shape[0]
        
        #... Dividing batch in 200 partitions ...#
        
        partition_rows=hstack((int0(arange(num_obs,step=num_obs/200)),num_obs))
        ldf={}
        partition_r_var=[]
        ldf_xyz={}
        pmax_stat=[]
        for j in range(len(partition_rows)-1):
            ldf[j]=polar_coordinates_df[partition_rows[j]:partition_rows[j+1],:]
            ldf_xyz[j]=xyz_df[partition_rows[j]:partition_rows[j+1],:]
            try:
                partition_r_var.append(round(var(ldf[j][:,0],ddof=1),5))                           #... Calculating variance of r for each partition ...#
                partition_abs_median=abl(apply_along_axis(median,arr=ldf[j][:,1:3],axis=0))
                pmax_stat.append(partition_abs_median.max()/partition_abs_median.sum())
            except:
                if j in ldf:
                    del ldf[j]
                if j in ldf_xyz:
                    del ldf_xyz[j]
                
                if len(partition_r_var)==j+1:
                    partition_r_var.pop()
                break
        
        #.. Making New Partitions of AC Data for calculating Chain Misalignment Gauge FOr DryerSense ..#

        partition_rows=hstack((int0(arange(num_obs,step=num_obs/20)),num_obs))
        mov_var_r=[]
        
        def run_var(x,window):
            rvar=[]
            for i in range(len(x)-window+1):
                rvar.append(var(x[i:(i+window)],ddof=1))
            
            return rvar
        
        for j in range(len(partition_rows)-1):
            tdf=polar_coordinates_df[partition_rows[j]:partition_rows[j+1],:]
            if tdf.shape[0]>5:
                mov_var_r=mov_var_r+run_var(tdf[:,0],min(40,tdf.shape[0]-5))
            else:
                mov_var_r=nanvar(tdf[:,0],ddof=1)
        
        mov_var_median_centred=(median(mov_var_r)-0.011176039)/0.011176039
        
        #... storing for 144 batches ...#
        
        stored_object['ac_machine_utilization_cutoff']['historical_ts'][str(current_datetime_stamp)]=partition_r_var
        key=sorted(list(stored_object['ac_machine_utilization_cutoff']['historical_ts'].keys()))[0]
        while len(stored_object['ac_machine_utilization_cutoff']['historical_ts'])>144 or current_datetime_stamp-int(key)>24*60*60:
            del stored_object['ac_machine_utilization_cutoff']['historical_ts'][key]
            key=sorted(list(stored_object['ac_machine_utilization_cutoff']['historical_ts'].keys()))[0]
            
        partition_r_var_ts=stored_object["ac_machine_utilization_cutoff"]["historical_ts"]
        partition_r_var_values=unseq(stored_object["ac_machine_utilization_cutoff"]["historical_ts"])
        if json_data['header']['manufacturer']=='retrofit':
            if len(partition_r_var_ts)>=24*6:    #... not using algo until data of 1 day is stored               
                from scipy.signal import argrelextrema                                 
                valleys_indices=argrelextrema(array(partition_r_var_values),less)[0]
                valleys=array(partition_r_var_values)[valleys_indices]
                current_batch_cut_off=mean(valleys[:min(3,len(valleys))])
                
                #... high value is given to implicitly make machine idle
                
                current_batch_cut_off_filtered=10**6 if current_batch_cut_off<=0.001 else current_batch_cut_off if current_batch_cut_off<=0.008 else 0.008
                previous_batch_cut_off=stored_object["machine_properties"]["ac_mac_util_cutoff_previous"]
                previous_batch_cut_off_filtered=10**6 if previous_batch_cut_off<=0.001 else previous_batch_cut_off if previous_batch_cut_off<=0.008 else 0.008
                current_batch_effective_cutoff=nanmin([previous_batch_cut_off_filtered,current_batch_cut_off_filtered])
                mac_util_indicator=1
            
            else:
                current_batch_effective_cutoff=0.008
                mac_util_indicator=0
            
        else:
            current_batch_effective_cutoff=0.008
            mac_util_indicator=int(len(partition_r_var_ts)>24*6)
        
        stored_object["machine_properties"]["ac_mac_util_cutoff_previous"]=current_batch_effective_cutoff              #... updating stored cutoff
        
        #... Machine Stationarity calculations ...#
        
        try:
            cutoff_var_r=float(gauge_rule_list['Machine Utilization'][subassembly_instance]["utilization_cutoff"])
            
        except:
            cutoff_var_r=current_batch_effective_cutoff
            
        partitions_more_than_cutoff=where(array(partition_r_var)>cutoff_var_r)[0]
        percent_of_partition_machine_running=100*len(partitions_more_than_cutoff)/len(partition_r_var)
        
        #... Data after deleting the machine-not-running partitions ...#
        
        
        if percent_of_partition_machine_running!=0:
            machine_running_data_raw=zeros(polar_coordinates_df.shape[1])
            machine_running_data_xyz_raw=zeros(xyz_df.shape[1])                                         #... Calculate these only if percent > 0, otherwise undefined column ...#
            for key in partitions_more_than_cutoff:
                machine_running_data_raw=row_stack((machine_running_data_raw,ldf[key]))
                machine_running_data_xyz_raw=row_stack((machine_running_data_xyz_raw,ldf_xyz[key]))
            
            machine_running_data=machine_running_data_raw[1:,:]
            machine_running_data_xyz=machine_running_data_xyz_raw[1:,:]
            
        else:
            machine_running_data=array([])
            machine_running_data_xyz=array([])
        #... Forcing machine running percent based on ambient temperature ...#
        
        if "Blower" in json_data["subassemblies"]:
            mean_ambient_temperature=json_data["subassemblies"]["Blower"]["collectors"]["ir1"]["Ambient Temperature"]["MEAN"]
            if Non_Negative_Out_Of_Range_Exception_Checking(mean_ambient_temperature)==0:           #... Mean ambient temperature is numeric ...#
                if percent_of_partition_machine_running<=20 and mean_ambient_temperature>60:
                    percent_of_partition_machine_running=100
                    machine_running_data=polar_coordinates_df
                    machine_running_data_xyz=xyz_df

        #.. Constructing AC Stat List, where mr stands for machine running ..#
        ac_feature_list={'raw_data':polar_coordinates_df,
                      'raw_data_xyz':xyz_df,
                      'machine_running_data':machine_running_data,
                      'machine_running_data_xyz':machine_running_data_xyz,
                      'machine_running_time_percentage':percent_of_partition_machine_running,
                      'mr_crest_factor_x':quantile(machine_running_data_xyz[:,0],0.98)/sqrt(mean(machine_running_data_xyz[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_y':quantile(machine_running_data_xyz[:,1],0.98)/sqrt(mean(machine_running_data_xyz[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_z':quantile(machine_running_data_xyz[:,2],0.98)/sqrt(mean(machine_running_data_xyz[:,2]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_skewness_r':skew(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_kurtosis_r':kurtosis(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_r':quantile(machine_running_data[:,0],0.98)/sqrt(mean(machine_running_data[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_rms_r':sqrt(mean(machine_running_data[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_skewness_theta':skew(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_kurtosis_theta':kurtosis(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_theta':quantile(machine_running_data_xyz[:,1],0.98)/sqrt(mean(machine_running_data_xyz[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_rms_theta':sqrt(mean(machine_running_data[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_var_theta':var(machine_running_data[:,1],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'mr_var_r':var(machine_running_data[:,0],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'mr_median_r':median(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_quantile_3rd_r':quantile(machine_running_data[:,0],0.75) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_r':std(machine_running_data[:,0],ddof=1)/mean(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_theta':std(machine_running_data[:,1],ddof=1)/mean(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_phi':std(machine_running_data[:,2],ddof=1)/mean(machine_running_data[:,2]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_quantile_1st_phi_standardised':quantile(machine_running_data[:,2],0.25)/std(machine_running_data[:,2],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'pmax_stat':pmax_stat,
                      'mac_util_indicator':mac_util_indicator
                      }

        #... Special stats calculation ...#
        
        rtc_and_absolute_area_of_asymmetry_called=apply_along_axis(RTC_And_Absolute_Area_Asymmetry_Calculation,axis=0,arr=polar_coordinates_df)
        summary_stats_called=apply_along_axis(lambda x:{'mean':nanmean(x),
                                                        'median':nanmedian(x),
                                                        'variance':nanvar(x,ddof=1),
                                                        'skewness':float(skew(x,nan_policy='omit')),
                                                        'kurtosis':float(kurtosis(x,nan_policy='omit')),
                                                        'rms':sqrt(nanmean(x**2)),
                                                        'percentile_5':nanquantile(x,0.05),
                                                        'percentile_25':nanquantile(x,0.25),
                                                        'percentile_75':nanquantile(x,0.75),
                                                        'percentile_95':nanquantile(x,0.95),
                                                        'max':max(x),
                                                        'min':min(x),
                                                        'crest':nanquantile(x,0.98)/sqrt(mean(x**2))
                                                        },axis=0,arr=polar_coordinates_df)
        
        ac_data_rate_per_sec=40
        ac_data_loss_percent=100*max(1-num_obs/(ac_data_rate_per_sec*60*10),0)
        modified_crest_factor_r=sqrt((ac_feature_list['mr_crest_factor_x'])**2+(ac_feature_list['mr_crest_factor_y'])**2+(ac_feature_list['mr_crest_factor_x'])**2)
        
        summary_stats_called_xyz=apply_along_axis(lambda t: {'rms':sqrt(nanmean((t-nanmean(t))**2)),
                                                             'percentile_99':nanquantile(t-nanmean(t),0.99)
                                                             },arr=xyz_df,axis=0)
        
        special_stats_list={'rtc_r':rtc_and_absolute_area_of_asymmetry_called[0]['rtc'],
                            'abs_area_of_asymmetry_r':rtc_and_absolute_area_of_asymmetry_called[0]['abs_area_of_asymmetry'],
                            'rtc_theta':rtc_and_absolute_area_of_asymmetry_called[1]['rtc'],
                            'abs_area_of_asymmetry_theta':rtc_and_absolute_area_of_asymmetry_called[1]['abs_area_of_asymmetry'],
                            'rtc_phi':rtc_and_absolute_area_of_asymmetry_called[2]['rtc'],
                            'abs_area_of_asymmetry_phi':rtc_and_absolute_area_of_asymmetry_called[2]['abs_area_of_asymmetry'],
                            'ac_data_loss_percent':ac_data_loss_percent,
                            'modified_crest_factor_r':modified_crest_factor_r,
                            'summary_stats_called':summary_stats_called,
                            'summary_stats_called_xyz':summary_stats_called_xyz,
                            'mov_var_median':mov_var_median_centred
                            }
        
        ac_feature_list.update(special_stats_list)
        return ac_feature_list
    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,current_datetime_stamp,"AC_Feature_Extractions_From_Input","error",error_text)
        #print("Error in AC_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None


def Generate_r_theta_phi_xyz_XL(json_data,subassembly_instance,log_json,current_datetime_stamp):                #... Input is a formatted json ...#
    u=json_data["subassemblies"][subassembly_instance]["collectors"]["xl1"]["CSV"]
    try:
        x=list(map(lambda a:float(a[1]),u))
        y=list(map(lambda a:float(a[2]),u))
        z=list(map(lambda a:float(a[3]),u))
    except Exception as e:
        import sys
        line=sys.exc_info()[2].tb_lineno
        add_log_info(log_json,current_datetime_stamp,"Generate_r_theta_phi_xyz_XL","error","Data From All Axes are Not Present. Detailed error at line {}: {}".format(line,str(e)))
        return None        

##    x=center(x)
##    y=center(y)
##    z=center(z)
    
    #... Storing values of x, y and z in an array ...#
    
    df_xyz=column_stack((x,y,z))
    
    #... Calculating r, theta and phi
    
    r=sqrt(array(x)**2+array(y)**2+array(z)**2)
    theta=arctan(array(y)/array(x))
    phi=arccos(array(z)/r)
    
    def theta_transform(theta_val):
        theta_val=theta_val-pi/2 if theta_val>pi/4 else theta_val
        theta_val=theta_val+pi/2 if theta_val<-pi/4 else theta_val
        return theta_val
    
    def phi_transform(phi_val):
        phi_val=phi_val-pi/2 if phi_val>pi/2 else phi_val
        phi_val=phi_val+pi/2 if phi_val<pi/4 else phi_val
        return phi_val
    
    #... Transforming phi and theta to their suitable ranges ...#
    
    theta_tr=list(map(theta_transform,theta))                       #... Values of theta greater than pi/4 now lie between -pi/4 and 0, and those less than -pi/4 now lie between 0 and pi/4
    phi_tr=list(map(phi_transform,phi))                             #... Values of phi greater than pi/2 are now greater than 0 and those less than pi/4 are now less than 3*pi/4
    
    #... Storing the values of r, theta and phi in an array ...#
    
    df_polar=column_stack((r,theta_tr,phi_tr))

    if 0 < (df_polar[:,0]>3).sum() < (df_polar[:,0]<=3).sum():        #... If no. of datapoints having r values > 3 is less than that of the rest, then these datapoints are considered outliers
    
        df_polar=df_polar[df_polar[:,0]<=3]                         #... removing outliers
    
    #... Giving the complete dictionary of x,y,z and r,theta,phi values
    
    return {'df_polar':df_polar,'df_xyz':df_xyz}


#... Extracting various features from json for xl ...#
    
def XL_Feature_Extractions_From_Input(json_data,subassembly_instance,gauge_rule_list,current_datetime_stamp,stored_object,log_json):
    try:
        xl_raw_data_extraction_called=Generate_r_theta_phi_xyz_XL(json_data,subassembly_instance,log_json,current_datetime_stamp)
        polar_coordinates_df=xl_raw_data_extraction_called['df_polar']                  #... Array containing r, theta and phi ...#
        xyz_df=xl_raw_data_extraction_called['df_xyz']                                  #... Array containing x, y and z ...#
        num_obs=xyz_df.shape[0]
        
        #... Dividing batch in 200 partitions ...#
        
        partition_rows=hstack((int0(arange(num_obs,step=num_obs/200)),num_obs))
        ldf={}
        partition_r_var=[]
        ldf_xyz={}
        pmax_stat=[]
        for j in range(len(partition_rows)-1):
            ldf[j]=polar_coordinates_df[partition_rows[j]:partition_rows[j+1],:]
            ldf_xyz[j]=xyz_df[partition_rows[j]:partition_rows[j+1],:]
            try:
                partition_r_var.append(round(var(ldf[j][:,0],ddof=1),5))                           #... Calculating variance of r for each partition ...#
                partition_abs_median=abl(apply_along_axis(median,arr=ldf[j][:,1:3],axis=0))
                pmax_stat.append(partition_abs_median.max()/partition_abs_median.sum())
            except:
                if j in ldf:
                    del ldf[j]
                if j in ldf_xyz:
                    del ldf_xyz[j]
                
                if len(partition_r_var)==j+1:
                    partition_r_var.pop()
                break

        #... Making New Partitions of XL Data For Chain Misalignment Gauge of DryerSense ...#
        partition_rows=hstack((int0(arange(num_obs,step=num_obs/20)),num_obs))
        mov_var_r=[]
        
        def run_var(x,window):
            rvar=[]
            for i in range(len(x)-window+1):
                rvar.append(var(x[i:(i+window)],ddof=1))
            
            return rvar
        
        for j in range(len(partition_rows)-1):
            tdf=polar_coordinates_df[partition_rows[j]:partition_rows[j+1],:]
            if tdf.shape[0]>5:
                mov_var_r=mov_var_r+run_var(tdf[:,0],min(40,tdf.shape[0]-5))
            else:
                mov_var_r=nanvar(tdf[:,0],ddof=1)
        
        mov_var_median_centred=(median(mov_var_r)-0.011176039)/0.011176039
        
        #... storing for 144 batches ...#
        
        stored_object['xl_machine_utilization_cutoff']['historical_ts'][str(current_datetime_stamp)]=partition_r_var
        key=sorted(list(stored_object['xl_machine_utilization_cutoff']['historical_ts'].keys()))[0]
        while len(stored_object['xl_machine_utilization_cutoff']['historical_ts'])>144 or current_datetime_stamp-int(key)>24*60*60:
            del stored_object['xl_machine_utilization_cutoff']['historical_ts'][key]
            key=sorted(list(stored_object['xl_machine_utilization_cutoff']['historical_ts'].keys()))[0]

        partition_r_var_ts=stored_object["xl_machine_utilization_cutoff"]["historical_ts"]
        partition_r_var_values=unseq(stored_object["xl_machine_utilization_cutoff"]["historical_ts"])
        if json_data['header']['manufacturer']=='retrofit':
            if len(partition_r_var_ts)>=24*6:   #... not using algo until data of 1 day is stored
                from scipy.signal import argrelextrema                                 
                valleys_indices=argrelextrema(array(partition_r_var_values),less)[0]
                valleys=array(partition_r_var_values)[valleys_indices]
                current_batch_cut_off=mean(valleys[:min(3,len(valleys))])
                
                #... high value is given to implicitly make machine idle
                
                current_batch_cut_off_filtered=10**6 if current_batch_cut_off<=0.001 else current_batch_cut_off if current_batch_cut_off<=0.008 else 0.008
                previous_batch_cut_off=stored_object["machine_properties"]["xl_mac_util_cutoff_previous"]
                previous_batch_cut_off_filtered=10**6 if previous_batch_cut_off<=0.001 else previous_batch_cut_off if previous_batch_cut_off<=0.008 else 0.008
                current_batch_effective_cutoff=nanmin([previous_batch_cut_off_filtered,current_batch_cut_off_filtered])
                mac_util_indicator=1
            
            else:
                current_batch_effective_cutoff=0.008
                mac_util_indicator=0
            
        else:
            current_batch_effective_cutoff=0.008
            mac_util_indicator=int(len(partition_r_var_ts)>24*6)
        
        stored_object["machine_properties"]["xl_mac_util_cutoff_previous"]=current_batch_effective_cutoff              #... updating stored cutoff
        
        #... Machine Stationarity calculations ...#
        
        try:
            cutoff_var_r=float(gauge_rule_list['Machine Utilization'][subassembly_instance]["utilization_cutoff"])
            
        except:
            cutoff_var_r=current_batch_effective_cutoff
            
        partitions_more_than_cutoff=where(array(partition_r_var)>cutoff_var_r)[0]
        percent_of_partition_machine_running=100*len(partitions_more_than_cutoff)/len(partition_r_var)
        
        #... Data after deleting the machine-not-running partitions ...#
        
        if percent_of_partition_machine_running!=0:
            machine_running_data_raw=zeros(polar_coordinates_df.shape[1])
            machine_running_data_xyz_raw=zeros(xyz_df.shape[1])                                         #... Calculate these only if percent > 0, otherwise undefined column ...#
            for key in partitions_more_than_cutoff:
                machine_running_data_raw=row_stack((machine_running_data_raw,ldf[key]))
                machine_running_data_xyz_raw=row_stack((machine_running_data_xyz_raw,ldf_xyz[key]))
            
            machine_running_data=machine_running_data_raw[1:,:]
            machine_running_data_xyz=machine_running_data_xyz_raw[1:,:]
            
        else:
            machine_running_data=array([])
            machine_running_data_xyz=array([])
        #... Forcing machine running percent based on ambient temperature ...#
        
        if "Blower" in json_data["subassemblies"]:
            mean_ambient_temperature=json_data["subassemblies"]["Blower"]["collectors"]["ir1"]["Ambient Temperature"]["MEAN"]
            if Non_Negative_Out_Of_Range_Exception_Checking(mean_ambient_temperature)==0:           #... Mean ambient temperature is numeric ...#
                if percent_of_partition_machine_running<=20 and mean_ambient_temperature>60:
                    percent_of_partition_machine_running=100
                    machine_running_data=polar_coordinates_df
                    machine_running_data_xyz=xyz_df
                    
        
        #... Creating XL Stat List, where mr stands for machine running ...#
        
        xl_feature_list={'raw_data':polar_coordinates_df,
                      'raw_data_xyz':xyz_df,
                      'machine_running_data':machine_running_data,
                      'machine_running_data_xyz':machine_running_data_xyz,
                      'machine_running_time_percentage':percent_of_partition_machine_running,
                      'mr_crest_factor_x':quantile(machine_running_data_xyz[:,0],0.98)/sqrt(mean(machine_running_data_xyz[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_y':quantile(machine_running_data_xyz[:,1],0.98)/sqrt(mean(machine_running_data_xyz[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_z':quantile(machine_running_data_xyz[:,2],0.98)/sqrt(mean(machine_running_data_xyz[:,2]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_skewness_r':skew(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_kurtosis_r':kurtosis(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_r':quantile(machine_running_data[:,0],0.98)/sqrt(mean(machine_running_data[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_rms_r':sqrt(mean(machine_running_data[:,0]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_skewness_theta':skew(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_kurtosis_theta':kurtosis(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_crest_factor_theta':quantile(machine_running_data_xyz[:,1],0.98)/sqrt(mean(machine_running_data_xyz[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_rms_theta':sqrt(mean(machine_running_data[:,1]**2)) if percent_of_partition_machine_running!=0 else -10,
                      'mr_var_theta':var(machine_running_data[:,1],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'mr_var_r':var(machine_running_data[:,0],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'mr_median_r':median(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_quantile_3rd_r':quantile(machine_running_data[:,0],0.75) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_r':std(machine_running_data[:,0],ddof=1)/mean(machine_running_data[:,0]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_theta':std(machine_running_data[:,1],ddof=1)/mean(machine_running_data[:,1]) if percent_of_partition_machine_running!=0 else -10,
                      'mr_coeff_of_var_phi':std(machine_running_data[:,2],ddof=1)/mean(machine_running_data[:,2]) if percent_of_partition_machine_running!=0 else -10,               
                      'mr_quantile_1st_phi_standardised':quantile(machine_running_data[:,2],0.25)/std(machine_running_data[:,2],ddof=1) if percent_of_partition_machine_running!=0 else -10,
                      'pmax_stat':pmax_stat,
                      'mac_util_indicator':mac_util_indicator
                      }
        
        #... Special stats calculation ...#
        
        
        rtc_and_absolute_area_of_asymmetry_called=apply_along_axis(RTC_And_Absolute_Area_Asymmetry_Calculation,axis=0,arr=polar_coordinates_df)
        summary_stats_called=apply_along_axis(lambda x:{'mean':nanmean(x),
                                                        'median':nanmedian(x),
                                                        'variance':nanvar(x,ddof=1),
                                                        'skewness':float(skew(x,nan_policy='omit')),
                                                        'kurtosis':float(kurtosis(x,nan_policy='omit')),
                                                        'rms':sqrt(nanmean(x**2)),
                                                        'percentile_5':nanquantile(x,0.05),
                                                        'percentile_25':nanquantile(x,0.25),
                                                        'percentile_75':nanquantile(x,0.75),
                                                        'percentile_95':nanquantile(x,0.95),
                                                        'max':max(x),
                                                        'min':min(x),
                                                        'crest':nanquantile(x,0.98)/sqrt(mean(x**2))
                                                        },axis=0,arr=polar_coordinates_df)#.tolist()
        
        xl_data_rate_per_sec=2000
        xl_data_loss_percent=100*max(1-num_obs/(xl_data_rate_per_sec*60*10),0)
        modified_crest_factor_r=sqrt((xl_feature_list['mr_crest_factor_x'])**2+(xl_feature_list['mr_crest_factor_y'])**2+(xl_feature_list['mr_crest_factor_x'])**2)
        
        summary_stats_called_xyz=apply_along_axis(lambda t: {'rms':sqrt(nanmean((t-nanmean(t))**2)),
                                                             'percentile_99':nanquantile(t-nanmean(t),0.99)
                                                             },arr=xyz_df,axis=0)#.tolist()
        
        special_stats_list={'rtc_r':rtc_and_absolute_area_of_asymmetry_called[0]['rtc'],
                            'abs_area_of_asymmetry_r':rtc_and_absolute_area_of_asymmetry_called[0]['abs_area_of_asymmetry'],
                            'rtc_theta':rtc_and_absolute_area_of_asymmetry_called[1]['rtc'],
                            'abs_area_of_asymmetry_theta':rtc_and_absolute_area_of_asymmetry_called[1]['abs_area_of_asymmetry'],
                            'rtc_phi':rtc_and_absolute_area_of_asymmetry_called[2]['rtc'],
                            'abs_area_of_asymmetry_phi':rtc_and_absolute_area_of_asymmetry_called[2]['abs_area_of_asymmetry'],
                            'xl_data_loss_percent':xl_data_loss_percent,
                            'modified_crest_factor_r':modified_crest_factor_r,
                            'summary_stats_called':summary_stats_called,
                            'summary_stats_called_xyz':summary_stats_called_xyz,
                            'mov_var_median':mov_var_median_centred
                            }
        
        xl_feature_list.update(special_stats_list)
        return xl_feature_list

    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,current_datetime_stamp,"XL_Feature_Extractions_From_Input","error",error_text)
        #print("Error in XL_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None



def IR_Feature_Extractions_From_Input(input_json,subassembly_instance,log_json):
    try:
        ir_u=input_json['subassemblies'][subassembly_instance]['collectors']['ir1']['CSV']
        n = len(ir_u)
        ir_timestamp=[];u=[];amb_temp_data=[]
        for i in range(0,len(ir_u)):
            ir_timestamp.append(ir_u[i][0])
            u.append(float(ir_u[i][1]))
            amb_temp_data.append(float(ir_u[i][2]))
        
        ir_data_rate_per_sec = 1
        amb_median = median(amb_temp_data)  
        ir_data_loss_percent = min( 100*( 1 - n/( ir_data_rate_per_sec*60*10 ) ), 100 )
        
        ir_feature_list = {'ir_temp_data' : u,
                           'ir_timestamp' : ir_timestamp,
                           'ir_amb_temp_data' : amb_temp_data,
        
                           'max_temp' : max( u ),
                           
                           'max_amb_temp' : max( amb_temp_data ),
                           
                           'ambient_median_temp' : amb_median,
                           
                           'ir_data_loss_percent' : ir_data_loss_percent,
                           }
        return ir_feature_list

    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,input_json["header"]["timestamp"],"IR_Feature_Extractions_From_Input","error",error_text)
        #print("Error in IR_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None
      
# HY input processing and feature extraction
    
def HY_Feature_Extractions_From_Input(input_json,subassembly_instance,log_json):
    try:
        hy_u=input_json['subassemblies'][subassembly_instance]['collectors']['hy1']['CSV']
        n = len(hy_u)
        hy_timestamp=[];u=[]
        for i in range(0,len(hy_u)):
            hy_timestamp.append(hy_u[i][0])
            u.append(float(hy_u[i][1]))
            
        
        hy_data_rate_per_sec = 2
      
        hy_data_loss_percent = min( 100*( 1 - n/( hy_data_rate_per_sec*60*10 ) ), 100 )
        
        hy_feature_list = {'hy_data' : u,
                           'hy_timestamp' : hy_timestamp,
                           
        
                           'max_humidity' : max( u ),
                           
                           'hy_data_loss_percent' : hy_data_loss_percent,
                           }
        return hy_feature_list

    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,input_json["header"]["timestamp"],"HY_Feature_Extractions_From_Input","error",error_text)
        #print("Error in HY_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None

    
#hy_features_list = HY_Feature_Extractions_From_Input(input_json,subassembly_instance)


def VAC_Feature_Extractions_From_Input( input_json,subassembly_instance,gauge_rule_list, stored_object, current_datetime_stamp, log_json ):

    try:

        def Vac_Processing(input_json,subassembly_instance):

            
            vac_u=input_json['subassemblies'][subassembly_instance]['collectors']['vac1']['CSV']
            n = len(vac_u)
            vac_timestamp=[];u=[]
            for i in range(0,len(vac_u)):
                vac_timestamp.append(vac_u[i][0])
                u.append(float(vac_u[i][1]))
            vac_timestamp=array(vac_timestamp)
            u=array(u)

            #a = -2.036 ; b = 29.92

            #u = a*u + b            #PSI to Inch Hg Conversion            

            #... VAC Adujustment with BP ...#
##            subassembly_instance_list=list(input_json['subassemblies'].keys())
##            subassembly_instance_name_bp = None
##            subassembly_instance_name_bp = [ i for i in subassembly_instance_list if 'bp1' in input_json['subassemblies'][i]['collectors'] and isinstance(input_json['subassemblies'][i]['collectors']['bp1'],dict) and "CSV" in input_json['subassemblies'][i]['collectors']['bp1'] and input_json['subassemblies'][i]['collectors']['bp1']['CSV']!=[]]
##
##            if( subassembly_instance_name_bp != None and subassembly_instance_name_bp!=[]):     #..... detecting name of subassembly instance which contains bp
##
##                bp_stat_list = BP_Feature_Extractions_From_Input( input_json, subassembly_instance_name_bp[0] )
##
##                bp_median_0 = bp_stat_list['bp_median']
##        
##                bp_median = bp_median_0*( 29.92/1013.25 )    #........ bp_median_0*( 14.7/1013.25 )*( 1/14.7 )*29.92 ..... 1013.25 HexaPascal = 14.7 PSI
##
##            else:
##
##                bp_median = None
##
##            group= input_json['header']['group']

##            if group != 'qc'  and current_datetime_stamp - int( stored_object["machine_properties"]["vac_zero_last_set_time"] ) > 24*60*60 and bp_median !=None and bp_median > 13 and bp_median < 15.5 and abs( bp_median - float( stored_object["machine_properties"]["last_bp_median"] ) ) > 0.05  and int( stored_object["machine_properties"]["vac_last_machine_idle_time"] ) >= int( stored_object["machine_properties"]["vac_zero_last_set_time"] ):
##
##                vac_zero_set_compute_indicator = True
##
##                stored_object["machine_properties"]["vac_zero_last_set_time"] = current_datetime_stamp
##
##                stored_object["machine_properties"]["last_bp_median"] = bp_median
##
##            else:
##
##                vac_zero_set_compute_indicator = False

            #print("vac_zero_set_compute_indicator: ",vac_zero_set_compute_indicator)


               #..... Initialize vac_zero_set by previous known value

##            if( vac_zero_set_compute_indicator ):    #.... if vac_zero_set should be computed for this batch
##
##                vac_zero_delta_factor =  float(stored_object["machine_properties"]["vac_zero_set_delta"])           #..... Initialize vac_zero_set by previous known value
##
##                vac_median_inch = median( u ) ;  bp_median_inch = a*bp_median + b
##
##                vac_zero_delta_factor = bp_median_inch - vac_median_inch    #.... unit is in inch
##
##                stored_object["machine_properties"]["vac_zero_set_delta"] =  vac_zero_delta_factor   #.... updating vac_zero_set_delta
##
##            else:
##
##                vac_zero_delta_factor = 0                                   #... Kept temporarily, to avoid change in values due to inclusion of vac_zero_delta_factor in stored_objects when bp data is not present

            #print("vac_zero_delta_factor: ",vac_zero_delta_factor)
            #u = u + vac_zero_delta_factor          ## BP Adjustment
##
        
##    
##                
##          # Exception Checking
            vac_data_numeric_input_exception_indicator = Numeric_Input_Type_Checking(u)
          
            vac_data_out_of_range_exception_indicator = VAC_Data_Out_Of_Range_Exception_Checking( u )
            
            combined_input_exception_indicator = 0 if( (vac_data_numeric_input_exception_indicator + vac_data_out_of_range_exception_indicator) == 0) else 1
            
            if combined_input_exception_indicator == 0:
                n1=len(u[u<=-1])
                rejection_l = 100 * ( n1 / n ) # lower bound : % of values <=-1  
                n2=len(u[u>=14])
                rejection_u = 100 * ( n2 / n ) # upper bound : % of values >=14
                if( rejection_l <= 70 ): 
                        if(len(u[u<0])>0):
                          u[u<=-1]=0
                          u[ u > -1 ] = u[u>-1] - min(u)
                        
                if(rejection_u<=70):
                        u[u>14] = 14 
            
                vac_data_status = 'Sufficient Data'
                  
                vac_data = u
                
                if( ( rejection_l > 70 ) | ( rejection_u > 70 ) ):
                      vac_data_status = 'Insufficient Data'
            else:
                vac_data_status = 'Insufficient Data'
                rejection_l=100
                rejection_u=100
                vac_data = []
        
            output = { 'vac_data_status' : vac_data_status,
                         
                         'vac_timestamp' : vac_timestamp,
                         
                         'vac_data' : vac_data,
                         
                         'num_obs' : len( u ),
                         
                         'rejection_l' : rejection_l,
                         
                         'rejection_u' : rejection_u,
                         
                         
                         }
      
            return  output
       
        processed_vac_data=Vac_Processing(input_json,subassembly_instance)
        
        vac_data_status = processed_vac_data['vac_data_status']
        num_obs = processed_vac_data['num_obs']
          
        rejection_l = processed_vac_data['rejection_l']
        rejection_u = processed_vac_data['rejection_u']
        vac_timestamp = processed_vac_data['vac_timestamp']
        vac_data = processed_vac_data['vac_data']
        
        if VAC_Data_Out_Of_Range_Exception_Checking(vac_data)==1:
            vac_data_status='Insufficient Data'
        #....... there should be sufficient amount of vac data to calculate various stats ......
            
        maximas=[]
          
        if  vac_data_status != "Insufficient Data" :
            
            raw_vac_data = vac_data  
            o = len( raw_vac_data )
            d=kde(vac_data.tolist())
            d_arg=d['arguments']
            d_den=d['densities']
            from scipy.signal import argrelextrema
            peaks = argrelextrema(array(d['densities']), greater, mode="wrap")[0]
                
            peak_values=list(map(lambda i: d['densities'][i],peaks))

            sorted_peak_values=sorted(peak_values,reverse=True)

            maximas=list(map(lambda x: round(d_arg[d_den.index(x)],1) if x in d_den else -1,sorted_peak_values[:min(4,len(sorted_peak_values))]))

            if len(maximas)<4:
                maximas.extend([-1]*(4-len(maximas)))

            #maximas=uniq_maxima
            
            peak_value_numeric_input_exception_indicator = Numeric_Input_Type_Checking( maximas )
            
            if peak_value_numeric_input_exception_indicator==0:
            
                m=array(maximas)
                
                positive_maximas = m[ m > 0 ]
                
                high_vac = max( positive_maximas )
                low_vac = min( positive_maximas )
                
                count_high_vac = len(raw_vac_data[(raw_vac_data >= (high_vac - 0.5) )& (raw_vac_data <= (high_vac + 0.5))])
                  
                count_low_vac = len(raw_vac_data[(raw_vac_data >= (low_vac - 0.5) )& (raw_vac_data <= (low_vac + 0.5))])
                  
                count_1st_maxima = len(raw_vac_data[(raw_vac_data >= (maximas[0] - 0.5) )& (raw_vac_data <= (maximas[0] + 0.5))])
                  
                count_2nd_maxima = len(raw_vac_data[(raw_vac_data >= (maximas[1] - 0.5) )& (raw_vac_data <= (maximas[1] + 0.5))])
                  
                count_3rd_maxima = len(raw_vac_data[(raw_vac_data >= (maximas[2] - 0.5) )& (raw_vac_data <= (maximas[2] + 0.5))])
                  
                count_4th_maxima = len(raw_vac_data[(raw_vac_data >= (maximas[3] - 0.5) )& (raw_vac_data <= (maximas[3] + 0.5))])
                  
                count_green = len( raw_vac_data[raw_vac_data <= 11] )
                  
                count_yellow = len(raw_vac_data[(raw_vac_data > 11 )& (raw_vac_data <= 12)])
                  
                count_red = len( raw_vac_data[raw_vac_data > 12] )
                  
                  #........"Green", "Yellow" & "Red" thresholds for 24 hr. Operating Vacuum Gauge
                  
                percent_green = 100*( count_green / o )
                percent_yellow = 100*( count_yellow / o ) 
                percent_red = 100*( count_red / o )
                  
                  #........ Thresholds for 24 hr. Pump Utilization Gauge
                  
                percent_high_vac = 100*( count_high_vac / o ) 
                percent_low_vac = 100*( count_low_vac / o )
                  
                  #........ Thresholds for Vacuum Frequency Gauge
                  
                percent_1st_maxima = 100*( count_1st_maxima / o ) 
                percent_2nd_maxima = 100*( count_2nd_maxima / o )
                percent_3rd_maxima = 100*( count_3rd_maxima / o )
                percent_4th_maxima = 100*( count_4th_maxima / o )
                  
                 #....... high vacuum detection ......
                  
                high_vac_cutoff = 3
                  
                values_greater_than_cutoff = raw_vac_data[ raw_vac_data >= high_vac_cutoff ] 
                
                percent_values_greater_than_cutoff = 100*len( values_greater_than_cutoff )/len( raw_vac_data )
                try:
                    high_vacuum_red_threshold = float(gauge_rule_list["High Vacuum"][subassembly_instance]["red_threshold"])
                except:
                    high_vacuum_red_threshold = 3
                
                if( high_vac < 2 ):
                    
                          vac_features_list = { 
                          
                          "vac_timestamp" : vac_timestamp.tolist(),
                          
                          "vac_data" : raw_vac_data.tolist(),
                          
                          "percent_lb_rejection" : rejection_l,
                          
                          "percent_ub_rejection" : rejection_u,
                          
                          "high_vac" : 0, "low_vac" : 0,
                          
                          "maxima_1" : 0, "maxima_2" : 0, "maxima_3" : 0, "maxima_4" : 0,
                          
                          "percent_high_vac" : 100, "percent_low_vac" : 100,
                          
                          "percent_1st_maxima" : 25, "percent_2nd_maxima" : 25, "percent_3rd_maxima" : 25, "percent_4th_maxima" : 25,
                          
                          "percent_green" : percent_green, "percent_yellow" : percent_yellow, "percent_red" : percent_red,'vac_data_status': 'Sufficient Data',
                          
                          "vac_extra_features" : {  "percent_values_greater_than_cutoff" : percent_values_greater_than_cutoff,
                                                       
                                                       "high_vacuum_red_threshold" : high_vacuum_red_threshold }
                          
                       }
                        
                else:
                    
                          vac_features_list = {
                          
                          "vac_timestamp" : vac_timestamp.tolist(),
                          
                          "vac_data" : raw_vac_data.tolist(),
                          
                          "percent_lb_rejection" : round( rejection_l, 1 ),
                          
                          "percent_ub_rejection" : round( rejection_u, 1 ),
                          
                          "high_vac" : round( high_vac, 1 ) , "low_vac" : round( low_vac, 1 ),
                          
                          "maxima_1" : round( maximas[0], 1 ), "maxima_2" : round( maximas[1], 1 ), "maxima_3" : round( maximas[2], 1 ), "maxima_4" : round( maximas[3], 1 ),
                          
                          "percent_high_vac" : round( percent_high_vac, 1 ), "percent_low_vac" : round( percent_low_vac, 1 ),
                          
                          "percent_1st_maxima" : round( percent_1st_maxima, 1 ), "percent_2nd_maxima" : round( percent_2nd_maxima, 1 ),
                          
                          "percent_3rd_maxima" : round( percent_3rd_maxima, 1 ), "percent_4th_maxima" : round( percent_4th_maxima, 1 ),
                          
                          "percent_green" : round( percent_green, 1 ), "percent_yellow" : round( percent_yellow, 1 ),
                          
                          "percent_red" : round( percent_red, 1 ), 'vac_data_status': 'Sufficient Data',
                          
                          "vac_extra_features" : { "percent_values_greater_than_cutoff" : percent_values_greater_than_cutoff,
                                                       
                                                       "high_vacuum_red_threshold" : high_vacuum_red_threshold }}
                          
                    
            
          
        else:
            vac_features_list={'vac_timestamp' : vac_timestamp.tolist(),
                                     'vac_data' : None,
                                     "percent_lb_rejection" : 100  ,
                                     "percent_ub_rejection" : 100,
                                     'vac_data_status': 'Insufficient Data'}
            
          #..... Extra calculations ........          
        
        vac_data_rate_per_sec = 2
          
        vac_data_loss_percent = min( 100*( 1 - num_obs/( vac_data_rate_per_sec*60*10 ) ), 100 )
          
        
        vac_features_list.update({'vac_data_loss_percent': vac_data_loss_percent,'vac_median': median(vac_data ) if vac_data!=[] else None})
          
        return vac_features_list

    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,current_datetime_stamp,"VAC_Feature_Extractions_From_Input","error",error_text)
        #print("Error in VAC_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None


def BP_Feature_Extractions_From_Input( json_data, subassembly_instance, log_json ):

    try:
        u = json_data["subassemblies"][subassembly_instance]["collectors"]["bp1"]["CSV"]

        #bptime=list(map(lambda x:x[0],u))
          
        bp_u=list(map(lambda x:float(x[1]),u))

        n = len(u)

        bp_data_rate_per_sec = 1

        bp_data_loss_percent = min( [100*( 1 - n/( bp_data_rate_per_sec*60*10 ) ), 100] )

        feature_list = {     'bp_data' : bp_u,

                           'bp_data_loss_percent' : bp_data_loss_percent,
                           
                           'bp_median' : median( bp_u ) if bp_u != [] else None  }
                           

        return feature_list

    except Exception:
# =============================================================================
#         import sys
#         exec_info=sys.exc_info()[2]
#         line=exec_info.tb_lineno
# =============================================================================
        error_text=format_exc()
        add_log_info(log_json,json_data["header"]["timestamp"],"BP_Feature_Extractions_From_Input","error",error_text)
        #print("Error in BP_Feature_Extractions_From_Input at line {}: {}".format(line,str(e)))
        return None


