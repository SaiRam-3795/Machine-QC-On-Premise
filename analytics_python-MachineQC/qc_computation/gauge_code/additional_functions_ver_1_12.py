
''' This module contains additional functions required while processing Input Data '''

#... Importing Several Modules ...#

from numpy import array, isfinite, genfromtxt, nanmax, diff, sign, median, std, nan, isnan#, percentile, linspace, exp
from scipy.signal import find_peaks
from qc_computation.gauge_code.kernel_density_user_defined_3 import kde
from qc_computation.gauge_code.exception_handler import Numeric_Input_Type_Checking
global stored_object  


def whch(np_array,val1,val2):
    count=0
    for element in np_array:
        if(element>val1 and element<=val2):
            count+=1
    return count


def RTC_And_Absolute_Area_Asymmetry_Calculation(working_parameter):
    if len(working_parameter)>=2:
        working_density=kde(working_parameter)
        dens_arg=working_density['arguments']

        try:
            working_parameter_median=median(dens_arg)
            working_parameter_sd=std(dens_arg,ddof=1)

            #... Computation regarding ratio of tail area to centre area ( RTC ) ...#
            
            area_at_centre=sum([working_density['densities'][l] for l in range(len(dens_arg)) if dens_arg[l]>working_parameter_median-working_parameter_sd and dens_arg[l]<working_parameter_median+working_parameter_sd])
            area_at_tail=sum([working_density['densities'][l] for l in range(len(dens_arg)) if dens_arg[l]<=working_parameter_median-1.5*working_parameter_sd or dens_arg[l]>=working_parameter_median+1.5*working_parameter_sd])
            rtc_value=10*area_at_tail/area_at_centre if area_at_centre!=0 else nan                 #... multiplied by 10 to make the value bigger ...#
            rtc=rtc_value if not isnan(rtc_value) and rtc_value<=3 else nan                        #... to check against abnormally high value ...#
            
            #... Computation regarding area of asymmetry ( AS ) ...#
            
            peaks_density=find_peaks(working_density['densities'])[0]                 
            max_peak_value_index=max(peaks_density)
            area_below_max_peak=sum(working_density['densities'][:max_peak_value_index])/sum(working_density['densities'])
            area_above_max_peak=sum(working_density['densities'][max_peak_value_index:])/sum(working_density['densities'])
            abs_area_of_asymmetry=abs(area_above_max_peak-area_below_max_peak)

        except Exception as e:
            import sys
            exec_info=sys.exc_info()[2]
            line=exec_info.tb_lineno
            print("An Exception has occured in RTC_And_Absolute_Area_Asymmetry_Calculation from {} at line {}: {}".format(__name__,line,str(e)))
            rtc=nan
            abs_area_of_asymmetry=nan
        
    else:
        rtc=nan
        abs_area_of_asymmetry=nan
    
    return {'rtc':rtc,'abs_area_of_asymmetry':abs_area_of_asymmetry}

def Machine_Stationary(stat_list):
    machine_status_indicator="Call Algo" if stat_list["machine_running_time_percentage"]>20 else "Machine Idle"
    return machine_status_indicator

def Machine_Stationary_VAC(vac_features_list,subassembly_instance,stored_object,gauge_rule_list,current_datetime_stamp):
    if "high_vac" in vac_features_list:
        batch_high_vac=vac_features_list["high_vac"]
    else:
        batch_high_vac=None

    if Numeric_Input_Type_Checking([batch_high_vac])==0:
        try:
            vac_load_cutoff=float(gauge_rule_list["24 Hour Pump Utilization"][subassembly_instance]["load_threshold"])
        except:
            vac_load_cutoff=2
        vac_machine_stationary_indicator="No" if batch_high_vac>vac_load_cutoff else "Yes"

    else:
        vac_machine_stationary_indicator="No"

    if vac_machine_stationary_indicator=="Yes":
        stored_object["machine_properties"]["vac_last_machine_idle_time"]=current_datetime_stamp

    return {"vac_machine_stationary_indicator":vac_machine_stationary_indicator}


def Overlap_Adjustment(gauge_values,number_of_maximas):

    try:
        total_maxima=sum(gauge_values)
        extra_percent_to_deduct=(total_maxima-100)/number_of_maximas
        gauge_values_adj=list(map(lambda x: x-extra_percent_to_deduct,gauge_values))
        gauge_values_new=list(map(lambda x: x if x>=0 else 0,gauge_values_adj))
        if round(sum(gauge_values_new,3)<=100):
            return gauge_values_new
        else:
            return Overlap_Adjustment(gauge_values_new,number_of_maximas)

    except:
        return gauge_values
    
 
                


