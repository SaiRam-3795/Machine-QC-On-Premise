

''' This module contains functions which call and compute gauges to be published in Output JSON '''


import traceback
from qc_computation.gauge_code.exception_handler import Numeric_Input_Type_Checking, Non_Negative_Out_Of_Range_Exception_Checking


from qc_computation.gauge_code.kernel_density_user_defined_3 import kde
global stored_object 
from qc_computation.gauge_code.additional_functions_ver_1_12 import Machine_Stationary
from numpy import array, nanmedian, nanstd, greater
from scipy.signal import argrelextrema
from DatahubQCLogger import AnalyticsLogger


from qc_computation.gauge_code.log_info_updation import add_log_info

global log_json


logger = AnalyticsLogger('localhost:5000', '/home/pi/machinesense_qc_middleware/analytics/qc_input_configuration.json')   


def Vibration_Quality_Control(ac_stat_list,subassembly_instance,subassembly):
    try:
        raw_data = ac_stat_list['raw_data']      #...... Data frame containing r, theta and phi
        if len(raw_data[:,0])>2:
            r_density = kde( raw_data[:,0] )
            #r_density=kde(raw_data)
            arg=r_density['arguments']
            dens=array(r_density['densities'])
            median=nanmedian(arg)
            stdev=nanstd(arg,ddof=1)
            index_centre=[index for index,value in enumerate(arg) if value>median-stdev and value<median+stdev]
            area_at_centre=sum(dens[index_centre])
            index_tail=[index for index,value in enumerate(arg) if value<=median-(1.5*stdev) or value>=median+(1.5*stdev)]
            area_at_tail=sum(dens[index_tail])
            
            rtc_10=10*(area_at_tail/area_at_centre)
            rtc_10_color='green' if rtc_10<=0.075 else 'red'
            #===check this========
            #index=max(argrelextrema(dens,np.greater)[0])
            local_max=argrelextrema(dens,greater)[0]
            index=max(local_max)
            
            area_below_max_peak=sum(dens[:index])/sum(dens)
            area_above_max_peak=sum(dens[(index+1):len(dens)])/sum(dens)
            abs_area_of_asymmetry=abs(area_above_max_peak-area_below_max_peak)
            abs_area_of_asymmetry_color='green' if abs_area_of_asymmetry<=0.6 else 'red'
            vib_qc_color='red' if rtc_10_color=='red' and abs_area_of_asymmetry_color=='red' else 'green'
            combined_input_exception_indicator = 0
        else:
            combined_input_exception_indicator = 1
        machine_stationary_status = Machine_Stationary( ac_stat_list )
        
        if machine_stationary_status == "Machine Idle":
            output= {"color":["red"], "value":[-1],"stats":{'rtc_10':rtc_10,'abs_area_of_asymmetry':abs_area_of_asymmetry}, "trendExistence":False,"purpose":"qc","success":"true","reason":"MachineIdle" }
        elif combined_input_exception_indicator == 0:
            output= {"color":[vib_qc_color], "value":[-1],"stats":{'rtc_10':rtc_10,'abs_area_of_asymmetry':abs_area_of_asymmetry}, "trendExistence":False, "purpose":"qc","success":"true" }
        else:
            output= {"color": ["red"],"value":[-1],"trendExistence":False,"purpose":"qc", "success":"true","reason":"Insufficient Data"}
            
    except Exception:
        #traceback.print_exc()
        output= {"color": ["green"],"value":[-1],"trendExistence":False,"purpose":"qc", "success":"false","reason":"Insufficient Data"}
    
    output.update({'subassemblyInstance':subassembly_instance, 'subassembly':subassembly})
    
    return output
    
    
    
def Vacuum_Quality_Control(vac_stat_list,subassembly_instance,subassembly,gauge_rule_list={}):                     #intermediate_stored_objects, , subassembly_instance, gauge_rule_list):
    try:
        
        if gauge_rule_list is None:
            gauge_rule_list={}
            
        vac_load_cutoff = int(gauge_rule_list.get("Vacuum Pump Utilization Under Load (24 hrs)",{}).get(subassembly_instance,{}).get("load_threshold",2))
            
        if Numeric_Input_Type_Checking( [vac_stat_list['high_vac']] ) == 0:
            vac_machine_stationary_indicator = 'No' if  vac_stat_list['high_vac'] > vac_load_cutoff else 'Yes'
        else:
            vac_machine_stationary_indicator = 'No'            
            
        
        #if not (vac_machine_stationary_indicator == 'Yes' or vac_stat_list['vac_data_status'] == 'Insufficient Data'):
        batch_high_vac=vac_stat_list["high_vac"] 
        
        #........... One Time specification needed ........
        non_autobaseline_numeric_inputs = [batch_high_vac]
        
        #....... Exception Handling .............. 0 for no errors and 1 for at least one error ........
        non_autobasline_numeric_inputs_exception_indicator = Numeric_Input_Type_Checking(non_autobaseline_numeric_inputs)
        numeric_out_of_range_exception_indicator = Non_Negative_Out_Of_Range_Exception_Checking(non_autobaseline_numeric_inputs )
        
        combined_input_exception_indicator=0 if numeric_out_of_range_exception_indicator+non_autobasline_numeric_inputs_exception_indicator==0 else 1
        
        if combined_input_exception_indicator==0:
            
            #========================need to discuss==============================
            
            vacuum_qc_color='green' if (batch_high_vac>=5 and batch_high_vac<=7) or (batch_high_vac>=9 and batch_high_vac<=13) else 'red'
            
        #===========to check for idle condition of the machine================       
        #machine_stationary_vac_called = MachineStationary_status_vac( input_json, intermediate_stored_objects, vac_stat_list, subassembly_instance, gauge_rule_list)
        #machine_stationary = machine_stationary_vac_called['vac_machine_stationary_indicator']
        output_conditions={'combined_input_exception_indicator':combined_input_exception_indicator}#, 'machine stationary':machine_stationary}

        if output_conditions['combined_input_exception_indicator']==1:
            output={"color":["red"], "value":[-1], "trendExistence":False, "success":"true", "puprose":"qc", "reason":"High Vac calculation not possible due to insufficient data"}
        #elif output_conditons['machine stationary']=='Yes':
            #output={"color":"red", "value":[-1], "stats":{"high_vac" : batch_high_vac }, "trendExistence":False, "success":"true", "purpose":"qc", "reason":"Low Vac Detected"}
        elif vac_machine_stationary_indicator == 'Yes':
            output={"color":["red"], "value":[-1], "stats":{"high_vac":batch_high_vac}, "trendExistence":False, "success" : "true", "purpose":"qc" ,"reason" :"Low Vac Detected" }
        else:
            output= {"color":[vacuum_qc_color], "value":[-1], "stats":{"high_vac":batch_high_vac}, "trendExistence":False, "success" : "true", "purpose":"qc"}
        
        
        
#=================need to write catch========================
        
    except Exception:
        #traceback.print_exc()
        output = {"color": ["green"],"value":[-1],"purpose":"qc", "success":"false", "trendExistence": False,"reason" : 'Insufficient Data.'}
 
    output.update({'subassemblyInstance':subassembly_instance, 'subassembly':subassembly})
    
    return output
 
def QC_Gauge_Output( subassembly_instance,Vibration_QC_output,Vacuum_QC_output,ac_stat_list,vac_stat_list ):
    try:
        
        final_output = {}
        
        if ac_stat_list is not None:
            gauge_name_list = subassembly_instance + ":  " + 'Vibration QC'
            final_output[gauge_name_list] =  Vibration_QC_output  
    
        if vac_stat_list is not None:
            gauge_name_list = subassembly_instance +':  ' + 'Vacuum QC'
            final_output[gauge_name_list] = Vacuum_QC_output
            

        return final_output
    except:
        traceback.print_exc()
        return {}
    
    
def QC_Output_Processing(input_json,stored_object,subassembly_instance,qc_gauge_output):
    qc_gauge_name="vibration_qc" if "machinesense" in subassembly_instance.lower() else "vacuum_qc"
    qc_external_gauge_name=subassembly_instance + ":  " + ('Vibration QC' if "machinesense" in subassembly_instance.lower() else 'Vacuum QC')
    if 'previous_state_count' not in stored_object[qc_gauge_name]:
        stored_object[qc_gauge_name]["previous_state_count"] = 0

        
    current_qc_state_count=int(input_json["header"].get("qc_profile",{}).get("flagStateCount",0))
    
    previous_qc_state_count = stored_object[qc_gauge_name]['previous_state_count']
    
    if current_qc_state_count != previous_qc_state_count:   #..... if state_count increases => change in state has occurred => reset gauges
        reset_gauge_indicator = "Yes"
    else:
        reset_gauge_indicator = "No"    #.... if no change in state_count => state is same => gauges may or may not reset. For now initialize to "No"
   
    if  current_qc_state_count % 2 == 1:               #..... if odd reset gauge and don't progress i.e. NULL output
        reset_gauge_indicator = "Yes"  
        gauge_null_output = "Yes"     #..... Note: previous reset_gauge_indicator value is overwritten and it should be
    else:
        gauge_null_output = "No"                                #.... if even there is non-NULL output
  
    if reset_gauge_indicator == "Yes":
        
        qc_gauge_output[qc_external_gauge_name]["progress"] = 0          #..... progress is reset to 0 in gauge_output
        
        
        stored_object[qc_gauge_name]['qc_gauge_progress_percent'] = 0    #....... progress is reset to 0 in stored_object
    
    
        stored_object[qc_gauge_name]['qc_gauge_green_percent'] = 0     #....... green percent is reset to 0 in stored_object
    
    
        stored_object[qc_gauge_name]['qc_gauge_red_percent'] = 0     #....... red percent is reset to 0 in stored_object
    
   #...... Updating previous state_count in stored_object with current values ........
  
    stored_object[qc_gauge_name]["previous_state_count"] = current_qc_state_count
  
  
  #...... Final processing ......
  
    if  gauge_null_output == "No":    #.... case when there is no NULL output
    
        return { 'qc_gauge_output':qc_gauge_output, 'updated_stored_objects':stored_object }
    
    else:         #....... case when there is NULL output
    
        return  {'qc_gauge_output': {}, 'updated_stored_objects': stored_object }
    

#... Mapping Internal Gauge Names To External Gauge Names For Color Modification With Respect To Gauge User Inputs ...#




# Calculation of Gauges for Meta-Parameters
def Gauge_Computation(input_json,model,assembly,subassembly,subassembly_instance,manufacturer,gauge_rule_list,stored_object,ac_stat_list,xl_stat_list,ir_stat_list,hy_stat_list,vac_stat_list,current_datetime_stamp,outlier_indicator_ac,machine_stationary_ac,outlier_indicator_xl,machine_stationary_xl,machine_stationary_vac,log_json):

    try:
        
        manufacturer_list = ['oem','retrofit']
        #list_of_subassembly = ['blower','componentanalyzer','machinesense','cdablower','processblower','genericmotor','advancedcomponentanalyzer','regenblower','machinebody','vfd','cassettemotor','gearbox','nvfd','motorpanel','boosterblower','vacuumsense','vacuumanalyzer']
        #assembly_list = ['All','componentanalyzer','vacuum-pump','advancedcomponentanalyzer','machinesense','railcar','vacuumanalyzer']
        
        #machineid = input_json["header"]["machineId"]

        
        if manufacturer in manufacturer_list:
                        
                
            if ac_stat_list is not None or xl_stat_list is not None:
                
                stat_list=ac_stat_list if ac_stat_list is not None else xl_stat_list
                vibration_QC_gauge_progress_percent = stored_object['vibration_qc']['qc_gauge_progress_percent']
                vibration_QC_gauge_green_percent = stored_object['vibration_qc']['qc_gauge_green_percent']
                vibration_QC_gauge_red_percent = stored_object['vibration_qc']['qc_gauge_red_percent']

                if vibration_QC_gauge_progress_percent<100:  
                    #vibration_qc_object=VibrationQualityControl()
                    #Vibration_QC_output = vibration_qc_object.Vibration_Quality_Control(ac_stat_list)
                    Vibration_QC_output = Vibration_Quality_Control(stat_list,subassembly_instance,subassembly)
                    #==================need to discuss============================
                    if Vibration_QC_output['success']=='true':
                        stored_object['vibration_qc']['qc_gauge_progress_percent']=vibration_QC_gauge_progress_percent+10
                         
                        if Vibration_QC_output['color']==['green']: 
                            stored_object['vibration_qc']['qc_gauge_green_percent']=vibration_QC_gauge_green_percent+10
                            logger.log('Vibration Machine QC process running : [Green] ' + str(stored_object['vibration_qc']['qc_gauge_green_percent'])+ " %", 'success', False, None)
                        # else: 
                        #     stored_object['vibration_qc']['qc_gauge_green_percent']=vibration_QC_gauge_green_percent
                         
                        if Vibration_QC_output['color']==['red']:
                            stored_object['vibration_qc']['qc_gauge_red_percent']=vibration_QC_gauge_red_percent+10
                            logger.log('Vibration Machine QC process running : [Red] ' + str(stored_object['vibration_qc']['qc_gauge_red_percent'])+ " %", 'failed', False, None)
                        # else: 
                        #     stored_object['vibration_qc']['qc_gauge_red_percent']=vibration_QC_gauge_red_percent
                    
                    if stored_object['vibration_qc']['qc_gauge_green_percent']>=30:
                        stored_object['vibration_qc']['qc_gauge_progress_percent']=100
                    
                    if stored_object['vibration_qc']['qc_gauge_red_percent']>=80:
                        stored_object['vibration_qc']['qc_gauge_progress_percent']=100
                    
                    if  stored_object['vibration_qc']['qc_gauge_progress_percent']==100:
                         
                        if stored_object['vibration_qc']['qc_gauge_red_percent'] >= 80:
                            Vibration_QC_output['color']=['red']
                            logger.log('Vibration Machine QC process completed', 'success', True, False)
                        else: 
                            Vibration_QC_output['color']=['green']   
                            logger.log('Vibration Machine QC process completed', 'success', True, True)
                        
                        logger.update_qc_input_configuration(False)
                    
                    #=========================need to do======================================
                    Vibration_QC_output['progress']=stored_object['vibration_qc']['qc_gauge_progress_percent']
                    
                    if model == "MVP":
                        Vibration_QC_output["progress"] = 100
                        Vibration_QC_output["color"] = ['green']
                        Vibration_QC_output["reason"] = None
                        stored_object['vibration_qc']["qc_gauge_progress_percent"] = 100
                    #custom_json_obj.update_json(path_name,stored_object)
# =============================================================================
#                         if 'MVP' in modelName:
#                             Vibration_QC_output_instant_called = QC_Instant_Pass(Vibration_QC_output,'vibration_qc',stored_object)
#                             Vibration_QC_output = Vibration_QC_output_instant_called['qc_gauge_output']
#                             stored_object = Vibration_QC_output_instant_called['updated_stored_objects_0']
# =============================================================================
# =============================================================================
#                         print("vibration qc")
#                         print(Vibration_QC_output)
                else:
                    Vibration_QC_output={}
                    
                
            else:
                Vibration_QC_output={}
                stat_list=None
                
            if vac_stat_list is not None:
                
                #vac_stat_list = VAC_Feature_Extractions_From_Input(input_json,i)

                #===================this needs to be done===========================
                vacuum_QC_gauge_progress_percent = stored_object['vacuum_qc']['qc_gauge_progress_percent']
                vacuum_QC_gauge_green_percent = stored_object['vacuum_qc']['qc_gauge_green_percent']
                vacuum_QC_gauge_red_percent = stored_object['vacuum_qc']['qc_gauge_red_percent']
                
                if vacuum_QC_gauge_progress_percent<100:    
                    Vacuum_QC_output=Vacuum_Quality_Control(vac_stat_list,subassembly_instance,subassembly,gauge_rule_list)
                    #==================need to discuss============================
                    
                    if Vacuum_QC_output['success']=='true':
                        stored_object['vacuum_qc']['qc_gauge_progress_percent']=vacuum_QC_gauge_progress_percent+10
                        if Vacuum_QC_output['color']==['green']: 
                            stored_object['vacuum_qc']['qc_gauge_green_percent']=vacuum_QC_gauge_green_percent+10
                            logger.log('Vacuum Machine QC process running : [Green] ' + str(stored_object['vacuum_qc']['qc_gauge_green_percent'])+ " %", 'success', False, None)
                        # else: 
                        #     stored_object['vibration_qc']['qc_gauge_green_percent']=vibration_QC_gauge_green_percent
                         
                        if Vacuum_QC_output['color']==['red']:
                            stored_object['vacuum_qc']['qc_gauge_red_percent']=vacuum_QC_gauge_red_percent+10
                            logger.log('Vacuum Machine QC process running : [Red] ' + str(stored_object['vacuum_qc']['qc_gauge_red_percent'])+ " %", 'failed', False, None)
                        #stored_object['vacuum_qc']['qc_gauge_green_percent']=vacuum_QC_gauge_green_percent+10 if Vacuum_QC_output['color']==['green'] else vacuum_QC_gauge_green_percent
                        #stored_object['vacuum_qc']['qc_gauge_red_percent']=vacuum_QC_gauge_red_percent+10 if Vacuum_QC_output['color']==['red'] else vacuum_QC_gauge_red_percent
                    
                    if stored_object['vacuum_qc']['qc_gauge_green_percent']>=30:
                        stored_object['vacuum_qc']['qc_gauge_progress_percent']=100
                    
                    if stored_object['vacuum_qc']['qc_gauge_red_percent']>=80:
                        stored_object['vacuum_qc']['qc_gauge_progress_percent']=100
                    
                    if  stored_object['vacuum_qc']['qc_gauge_progress_percent']==100:
                        #Vacuum_QC_output['color']=['red'] if stored_object['vacuum_qc']['qc_gauge_red_percent'] >= 80 else ['green']
                        if stored_object['vacuum_qc']['qc_gauge_red_percent'] >= 80:
                            Vacuum_QC_output['color']=['red']
                            logger.log('Vacuum Machine QC process completed', 'success', True, False)
                        else: 
                            Vacuum_QC_output['color']=['green']   
                            logger.log('Vacuum Machine QC process completed', 'success', True, True)
                        
                        logger.update_qc_input_configuration(False)
                    
                    #=========================need to do======================================
                    Vacuum_QC_output['progress']=stored_object['vacuum_qc']['qc_gauge_progress_percent']
                    #custom_json_obj.update_json(path_name,stored_object)
                    
                    if model == "MVP":
                        Vacuum_QC_output["progress"] = 100
                        Vacuum_QC_output["color"] = ['green']
                        Vacuum_QC_output["reason"] = None
                        stored_object['vacuum_qc']["qc_gauge_progress_percent"] = 100
# =============================================================================
#                         if 'MVP' in modelName:
#                             Vacuum_QC_output_instant_called = QC_Instant_Pass(Vacuum_QC_output,'vacuum_qc',updated_stored_objects)
#                             Vacuum_QC_output = Vacuum_QC_output_instant_called['qc_gauge_output']
#                             updated_stored_objects = Vacuum_QC_output_instant_called['updated_stored_objects_0']
# # =============================================================================
# =============================================================================
#                         print("vacuum qc")
#                         print(Vacuum_QC_output)
                else: 
                    Vacuum_QC_output={}
                #vac_sensor_output=Vacuum_QC_output
            
            else:
                #vac_sensor_output=None
                Vacuum_QC_output={}
            
            qc_gauge_output = QC_Gauge_Output(subassembly_instance,Vibration_QC_output,Vacuum_QC_output,stat_list,vac_stat_list)
            processed_gauge_output = QC_Output_Processing( input_json, stored_object, subassembly_instance, qc_gauge_output )
            gauge_output=processed_gauge_output['qc_gauge_output']
# =============================================================================
#             for x in ['vibration_qc','vacuum_qc']:
#                 if stored_object[x]['qc_gauge_progress_percent']==100:
#                     stored_object[x]['qc_gauge_red_percent'] = 0
#                     stored_object[x]['qc_gauge_green_percent']=0
#                     stored_object[x]['qc_gauge_progress_percent']=0
# =============================================================================
                
            
        else:
            
            gauge_output = {}
            add_log_info(log_json,current_datetime_stamp,"Gauge_Computation","normal","manufacturer {} is not in the list of manufacturers in Gauge Computation".format(manufacturer))

    except Exception as e:
        
        import sys
        exec_info=sys.exc_info()[2]
        line=exec_info.tb_lineno
        error_text="An exception has occured in Gauge_Computation at line {}: {}".format(line,e)
        add_log_info(log_json,current_datetime_stamp,"Gauge_Computation","error",error_text)
        traceback.print_exc()
        gauge_output={}

    
    return gauge_output
