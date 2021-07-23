###################################################################################################
# File name: pump_analytics.py
# Created on: July 17, 2020
# Copyright: MachineSense
# Author: Sai Ram, Biswadeep Ghoshal
# Description: This is a python3 script, this script creates output JSON. It takes 
#              input JSON and creates the output JSON.        
###################################################################################################

import time, glob
import platform
from platform import system
from copy import deepcopy
import json
import sys
import os
import traceback

import warnings
warnings.filterwarnings("ignore")

import zmq


##################################################################################################

system_os = platform.system()

if system_os == 'Windows':
    print("System is Windows \n")
    json_details_path = "F:/MachineSense/qc_computation/json_data"
    working_dir = 'F:/MachineSense/qc_computation'
    log_file='F:/MachineSense/qc_computation/reason_log.json'
    config_path='F:/MachineSense/qc_computation/r_config.json'
else:
    print("System is Linux \n")
    json_details_path = '/var/json_data/'
    working_dir = '/home/pi/qc_computation/'
    log_file='/home/pi/qc_computation/reason_log.json'
    config_path="/var/r_config.json"
    
####################################################################################################
##                           Config Json File Existence and Reading Data
####################################################################################################

    
os.chdir(working_dir)
from qc_computation.gauge_code.api_processing_2 import *
from qc_computation.gauge_code.additional_functions_ver_1_12 import *
from qc_computation.gauge_code.Input_Processing import AC_Feature_Extractions_From_Input, XL_Feature_Extractions_From_Input, VAC_Feature_Extractions_From_Input
from qc_computation.gauge_code.exception_handler import *
from qc_computation.gauge_code.Gauge_Processing_qc import *
from qc_computation.gauge_code.vmart_output import *
from qc_computation.gauge_code.stored_objects_qc import * #stored_object, Stored_Object_Saving
from qc_computation.gauge_code.Outlier import Outlier_Indicator
from qc_computation.gauge_code.log_info_updation import read_log_info, add_log_info, remove_old_log_info, write_log_info

from DatahubQCLogger import AnalyticsLogger

logger = AnalyticsLogger('localhost:5000', '/usr/src/conf/qc_input_configuration.json')   

log_json=read_log_info(log_file)        #.. To store log of errors or issues ..#
old_log_json=deepcopy(log_json)

try:
    with open(config_path) as config:
        config_data=json.load(config)
except Exception:
    
    traceback.print_exc()
    config_data={}
#hy_data_rate_per_sec = config_info['hy_data_rate_per_sec']
#ac_data_rate_per_sec = config_info['ac_data_rate_per_sec']
#ir_data_rate_per_sec = config_info['ir_data_rate_per_sec']


context_jsonsender = zmq.Context()
jsonsender_socket = context_jsonsender.socket(zmq.PUSH)
jsonsender_socket.connect("tcp://localhost:" + config_data.get('json_sender_port',"4445"))
  
###################################################################################################
#                                       ZMQ Connections
###################################################################################################  
#        context_jsonreceiver = zmq.Context()
#        jsonreceiver_socket = context_jsonreceiver.socket(zmq.PULL)
#        jsonreceiver_socket.bind("tcp://*:" + config_info['json_receiver_port'])
#        


###################################################################################################
#                           Data Processing After Receiving Json Files
###################################################################################################


def main_output_qc(json_parsed):

    print("Called QC Main function")
    global log_json
    global old_log_json
    current_datetime_stamp=int(time.time())
    version_product=None
    input_json_write=config_data.get("write_qc_input_json",False)
    output_json_write=config_data.get("write_qc_output_json",False)
    end_qc=False
    
    try:
        
        if len(json_parsed) != 0:

            if system_os=="Linux" and input_json_write:
            
                input_json_folder=os.path.join(working_dir,"INPUT_JSON")
            
                if not os.path.isdir(input_json_folder):
                    try:
                        os.mkdir(input_json_folder)
                    except:
                        print("Permission Denied")
                
                file_name="{}_{}_input.json".format(json_parsed.get('header',{}).get('timestamp',0),json_parsed.get('header',{}).get('machineId'))
                try:
                    input_json_file=os.path.join(input_json_folder,file_name)
                    with open(input_json_file,"w") as f:
                        json.dump(json_parsed,f)
                except Exception as e:
                    print("Error Writing Input JSON: ",str(e))

            #... Input json header processing ...#
            #json_parsed["header"]["qc_profile"]["flagStateCount"]=0
            current_datetime_stamp = int(json_parsed['header']['timestamp'])
            group = json_parsed['header']['group']                   
            manufacturer = json_parsed['header']['manufacturer'].lower()
            modelName = json_parsed['header']['modelName']
            model="VPDB" if "VPDB" in modelName else "SVP" if "SVP" in modelName else "MVP" if "MVP" in modelName else "Default"
            subassembly_instance_list = list(json_parsed['subassemblies'].keys())
            assembly = json_parsed['header']['assemblyName'].lower()

            version_product="301.0000" if any(["componentanalyzer" in x.lower() for x in subassembly_instance_list]) else "302.0000"
            
            
            #... stored_objects_initialization ...#

            machineid = json_parsed["header"]["machineId"]

            stored_object_path="F:/MachineSense/qc_computation/stored_objects/stored_objects_{}.json".format(machineid) if system()=="Windows" else "/home/pi/qc_computation/stored_objects/stored_objects_{}.json".format(machineid)

            stored_object=read_stored_object(stored_object_path)

            
            #... Fetching the gauge details from the gauge details json files in the datahub ...#
            
            gauge_details_file_paths = glob.glob(os.path.join(json_details_path, 'gauge_details_*.json'))
            if gauge_details_file_paths!=[]:
                try:
                    gauge_details_jsons = list(map(lambda x: json.load(open(x)),gauge_details_file_paths))                              #.. Reading all the gauge details JSON files ..#
                    gauge_details_json=list(filter(lambda x: x['id']==json_parsed["header"]["machineId"],gauge_details_jsons))[0]       #.. Choosing the gauge details JSON for the current Input JSON Data by comparing the machine ids in Input and gauge details JSONs ..#
                    gauge_rule_list = Gauge_Rule_Extraction(gauge_details_json)['gauge_rule_list']
                except:
                    gauge_rule_list=None
            else:
                gauge_rule_list = None
                   
              
                                                                 
            count_qc=0                                                      #.. This count is defined just to know whether required collector data are present in Input JSON ..#
            #... Starting to Process Input Data to Publish Gauges and VMart Stats ...#
            
            if len(subassembly_instance_list) != 0:
                i = 0; combined_gauge_output = {}; combined_vmart_output = {}
                for i in range(len(subassembly_instance_list)):
                    subassembly_instance = subassembly_instance_list[i]
                    try:
                        subassembly = json_parsed['subassemblies'][subassembly_instance]['header']['subAssembly']['name'].lower()
                    except:
                        subassembly="".join([i for i in subassembly_instance if i.isalpha()]).lower()

                    if json_parsed['subassemblies'][subassembly_instance]['collectors']!={}:
                        collector_names = list(json_parsed['subassemblies'][subassembly_instance]['collectors'].keys())
                        
                        ac_key_indicator = int( 'ac1' in collector_names )
                        ir_key_indicator = int( 'ir1' in collector_names )
                        hy_key_indicator = int( 'hy1' in collector_names )
                        xl_key_indicator = int( 'xl1' in collector_names )
                        vac_key_indicator = int( 'vac1' in collector_names )
                        bp_key_indicator = int( 'bp1' in collector_names )
                        
                        
                        if group.lower() == "qc" and json_parsed["header"].get("qc_profile",{}) not in [{},None,"null"]:
                            ir_stat_list=None
                            hy_stat_list=None
                            bp_stat_list=None
                            
                            
                            if ac_key_indicator == 1 and isinstance(json_parsed['subassemblies'][subassembly_instance]['collectors']['ac1'],dict) and "CSV" in json_parsed['subassemblies'][subassembly_instance]['collectors']['ac1'] and json_parsed['subassemblies'][subassembly_instance]['collectors']['ac1']['CSV']!=[]:
                                ac_stat_list = AC_Feature_Extractions_From_Input(json_parsed,subassembly_instance,gauge_rule_list,current_datetime_stamp,stored_object,log_json)
                                outlier_indicator_ac = Outlier_Indicator(ac_stat_list) if ac_stat_list is not None else None
                                machine_stationary_ac = Machine_Stationary( ac_stat_list ) if ac_stat_list is not None else None
                               
                            else:
                                ac_stat_list = None; machine_stationary_ac = None; outlier_indicator_ac = None; count_qc+=1
                                
                            if xl_key_indicator == 1 and isinstance(json_parsed['subassemblies'][subassembly_instance]['collectors']['xl1'],dict) and "CSV" in json_parsed['subassemblies'][subassembly_instance]['collectors']['xl1'] and json_parsed['subassemblies'][subassembly_instance]['collectors']['xl1']['CSV']!=[]:
                                xl_stat_list = XL_Feature_Extractions_From_Input(json_parsed,subassembly_instance,gauge_rule_list,current_datetime_stamp,stored_object,log_json)
                                outlier_indicator_xl = Outlier_Indicator(xl_stat_list) if xl_stat_list is not None else None
                                machine_stationary_xl = Machine_Stationary( xl_stat_list ) if xl_stat_list is not None else None

                            else:
                                xl_stat_list = None; machine_stationary_xl = None; outlier_indicator_xl = None; count_qc+=1
                                
                            if vac_key_indicator == 1 and isinstance(json_parsed['subassemblies'][subassembly_instance]['collectors']['vac1'],dict) and "CSV" in json_parsed['subassemblies'][subassembly_instance]['collectors']['vac1'] and json_parsed['subassemblies'][subassembly_instance]['collectors']['vac1']['CSV']!=[]:
                                vac_stat_list = VAC_Feature_Extractions_From_Input( json_parsed,subassembly_instance,gauge_rule_list,stored_object,current_datetime_stamp,log_json )
                                machine_stationary_vac = Machine_Stationary_VAC( vac_stat_list,subassembly_instance,stored_object,gauge_rule_list,current_datetime_stamp ) if vac_stat_list is not None else None
                            else:
                                vac_stat_list = None
                                machine_stationary_vac = None
                                count_qc+=1
                                
                            gauge_output = Gauge_Computation(json_parsed,model,assembly,subassembly,subassembly_instance,manufacturer,gauge_rule_list,stored_object,ac_stat_list,xl_stat_list,ir_stat_list,hy_stat_list,vac_stat_list,current_datetime_stamp,outlier_indicator_ac,machine_stationary_ac,outlier_indicator_xl,machine_stationary_xl,machine_stationary_vac,log_json)
                            vmart_output = Vmart(json_parsed, subassembly_instance, ac_stat_list, ac_key_indicator, xl_stat_list, xl_key_indicator,vac_stat_list, vac_key_indicator, ir_stat_list, ir_key_indicator, hy_stat_list, hy_key_indicator, bp_stat_list, bp_key_indicator)

                            
                            
# =============================================================================
#                             elif group.lower() != "qc":
#                                 gauge_output={}
#                                 vmart_output={}
#                                 add_log_info(log_json,current_datetime_stamp,"main","normal","Unidentified group '{}'".format(group))
#                                 
# =============================================================================
                        else:
                            gauge_output={}
                            vmart_output={}
                            add_log_info(log_json,current_datetime_stamp,"main","normal","QC Profile Missing or Some Other Group {}".format(group))

                        
                        combined_gauge_output.update(gauge_output)
                        combined_vmart_output.update(vmart_output)
                        
                #... Gauge and VMart Stats Published ...#

                #... Output JSON ...#
                json_parsed["header"]["timestamp"]=int(json_parsed["header"]["timestamp"])            
                final_output={'header':json_parsed["header"],
                              'gauge': combined_gauge_output,
                              'machine_parameters' : None,
                              'subassemblies': combined_vmart_output,
                              'version':version_product,
                              'last modified':"13 July 2021"}

                #... Writing Stored Object For Use in Processing Further Batch of Data ...#
                Stored_Object_Saving(stored_object,stored_object_path)
                print("stored object updated successfully")
                
                                
                #... Creating Socket String For Output JSON ...#
                filename=str(json_parsed["header"]["timestamp"])+"_"+json_parsed["header"]["machineId"]+".json"
                socket_string="R~~output~~{}~~{}".format(filename,json.dumps(final_output))
                #... Sending Output JSON ...#
                if any([json_parsed['subassemblies'][subassembly_instance]['collectors']!={} for subassembly_instance in subassembly_instance_list]) and count_qc!=3*len(subassembly_instance_list):
                    
                    if output_json_write:
                        
                        output_json_folder=os.path.join(working_dir,"OUTPUT_JSON")
                        if not os.path.isdir(output_json_folder):
                            try:
                                os.mkdir(output_json_folder)
                            except:
                                print("Permission Denied")
                        output_json_filename=os.path.join(output_json_folder,"machine_qc_{}.json".format(json_parsed.get('header',{}).get('timestamp',0)))
                        
                        try:
                            with open(output_json_filename,"w") as out_conn:
                                json.dump(final_output, out_conn)
                            print("Output JSON written successfully")
                        except Exception as e:
                            print("Error Writing Output JSON: ",str(e))
                            
                    
                    jsonsender_socket.send_string(socket_string)
                    print("Output JSON sent successfully")
                else:
                    add_log_info(log_json,current_datetime_stamp,"main_output","normal","{}: Reqd Collector Data Not Present in Input JSON".format(json_parsed["header"]["machineId"]))
                    
            else:
                final_output = None
                add_log_info(log_json,current_datetime_stamp,"main_output","normal","{}: No Subassembly in Input JSON".format(json_parsed["header"]["machineId"]))
                
            
                
            if stored_object['vacuum_qc']['qc_gauge_progress_percent']==100 and stored_object['vibration_qc']["qc_gauge_progress_percent"]==100:
                
               
                if stored_object['vacuum_qc']['qc_gauge_green_percent'] >= 30 and stored_object['vibration_qc']['qc_gauge_green_percent'] >= 30:
                    logger.log('Overall Machine QC process completed', 'success', True, True)
                    
                
                else:
                    logger.log('Overall Machine QC process completed', 'success', True, False)
                
                
                end_qc=True
   
            
            #.. Else, if Vacuum QC Process Is Complete But Vibration QC has not started yet, meaning the AC Sensor is disconnected ..#
            elif stored_object['vacuum_qc']['qc_gauge_progress_percent']==100 and stored_object['vibration_qc']['qc_gauge_progress_percent']==0:
                
                if stored_object['vacuum_qc']['qc_gauge_green_percent'] >= 30:
                    logger.log('Overall Machine QC process completed only with Vacuum', 'success', True, True)
                
                else:
                    logger.log('Overall Machine QC process completed only with Vacuum', 'success', True, False)
                
                end_qc=True
                    
            
            #.. Else, if Vibration QC Process Is Complete But Vacuum QC has not started yet, meaning the VAC Sensor is disconnected ..#
            elif stored_object['vacuum_qc']['qc_gauge_progress_percent']==0 and stored_object['vibration_qc']['qc_gauge_progress_percent']==100:
                
                if stored_object['vibration_qc']['qc_gauge_green_percent'] >= 30:
                    logger.log('Overall Machine QC process completed only with Vibration', 'success', True, True)
                
                else:
                    logger.log('Overall Machine QC process completed only with Vibration', 'success', True, False)
                
                end_qc=True
                    
            
            #.. If none of the above are satisfied, it means QC is still in progress
            else:    
                logger.log('Overall Machine QC process completion pending', 'success', False, None)       
        else:
            final_output = None
            add_log_info(log_json,current_datetime_stamp,"main_output","normal","Input JSON is blank")


    except Exception:
        error_text=traceback.format_exc()
        add_log_info(log_json,current_datetime_stamp,"main_output","error",error_text)
        traceback.print_exc()

        #... Writing Input JSON and Output JSON for debugging purposes ...#
        
        if system_os=="Linux":
            
            input_json_folder=os.path.join(working_dir,"INPUT_JSON")
        
            if not os.path.exists(input_json_folder):
                try:
                    os.mkdir(input_json_folder)
                except:
                    print("Permission Denied")

            if "header" in json_parsed and "timestamp" in json_parsed["header"] and "machineId" in json_parsed["header"]:
                file_name=str(json_parsed["header"]["timestamp"])+"_"+json_parsed["header"]["machineId"]+"_input.json"
                try:
                    input_json_file=os.path.join(input_json_folder,file_name)
                    with open(input_json_file,"w") as f:
                        json.dump(json_parsed,f)
                except Exception as e:
                    print("Error Writing JSON: ",str(e))

                
                
    if system_os=="Linux":
                
        if version_product=="302.0000":
            with open("/usr/src/conf/analytics_version_vpa.txt", "w") as v_file:
                v_file.write("version={}".format(version_product))

        else:
            with open("/usr/src/conf/analytics_version_ca.txt", "w") as v_file:
                v_file.write("version={}".format(version_product))

        
    remove_old_log_info(log_json,current_datetime_stamp)        #.. Remove issues or errors older than 24 hours from the log ..#
    write_log_info(log_json,old_log_json,log_file)              #.. Write the currently stored error or issue logs to the log_file ..#
    
    #... Stopping QC Service When Machine QC completes ...#
    if end_qc:
        logger.update_qc_input_configuration(False)