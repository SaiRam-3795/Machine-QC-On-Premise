# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 15:28:27 2019

@author: HERO
"""


import re,time

def Gauge_Rule_Extraction(gauge_details_json):
    gauge_rule_df={}
    
    machine_inputs_api_called = gauge_details_json.copy()
    
    gauge_rule_api_called = machine_inputs_api_called['gaugeUserInputs']
    machine_inputs_events_called = machine_inputs_api_called['eventInstances']
    
    if len(gauge_rule_api_called)!=0:
        for i in range(0,len(gauge_rule_api_called)):
            
            gauge_output_name = gauge_rule_api_called[i]['pmFunction']["name"]
        
            subassembly_instance = gauge_rule_api_called[i]["subAssemblyInstance"]["name"]
      
            gauge_rule_name = gauge_rule_api_called[i]["userRequiredInput"]["name"]
      
            gauge_rule_value=gauge_rule_api_called[i]["value"]
            
            temp={gauge_output_name:{subassembly_instance:{gauge_rule_name:gauge_rule_value}}}

            if gauge_output_name not in gauge_rule_df:

                gauge_rule_df.update(temp)

            else:

                if subassembly_instance not in gauge_rule_df[gauge_output_name]:

                    gauge_rule_df[gauge_output_name].update(temp[gauge_output_name])

                else:

                    gauge_rule_df[gauge_output_name][subassembly_instance].update(temp[gauge_output_name][subassembly_instance])
                    
    else:      
        gauge_rule_df={}
    
    if len(machine_inputs_events_called)!=0:
        machine_inputs_processed_list={}
        for k in range(0,len(machine_inputs_events_called)):
            current_event_name = machine_inputs_events_called[k]['name']
            current_event_value = machine_inputs_events_called[k]["event_value"] if machine_inputs_events_called[k]["event_value"]!=None else machine_inputs_events_called[k]['name'] 
            current_batch_time = machine_inputs_events_called[k]["timestamp"].split('T')
            current_event_timestamp=int(time.strftime(current_batch_time[0] + re.sub("Z", "", current_batch_time[1])))
            current_event_info={'value' : current_event_value, 'timestamp' : current_event_timestamp}
            mi_dic={current_event_name:current_event_info}
            machine_inputs_processed_list.update(mi_dic)
    
    else:
        machine_inputs_processed_list={}
        
        
    gauge_details_list={'gauge_rule_list':gauge_rule_df,'machine_inputs_list':machine_inputs_processed_list}    
    
    return(gauge_details_list)
