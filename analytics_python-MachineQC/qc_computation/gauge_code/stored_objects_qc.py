# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 12:25:19 2020

@author: Biswadeep Ghoshal, SaiRam

This code reads an already existing stored object, or, creates one and initializes it
"""

from os import path
#from platform import system
from json import load, dump
#from numpy import array#, isnan
#from rstl import STL
global stored_object


#stored_object_path="F:/MachineSense/pumpsense_computation/stored_objects/stored_object.json" if system()=="Windows" else "/home/pi/pumpsense_computation/stored_objects/stored_object.json"


#stored_object_path="F:/MachineSense/pumpsense_computation/stored_objects/stored_objects_{}.json".format(machineid) if system()=="Windows" else "/home/pi/pumpsense_computation/stored_objects/stored_objects_{}.json".format(machineid)

#"stored_objects_{}".format(machineid)

def read_stored_object(stored_object_path):

    def checkKey(dict, key): 
          
        if key in dict: 
            return 1
        else: 
            return 0
        
    if path.exists(stored_object_path):
        try:
            with open(stored_object_path) as stored_file:
                stored_object=load(stored_file)
        except:
            stored_object={}
    else:
        stored_object={}
        
            
    qc_progress_properties = ['vibration_qc','vacuum_qc']
    
    
    for qc_prop in qc_progress_properties:
        
        if qc_prop not in stored_object:
            stored_object[qc_prop]= {}
            
        for prog_prop in ["qc_gauge_progress_percent","qc_gauge_red_percent","qc_gauge_green_percent"]:
            
            if prog_prop not in stored_object[qc_prop]:
                stored_object[qc_prop][prog_prop]=0
            
            
                

    if 'ac_machine_utilization_cutoff' not in stored_object:
        stored_object['ac_machine_utilization_cutoff']={}
        stored_object['xl_machine_utilization_cutoff']={}
        stored_object['ac_machine_utilization_cutoff']['historical_ts']={}
        stored_object['xl_machine_utilization_cutoff']['historical_ts']={}
        
        

    if 'machine_properties' not in stored_object:
        stored_object['machine_properties']={}
        stored_object['machine_properties']['ac_mac_util_cutoff_previous']=0.008
        stored_object['machine_properties']['xl_mac_util_cutoff_previous']=0.008
        stored_object['machine_properties']["vac_zero_last_set_time"]=0
        stored_object["machine_properties"]["last_bp_median"]=14.7
        stored_object["machine_properties"]["vac_zero_set_delta"]=0
        stored_object["machine_properties"]["vac_last_machine_idle_time"]=0



    if 'pump_utilization' not in stored_object:
        stored_object['pump_utilization']={}
        stored_object['pump_utilization']['historical_ts']={}
        stored_object['pump_utilization']['last_known_success']={'true':0,'false':0, 'color':['white']}


    return stored_object            

       
def Stored_Object_Saving(stored_object,stored_object_path):
    with open(stored_object_path,'w') as object_to_store:
        dump(stored_object,object_to_store)
