
''' This module consists of functions needed to log errors or issues in any code execution '''

import sys, os, datetime, json
global log_json



def read_log_info(log_file):
    
    try:
        if os.path.exists(log_file):
            with open(log_file) as log:
                log_json=json.load(log)
        else:
            log_json={}
    except:
        log_json={}

    return log_json



def add_log_info(log_json,current_datetime_stamp,func_name,log_type,log_text):

    try:
        
        if isinstance(log_json,dict):
            
            if str(current_datetime_stamp) not in log_json:
                log_json[str(current_datetime_stamp)]={}
                
            if func_name not in log_json[str(current_datetime_stamp)]:
                log_json[str(current_datetime_stamp)][func_name]={}

            if log_type in ["error","normal"]:
                
                if log_type not in log_json[str(current_datetime_stamp)][func_name]:
                    log_json[str(current_datetime_stamp)][func_name][log_type]=[]

                log_json[str(current_datetime_stamp)][func_name][log_type].append(log_text)

            else:
                print("log type should be 'normal' or 'error', not '{}'".format(log_type))

        else:
            print("Cannot update log_json as it is not a dictionary")

    except Exception as e:
        
        line=sys.exc_info()[2].tb_lineno
        
        print("Error occured while updating log at line {}: {}".format(line,str(e)))



def remove_old_log_info(log_json,current_datetime_stamp):

    if log_json!={}:

        key=sorted(list(log_json.keys()))[0]

        while current_datetime_stamp-int(key)>24*3600:

            del log_json[key]
            
            if log_json!={}:
            
                key=sorted(list(log_json.keys()))[0]

            else:

                break

        del key

    

def write_log_info(log_json,old_log_json,log_file):
    
    if log_json!=old_log_json:
        
        if log_json!={}:
            
            with open(log_file,"w") as log:
                
                json.dump(log_json,log)
        else:

            if os.path.exists(log_file):
                
                os.remove(log_file)
        
                
        
