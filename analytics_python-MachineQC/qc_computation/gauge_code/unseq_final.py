# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 10:39:27 2019

@author: BISWADEEP
"""


def unseq(obj,recursive=True):
    if type(obj)==list:
        if all([type(l) not in [list,tuple,dict,set] for l in obj]):
            return obj
        else:
            list1=[]
            for value in obj:
                if type(value) not in [list,tuple,dict,set]:
                    list1.append(value)
                else:
                    list1.extend(unseq(value,recursive=True))
            return list1
    
    elif type(obj)==tuple:
        if all([type(l) not in [list,tuple,dict,set] for l in obj]):
            return list(obj)
        else:
            list1=[]
            for value in obj:
                if type(value) not in [list,tuple,dict,set]:
                    list1.append(value)
                else:
                    list1.extend(unseq(value,recursive=True))
            return list1
        
    elif type(obj)==dict:
        val_list=list(obj.values())
        if not recursive:
            return val_list
        else:
            if all([type(l) not in [list,tuple,dict,set] for l in val_list]):
                return val_list
            else:
                list1=[]
                for value in val_list:
                    if type(value) not in [list,tuple,dict,set]:
                        list1.append(value)
                    else:
                        list1.extend(unseq(value))
                
                return list1
            
