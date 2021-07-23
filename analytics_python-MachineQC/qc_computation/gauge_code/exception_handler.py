# -*- coding: utf-8 -*-


'''This module consists of functions needed to handle exceptions'''

#Importing some modules
from math import isfinite

def Numeric_Input_Type_Checking(input_list):
    '''Checking if all the input types are numeric'''
    if type(input_list)==dict:
        input_list=list(input_list.values())
        
    flag1=1-int(all([isinstance(element,(int,float,complex)) for element in input_list]))                   #all([condtion_satisfy(l) for l in list]) is True if all elements of 'list' satisfy the condition
    if flag1==1:                                                                                                    #that is, if the input list is not numeric    
        flag_indicator=1
    else:
        flag2=1-int(all([isfinite(element) for element in input_list]))
        flag_indicator=int(flag1+flag2!=0)                                                                          #flag_indicator=1 if flag1+flag2!=0, and 0, otherwise. 0 means all the input types are numeric, else, non-numeric
    return flag_indicator


def Character_Input_Type_Checking(input_list):
    '''Checking if all the elements of input_list are strings'''
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
        
    flag_indicator=1-int(all([isinstance(element,str) for element in input_list]))                           # 0 if all the elements of input_list are strings
    return flag_indicator


def Non_Negative_Out_Of_Range_Exception_Checking(input_list):
    '''Checking if all the numbers in input_list are non negative'''
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
    
                                                                                 
    if Numeric_Input_Type_Checking(input_list)==0:                                                      #that is, if the input list is numeric 
        flag1=1-int(all([element>=0 for element in input_list]))
    else:
        flag1=2
        
    return flag1


def Percentage_Out_Of_Range_Exception_Checking(input_list):
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
        
                                                                                     
    if Numeric_Input_Type_Checking(input_list)==0:                                                          #that is, if input list is numeric
        flag1=1-int(all([element>=0 and element<=100 for element in input_list]))
    else:
        flag1=2


    return flag1


def Fraction_Out_Of_Range_Exception_Checking(input_list):
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
    

    if Numeric_Input_Type_Checking(input_list)==0:                                                          #that is, if input list is numeric

        flag1=1-int(all([element>=0 and element<=1 for element in input_list]))
    else:
        flag1=2


    return flag1


def Blower_Numeric_Out_Of_Range_Exception_Checking(input_list):
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
    
    flag1=int(Numeric_Input_Type_Checking(input_list)!=0)                                                   # 0 if input_list is numeric, else 1


    return flag1


def Oil_Check_Numeric_Out_Of_Range_Exception_Checking(input_list):
    
    if type(input_list)==dict:
        input_list=list(input_list.values())
                                                                                  #that is, if the input list is non-empty
    flag1=int(Numeric_Input_Type_Checking(input_list)!=0)                                               # 0, if input_list is numeric, else 1


    return flag1


def Oil_Overfill_Numeric_Out_Of_Range_Exception_Checking(input_list):
    
    if Numeric_Input_Type_Checking(input_list)==0:                                                      #that is, if input lst is numeric
    
        flag1=int(input_list['batch_var_r']<0)
    else:
        flag1=2


    return flag1


def Filter_Status_Numeric_Out_Of_Range_Exception_Checking(input_list):
    '''Checking range for high_level and break_level'''
    flag1=int(input_list['batch_high_vac']<=2 or input_list['batch_high_vac']>13)                   #0 means not out of range, 1 means out of range
    flag2=int(input_list['batch_break_level']<0)
    flag=int(flag1+flag2!=0)                                                                                        #0 means not out of range, 1 means out of range
    return flag        
        

def Element_Exists_Check(full_index_path):
    try:
        len_element=len(full_index_path)
        exists_indicator=len_element>0                                                                              #exists_indicator is True or False according as len_element>0 or <=0
        return exists_indicator
    
    except Exception as e:
        print(e)


def Pressure_Data_Out_Of_Range_Exception_Checking(numeric_input):

    '''Detecting whether there is sufficient amount of pressure data to analyze'''

    flag1=int(len(numeric_input)<=100)                                                                              #int(True)=1 and int(False)=0
    flag2=int(len(set(numeric_input))<4)
    flag_indicator=int(flag1+flag2!=0)                                                                              #flag_indicator=0 if flag1+flag2==0, and 0 otherwise
    return flag_indicator


def VAC_Data_Out_Of_Range_Exception_Checking(numeric_input):

    '''Detecting whether there is sufficient amount of VAC data to analyze'''

    flag1=int(len(numeric_input)<=100)                                                                              #int(True)=1 and int(False)=0
    flag2=int(len(set(numeric_input))<4)
    flag_indicator=int(flag1+flag2!=0)                                                                              #flag_indicator=0 if flag1+flag2==0, and 0, otherwise
    return flag_indicator
