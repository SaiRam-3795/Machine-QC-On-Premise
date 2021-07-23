# -*- coding: utf-8 -*-
"""
This module contains function to compute kernel density estimates for a dataset
"""

from numpy import array, std, percentile, linspace
from scipy.stats import norm

def kde(data,x=None,bandwidth='silverman',num=512):
    ''' parameters:
    
            data:       The input list of elements/values with which the kernel density function is constructed
    
            x:          The list of values whose densities are estimated. If it is None, then this list is created automatically. It is set to None by default.
    
            bandwidth:  The bandwidth used for kernel density estimation. It can be one among 'silverman' or 'scott'. By default (bandwidth='silverman'), Silverman's Method is used to get the bandwidth. Scott's method can also be used.
    
            num:        The number of elements needed to create x, if x is None. By default, it is set to 512.


        Return: A dictionary of two items 'arguments' and 'densities', where 'arguments' is x, the list or numpy array of values at which kernel density estimates are obtained, and 'densities' is the list of kernel density estimates corresponding to values in x.
        If however, data does not have more than one unique value, or, an error occurs in calculating the kernel density estimates, both 'arguments' and 'densities' are empty array and list respectively. In case of any error, the error message is printed.
    
    '''

    
    n=len(data)
    if len(set(data))>1:

        try:
            data_sd=std(data,ddof=1)
            
            if bandwidth=='silverman':
                iqr=percentile(data,75)-percentile(data,25)
                y=iqr/1.34
                if y==0:
                    c=data_sd
                else:
                    c=min(y,data_sd)
                    
                h=0.9*c*len(data)**(-1/5)
            else:
                h=(4*data_sd**5/(3*n))**(1/5)
                
            if x is None:
                dens_arg=linspace(min(data)-3*h,max(data)+3*h,num)

            else:
                dens_arg=x
            
            def dens(x):
                return 1/(n*h)*norm.pdf((x-array(data))/h).sum()

            densities=list(map(dens,dens_arg))
                        
            return {'arguments':dens_arg,'densities':densities}

        except Exception as e:
            import sys
            exec_info=sys.exc_info()[2]
            line=exec_info.tb_lineno
            print("An exception has occured in kde at line {}: {}".format(line,str(e)))
            return {"arguments":array([]),"densities":[]}

    else:
        return {"arguments":array([]),"densities":[]}



