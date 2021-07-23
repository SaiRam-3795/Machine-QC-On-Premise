# Outlier Indicators

#............. AC sensor outlier removal ...........

def Outlier_Indicator(stat_list):
  
    mr_skew_r=stat_list['mr_skewness_r']  
    mr_crest_factor_r=stat_list['mr_crest_factor_r']
    outlier_indicator=int(mr_skew_r>1.8 and mr_crest_factor_r>1.4)        #..... 1 means batch is outlier 
    return outlier_indicator
  



