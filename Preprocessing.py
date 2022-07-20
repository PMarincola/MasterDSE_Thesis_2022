import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime
import os

############################ REDUCE DATASET ####################################################
# Due to imbalance label (DoS attack reduced -60%)
df = pd.read_csv('OPCUA_dataset_public.csv')
df.drop(df.loc[8116:60000].index, inplace=True) 

############################ CREATE CASE ID ####################################################
# here we construct the case id by concatenating the desired attributes
# if you want, you don't need to convert the case id into numbers {0, 1, ..., n}
# this case concatenation is already enough to identify different cases
# and pm4py also works well with it
df['CaseID'] = df['src_ip'].astype(str) + '_' + df['src_port'].astype(str) + '_' + df['dst_ip'].astype(str) + '_' + df['dst_port'].astype(str)

# in any case, if you want to convert it to numbers, I found two simple ways
#case_id_counter = 1
#for case in tqdm(set(df['case_id'])):
    # here the conversion uses df.loc to select the case and sets the new value with a counter
    # df.loc[df['case_id'] == case, 'case_id'] = case_id_counter

    # here the conversion uses np.where and does the same thing, the only change is in the syntax
    #df['case_id'] = np.where(df['case_id'] == case, case_id_counter, df['case_id'])
    #case_id_counter += 1

#print(df)
df.to_csv('OPCUA_dataset_Preprocessing_1.csv', index=False)
print("CaseID entity created.")
# notes:
# the np.where method is a bit faster
# only use one of the methods at a time, otherwise you're changing twice the case column value
# probably there are other faster methods, but that would require a bit of research

################### CONVERT TIMESTAMP ###################################################
# Import a dataset
dataset = pd.read_csv("OPCUA_dataset_Preprocessing_1.csv")
#init variables for Cases and Events creation
numOfEvents = dataset.src_ip.count() #Total events in dataset
eventRow = 0 # used in for loop

flowStartCol = dataset.columns.get_loc("flowStart")
flowEndCol = dataset.columns.get_loc("flowEnd")

# index starting from 1 (Cases corresponds to every row/istances of dataset with the same src_ip + src_port + dst_ip + dst_port)
for eventRow in tqdm(range(numOfEvents)):
    #

    flowStart = dataset.iat[eventRow,flowStartCol] 
    flowStartConverted = pd.to_datetime(flowStart, unit='s')
    dataset.iat[eventRow,flowStartCol] = flowStartConverted

    flowEnd = dataset.iat[eventRow,flowEndCol] 
    flowEndConverted = pd.to_datetime(flowEnd, unit='s')
    dataset.iat[eventRow,flowEndCol] = flowEndConverted
    

#create condition for new dataset export in .csv format
datasetExportName = "OPCUA_dataset_Preprocessing_2" + ".csv"
dataset.to_csv (datasetExportName,index=False,header=True)
print("TIMESTAMP adjusted.")
######################### DATA CONCATENATION  #########################

# Import a dataset
dataset = pd.read_csv("OPCUA_dataset_Preprocessing_2.csv")
#init variables for Cases and Events creation

#dataset['flags'] = dataset['flags'].astype(str)
dataset['dst_host_same_src_port_rate'] = dataset['dst_host_same_src_port_rate'].astype(str)
dataset['b_pktTotalCount'] = dataset['b_pktTotalCount'].astype(str)

dataset['service_concat'] = dataset['service'] + "_" + dataset['dst_host_same_src_port_rate'] + "_" + dataset['b_pktTotalCount']

#create condition for new dataset export in .csv format
datasetExportName = "OPCUA_dataset_Preprocessing_3" + ".csv"
dataset.to_csv (datasetExportName,index=False,header=True)

print("OK data concatenation.")
######################### DROP + COLUMNS RENAME + ADJUST WRONG VALUES #########################
# Import a dataset
dataset = pd.read_csv("OPCUA_dataset_Preprocessing_3.csv")
#Drop
dataset.drop(columns=['src_ip', 'src_port','dst_ip', 'dst_port', 'proto', 'service','flowEnd','f_flowStart','b_flowStart'], axis=1, inplace=True)
#Col Rename
concact_dataset = dataset.rename(columns={"CaseID": "case:concept:name", "service_concat": "concept:name", "flowStart": "time:timestamp"})

#Adjust wrong values under -concept:name- col ( should be 0.0 or 100.0)
boolean_condition1 = concact_dataset['concept:name'] == 'Attribute_26.76518883415435_1'
boolean_condition2 = concact_dataset['concept:name'] == 'Attribute_14.154200230149597_1'
column_name = "concept:name"
new_value = 'Attribute_0.0_1' #values are closed to 0.0 than 100.0
concact_dataset.loc[boolean_condition1, column_name] = new_value
concact_dataset.loc[boolean_condition2, column_name] = new_value

#create condition for new dataset export in .csv format
datasetExportName = "OPCUA_dataset_Preprocessing_4" + ".csv"
concact_dataset.to_csv (datasetExportName,index=False,header=True)

print("OK DROP + COLUMNS RENAME + ADJUST WRONG VALUES")








