### Process Mining preprocessing

#Libraries

import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime
import os

#Process mining libraries
import pm4py
import numpy as np
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.filtering.log.timestamp import timestamp_filter

from pm4py.visualization.petrinet import visualizer as pn_visualizer
from pm4py.objects.petri.petrinet import PetriNet, Marking

from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.algo.conformance.alignments import algorithm as alignments

###########################
# Create event log object
###########################
log_csv = pd.read_csv(("OPCUA_dataset_Preprocessing_4.csv"), sep = ',')
log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)
log_csv = log_csv.sort_values('time:timestamp')
event_log = log_converter.apply(log_csv)

print("Eventlog object creation completed!")

################################################################################
# Create a Petri net representing the model of Normal behaviour in OPCUA standard
#################################################################################
#MOdello OK

net = PetriNet("new_petri_net")
# creating 
start_StartRawConection = PetriNet.Place("Start_StartRawConection_0.0_1")
startRawConection_SecureChannel = PetriNet.Place("StartRawConection_SecureChannel_0.0_1")
secureChannel_Session = PetriNet.Place("SecureChannel_Session_0.0_1")
#session_Session = PetriNet.Place("Session_Session_0.0_1")
session_Attribute = PetriNet.Place("Session_Attribute_0.0_1")
attribute_Attribute = PetriNet.Place("attribute_Attribute_100.0_1")
attribute_SecureChannel = PetriNet.Place("attribute_SecureChannel_100.0_1")
attribute_end = PetriNet.Place("attribute_end")

hidden_attribute100 = PetriNet.Place("hidden_attribute100")
hidden_securechannel100 = PetriNet.Place("hidden_securechannel100")

# add the places to the Petri Net
net.places.add(start_StartRawConection)
net.places.add(startRawConection_SecureChannel)
net.places.add(secureChannel_Session)
#net.places.add(session_Session)
net.places.add(session_Attribute)
net.places.add(attribute_Attribute)
net.places.add(attribute_SecureChannel)
net.places.add(hidden_attribute100)
net.places.add(hidden_securechannel100)
net.places.add(attribute_end)

# Create transitions
t_1 = PetriNet.Transition("p1", "StartRawConnection_0.0_1")
t_2 = PetriNet.Transition("p2", "SecureChannel_0.0_1")
t_3 = PetriNet.Transition("p3", "Session_0.0_1")
t_4 = PetriNet.Transition("p4", "Attribute_0.0_1")
t_5 = PetriNet.Transition("p5", "Attribute_100.0_1")
t_6 = PetriNet.Transition("p6", "SecureChannel_100.0_1")

t_7 = PetriNet.Transition("p7", None)
t_8 = PetriNet.Transition("p8", None)
t_9 = PetriNet.Transition("p9", None)
t_10 = PetriNet.Transition("p10", None)
t_11 = PetriNet.Transition("p11", None)

# Add the transitions to the Petri Net
net.transitions.add(t_1)
net.transitions.add(t_2)
net.transitions.add(t_3)
net.transitions.add(t_4)
net.transitions.add(t_5)
net.transitions.add(t_6)
net.transitions.add(t_7)
net.transitions.add(t_8)
net.transitions.add(t_9)
net.transitions.add(t_10)
net.transitions.add(t_11)

# Add arcs
from pm4py.objects.petri import utils
utils.add_arc_from_to(start_StartRawConection, t_1, net)
utils.add_arc_from_to(t_1, startRawConection_SecureChannel, net)
utils.add_arc_from_to(startRawConection_SecureChannel, t_2, net)
utils.add_arc_from_to(t_2, secureChannel_Session, net)
utils.add_arc_from_to(secureChannel_Session, t_3, net)
utils.add_arc_from_to(t_3, session_Attribute, net)

utils.add_arc_from_to(session_Attribute,t_7, net)
utils.add_arc_from_to(t_7,secureChannel_Session, net)

utils.add_arc_from_to(session_Attribute, t_4, net)
utils.add_arc_from_to(t_4, attribute_Attribute, net)

utils.add_arc_from_to(attribute_Attribute,t_8, net)
utils.add_arc_from_to(t_8,hidden_attribute100, net)
utils.add_arc_from_to(hidden_attribute100,t_5, net)
utils.add_arc_from_to(t_5,attribute_SecureChannel, net)

utils.add_arc_from_to(attribute_SecureChannel,t_9, net)
utils.add_arc_from_to(t_9,hidden_attribute100, net)

utils.add_arc_from_to(attribute_SecureChannel,t_10, net)
utils.add_arc_from_to(t_10,hidden_securechannel100, net)

utils.add_arc_from_to(hidden_securechannel100,t_6, net)
utils.add_arc_from_to(t_6,attribute_Attribute, net)
utils.add_arc_from_to(hidden_securechannel100,t_11, net)

utils.add_arc_from_to(t_11,attribute_end, net)

# Adding tokens
initial_marking = Marking()
initial_marking[start_StartRawConection] = 1
final_marking = Marking()
final_marking[attribute_end] = 1

# The model
knownet, initial_marking, final_marking = net, initial_marking, final_marking
print("Syntethic model for OPCUA standard completed!")

################################################################################
# Process Mining Algorithms
#################################################################################
#### PM Algorithms

#Token Reply
replayed_traces = token_replay.apply(event_log, knownet, initial_marking, final_marking)
print("TokenReply Algorithm completed!")
#Alignment
aligned_traces = alignments.apply_log(event_log, knownet, initial_marking, final_marking)
print("Alignment Algorithm completed!")

################################################################################
# Create new dataset with Process Mining statistics
#################################################################################

# token reply outputs with know Inductive Miner 
trace_is_fit = 0
trace_fitness = 0
missing_tokens = 0
consumed_tokens = 0
remaining_tokens = 0
produced_tokens = 0

#alignment outputs with know Inductive Miner 
cost = 0
visited_states = 0
queued_states = 0
traversed_arcs = 0
lp_solved = 0
fitness_AL = 0
output = []
for rp, al, trace in zip(replayed_traces, aligned_traces, event_log):
    #caseid
    case_id = trace.attributes.get('concept:name')
    #Extract IP Class
    IPclass = int(case_id.split('.')[0])
    
    if IPclass <= 127 :
        IPlabel = 'A'
    elif (IPclass >127) & (IPclass <= 191) :
        IPlabel = 'B'
    elif (IPclass >= 192) :
        IPlabel = 'C'
    
    #trace_is_fit - change to int value
    if rp['trace_is_fit'] == True :
        trace_is_fit = 1
    else:
        trace_is_fit = 0
    #trace_fitness
    trace_fitness = float(rp['trace_fitness'])
    #missing_tokens
    missing_tokens = rp['missing_tokens']
    #consumed_tokens 
    consumed_tokens = rp['consumed_tokens']
    #remaining_tokens
    remaining_tokens = rp['remaining_tokens']
    #produced_tokens
    produced_tokens = rp['produced_tokens']
    #AL cost
    cost = al['cost']
    #visited_states
    visited_states = al['visited_states']
    #queued_states
    queued_states = al['queued_states']
    # traversed_arcs
    traversed_arcs = al['traversed_arcs']
    #lp_solved
    lp_solved = al['lp_solved']
    #fitness_AL
    fitness_AL = float(al['fitness'])
    #Create int labels from string
    # Next time better to use .map 
    label = '1'
    # general statistics output
    tot_flags = 0
    tot_msg_size = 0
    tot_flowDuration = 0
    
    for event in trace:
        multi_label_value = event['multi_label']
        if multi_label_value == 'Impersonation' :
            label = '2'
        elif multi_label_value == 'DoS' :
            label = '3'
        elif multi_label_value == 'MITM' :
            label = '4'
        #Add others statistics values - flags - msg_size - flowDuration   
        flags = event['flags']
        tot_flags = tot_flags + flags
        msg_size = event['msg_size']
        tot_msg_size = tot_msg_size + msg_size 
        flowDuration = event['flowDuration']
        tot_flowDuration = tot_flowDuration + flowDuration

    len_flag = len(trace)
    len_msg = len(trace)
    
    flags_mean = tot_flags/len_flag
    msg_size_mean = tot_msg_size/len_msg
    flowDuration = tot_flowDuration
        
       
    output.append([case_id, IPlabel,flags_mean,msg_size_mean,flowDuration, trace_is_fit, trace_fitness,missing_tokens,consumed_tokens,
                   remaining_tokens,produced_tokens,cost,visited_states,queued_states,
                   traversed_arcs,lp_solved,fitness_AL,label])
	
df = pd.DataFrame(output, columns=['case_id','IPclass','stat_flags_mean','stat_msg_size_mean','stat_flowDuration_tot','trace_is_fit_TR', 'trace_fitness_TR','missing_tokens_TR',
                                   'consumed_tokens_TR','remaining_tokens_TR','produced_tokens_TR','cost_AL',
                                   'visited_states_AL','queued_states_AL','traversed_arcs_AL','lp_solved_AL',
                                   'fitness_AL','label'])

# Export to csv
df.to_csv(os.path.join("ML",'OPCUA_dataset_ML.csv'))
print("Process Mining preprocessing completed!")