#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 22:38:39 2020

@author: Bowen Yan

"""
import zipfile as zip
import pandas as pd
import csv
#import numpy as np
#import string


def read_zip_file(zf_name, fn):
    #read the zip file
    zf = zip.ZipFile(zf_name)
    #read the selected file in the zip
    df = pd.read_csv(zf.open(fn), delimiter="\t", quoting = csv.QUOTE_NONNUMERIC)
    print (df.head())
    return df

def save_as_tsv(df, fn):
    df.to_csv(fn, sep='\t', encoding='UTF-8', index=False)
    print (fn + " processed.")

#def get_utility_patent(df, col_id, idx):
#    df = df[~df[col_id].str.contains(idx)] 
#    return df

def get_utility_patent(df, col_id, idx_list):
    for idx in idx_list:
        df = df[~df[col_id].str.contains(idx)] 
    return df

def get_top5(df, sort_idx, groupby_idx):
    tmp_df = df.sort_values([sort_idx], 
                         ascending=False).groupby(groupby_idx).head(5)
    tmp_df = tmp_df.rename(columns={"patent_no": "patent_number"})
    return tmp_df

def calculate_cpc_object_patent_count(cpc_patn_df, patn_obj_df, obj_idx):
    cpc_obj_patn_df = cpc_patn_df.merge(patn_obj_df, on='pid')
    cpc_obj_patn_cnts_df = cpc_obj_patn_df.groupby(['class_id', 'class', obj_idx]).size().reset_index(name='cnts')
    return cpc_obj_patn_cnts_df

"""
Preprocess the downloaded tsv files

Only select the required data columns

"""
#exclude non-utility patents start with "D", "P", "H", "T"
idx_list = ["D", "P", "T", "H"]

#------table: assignee-------
#data column: id, organization
zf_name = "assignee.tsv.zip"
fn = "assignee.tsv"
asg_df = read_zip_file(zf_name,fn)
asg_df = asg_df[['id', 'organization']]  #only keep id and organization
#remove rows if organization is null, individuals stored in inventor table
asg_df = asg_df.fillna('')
asg_df = asg_df[asg_df['organization'] != '']  
asg_df['asg_id'] = asg_df.index
asg_df = asg_df.rename(columns={"id": "asg_str", "organization": "assignee"})
save_as_tsv(asg_df, fn)

#------patent_assignee------
zf_name = "patent_assignee.tsv.zip"
fn = "patent_assignee.tsv"
patn_asg_df = read_zip_file(zf_name,fn)
#only keep utility patents
patn_asg_df = get_utility_patent(patn_asg_df, 'patent_id', idx_list)
patn_asg_df = patn_asg_df.rename(columns={"patent_id": "patent_no", 
                                         "assignee_id": "asg_str"})
save_as_tsv(patn_asg_df, fn)

#------table: inventor------
zf_name = "inventor.tsv.zip"
fn = "inventor.tsv"
inv_df = read_zip_file(zf_name,fn)
inv_df['inv_id'] = inv_df.index
inv_df = inv_df.rename(columns={"id": "inv_str"})
save_as_tsv(inv_df, fn)

#------patent_inventor------ 
zf_name = "patent_inventor.tsv.zip"
fn = "patent_inventor.tsv"
patn_inv_df = read_zip_file(zf_name,fn)
patn_inv_df = get_utility_patent(patn_inv_df, 'patent_id', idx_list)
patn_inv_df = patn_inv_df.rename(columns={"patent_id": "patent_no", 
                                         "inventor_id": "inv_str"})
save_as_tsv(patn_inv_df, fn)

#------patent - utility------
zf_name = "patent.tsv.zip"
fn = "patent.tsv"
patn_df = read_zip_file(zf_name,fn)
#data columns: number, type, date, abstract, title
patn_df = patn_df[['number', 'type', 'date', 'abstract', 'title']]
patn_df = patn_df[patn_df['type'] == 'utility']  #only select utility patents
#remove the type column
patn_df = patn_df[['number', 'date', 'abstract', 'title']]
#the number column contains float type (e.g., 10000000) and string type (e.g., RE31700)
#convert to string type only
patn_df["number"] = patn_df["number"].apply(lambda x: str(int(x)) if type(x) == float else str(x))
patn_df = patn_df.rename(columns={"date": "grant_date",
                                 "number": "patent_no"})
patn_df['pid'] = patn_df.index  #initialize integer index

#create the patent_abstract file
patn_abst_df = patn_df[['pid', 'patent_no', 'abstract']]
patn_abst_df = patn_abst_df.rename(columns={"patent_no": "patent_number"})
save_as_tsv(patn_abst_df, "patent_abstract.tsv")

#create the patent_info file
patn_info_df = patn_df[['pid', 'patent_no', 'grant_date', 'title']]
save_as_tsv(patn_info_df, "patent_utility_temp.tsv")

#------application date------
zf_name = "application.tsv.zip"
fn = "application.tsv"
app_df = read_zip_file(zf_name,fn)
#only need application date and patent_id
app_df = app_df[['patent_id', 'date']]
app_df["patent_id"] = app_df["patent_id"].apply(lambda x: str(int(x)) if type(x) == float else str(x))
app_df = app_df.rename(columns={"date": "app_date",
                                 "patent_id": "patent_no"})
save_as_tsv(app_df, fn)

#------patent citation------
#this process will take a long time and require enough memory
zf_name = "uspatentcitation.tsv.zip"
fn = "uspatentcitation.tsv"
citation_df = read_zip_file(zf_name,fn)
citation_df = citation_df[['patent_id', 'citation_id']]
citation_df = get_utility_patent(citation_df, 'patent_id', idx_list) #can also apply on citation_id
#save_as_tsv(citation_df, "uspatentcitation_simple.tsv")
citation_cnt_df = citation_df.groupby(['citation_id']).size().reset_index(name='counts')
citation_cnt_df = citation_cnt_df.rename(columns={"citation_id": "patent_no"})
save_as_tsv(citation_cnt_df, "patn_citation.tsv")

#------table: patent utility------
#combine app_date, grant_date, patent information, citation, etc.
#collect app_date from application file
patn_utility_df = patn_info_df.merge(app_df, on='patent_no')
#collect forward citation from citation file
patn_utility_df = patn_utility_df.merge(citation_cnt_df, on='patent_no', how='left')
#fill nan to 0
patn_utility_df = patn_utility_df.fillna(0)
#covert float to integer
patn_utility_df["counts"] = patn_utility_df["counts"].apply(lambda x: int(x) if type(x) == float else x)
patn_utility_df = patn_utility_df.rename(columns={"counts": "patent_forward_citations"})
save_as_tsv(patn_utility_df, "patent_utility.tsv")


#-------table: patent_assignee------
patn_asg_df = patn_asg_df.merge(patn_info_df[['pid', 'patent_no']], on='patent_no', how='inner')
patn_asg_df = patn_asg_df.merge(asg_df[['asg_str', 'asg_id']], on='asg_str', how='inner')
patn_asg_df = patn_asg_df.rename(columns={"patent_no": "patent_number"})
save_as_tsv(patn_asg_df[['pid','patent_number','asg_id', 'asg_str']], "patent_assignee.tsv")

#-------table: patent_inventor------
patn_inv_df = patn_inv_df.merge(patn_info_df[['pid', 'patent_no']], on='patent_no', how='inner')
patn_inv_df = patn_inv_df.merge(inv_df[['inv_str', 'inv_id']], on='inv_str', how='inner')
patn_inv_df = patn_inv_df.rename(columns={"patent_no": "patent_number"})
save_as_tsv(patn_inv_df[['pid','patent_number','inv_id', 'inv_str']], "patent_inventor.tsv")

#------table: patent_extra_info------
zf_name = "rawlocation.tsv.zip"
fn = "rawlocation.tsv"
raw_loc_df = read_zip_file(zf_name,fn)
raw_loc_df = raw_loc_df.fillna('')
#id - rawlocation_id
raw_loc_df = raw_loc_df[['id', 'city', 'state', 'country','location_id']]
raw_loc_df = raw_loc_df.rename(columns={"id": "rawlocation_id"})

#check the sequence of inventors in rawinventor file
zf_name = "rawinventor.tsv.zip"
fn = "rawinventor.tsv"
raw_loc_inv_df = read_zip_file(zf_name,fn)
#only consider the first author's corresponding location
raw_loc_inv_df = raw_loc_inv_df[raw_loc_inv_df['sequence'] == 0.0]
raw_loc_inv_df = get_utility_patent(raw_loc_inv_df, 'patent_id', idx_list)
raw_loc_inv_df = raw_loc_inv_df[['patent_id', 'inventor_id', 'rawlocation_id']]

patn_first_inv_loc_df = raw_loc_inv_df.merge(raw_loc_df, on='rawlocation_id', how='inner')
patn_first_inv_loc_df = patn_first_inv_loc_df[['patent_id', 'city', 'state', 'country']]
patn_first_inv_loc_df = patn_first_inv_loc_df.rename(columns={"patent_id": "patent_no",
                                                             "city":"patent_first_inv_city",
                                                             "state":"patent_first_inv_state",
                                                             "country":"patent_first_inv_country"})
patn_first_inv_loc_df = patn_first_inv_loc_df.merge(patn_utility_df[['patent_no', 'pid']], on="patent_no", how="inner")
save_as_tsv(patn_first_inv_loc_df, "patent_extra_info.tsv")

#-------table: inventor lastknown location------
zf_name = "location.tsv.zip"
fn = "location.tsv"
loc_df = read_zip_file(zf_name,fn)

#get the latest patent for inventor, and decide his/her location
inv_info_df = patn_inv_df[['pid', 'inv_id', 'inv_str', 'location_id','patent_number']].merge(patn_utility_df[['pid', 'grant_date']], on='pid', how='inner')
inv_latest_patn_df = inv_info_df.sort_values(['grant_date'], ascending=False).groupby('inv_str').head(1)
inv_lastknown_loc_df = inv_latest_patn_df[['inv_id', 'inv_str', 'location_id']].merge(loc_df[['id', 'city', 'state', 'country']], 
                                                               left_on='location_id', right_on='id', how='left')
inv_lastknown_loc_df = inv_lastknown_loc_df[['inv_id', 'inv_str', 'city', 'state', 'country']].merge(inv_df, on='inv_id')
inv_lastknown_loc_df = inv_lastknown_loc_df[['inv_id', 'inv_str_x', 'name_first', 'name_last', 'city', 'state', 'country']]
inv_lastknown_loc_df = inv_lastknown_loc_df.rename(columns={"inv_str_x": "inv_str"})
save_as_tsv(inv_lastknown_loc_df, "inventor_lastknown_location.tsv")

#-------table: patent_cpc3, patent_cpc4------
zf_name = "cpc_current.tsv.zip"
fn = "cpc_current.tsv"
cpc_df = read_zip_file(zf_name,fn)
cpc_df["patent_id"] = cpc_df["patent_id"].apply(lambda x: str(int(x)) if type(x) == float else str(x))
cpc_df = cpc_df[['patent_id', 'subsection_id', 'group_id']] #collect cpc3 and cpc4 only
cpc_df = cpc_df.drop_duplicates(subset=['patent_id','subsection_id','group_id'], keep='last') #drop duplicates
merged_df = cpc_df.merge(patn_utility_df[['pid', 'patent_no']], left_on="patent_id", right_on="patent_no")
save_as_tsv(merged_df[['patent_no', 'subsection_id', 'group_id', 'pid']], "patent_cpc.tsv")
cpc3_df = merged_df[['pid', 'patent_id', 'subsection_id']]
cpc4_df = merged_df[['pid', 'patent_id', 'group_id']]
cpc3_df = cpc3_df.drop_duplicates(subset=['pid','patent_id','subsection_id'], keep='last') #drop duplicates
cpc4_df = cpc4_df.drop_duplicates(subset=['pid', 'patent_id', 'group_id'], keep='last') #drop duplicates
#unify the class column name
cpc3_df = cpc3_df.rename(columns={"subsection_id": "class",
                                 "patent_id": "patent_number"})
cpc4_df = cpc4_df.rename(columns={"group_id": "class",
                                 "patent_id": "patent_number"})
#read id in tsv file 
cpc3_id_df = pd.DataFrame(pd.read_csv("cpc3_id.tsv", sep='\t'))
cpc4_id_df = pd.DataFrame(pd.read_csv("cpc4_id.tsv", sep='\t'))

patn_cpc3_df = cpc3_df.merge(cpc3_id_df, on='class')
patn_cpc3_df = patn_cpc3_df.rename(columns={"id": "class_id"})
save_as_tsv(patn_cpc3_df, "patent_cpc3.tsv")

patn_cpc4_df = cpc4_df.merge(cpc4_id_df, on='class')
patn_cpc4_df = patn_cpc4_df.rename(columns={"id": "class_id"})
save_as_tsv(patn_cpc4_df, "patent_cpc4.tsv")

#------table: cpc3_patent_info, cpc4_patent_info------
#create cpc-related tables
cpc3_patn_df = patn_utility_df.merge(patn_cpc3_df, on='pid')[['pid','app_date','grant_date','patent_forward_citations','class_id', 'class', 'title','patent_no']]
cpc4_patn_df = patn_utility_df.merge(patn_cpc4_df, on='pid')[['pid','app_date','grant_date','patent_forward_citations','class_id', 'class', 'title','patent_no']]
save_as_tsv(cpc3_patn_df[['pid','app_date','grant_date','patent_forward_citations','class_id','title']], "cpc3_patent_info.tsv")
save_as_tsv(cpc4_patn_df[['pid','app_date','grant_date','patent_forward_citations','class_id','title']], "cpc4_patent_info.tsv")

#prepare top-5 data
#grant_date
cpc3_patn_grant_date_top5 = get_top5(cpc3_patn_df, 'grant_date', 'class_id')
cpc4_patn_grant_date_top5 = get_top5(cpc4_patn_df, 'grant_date', 'class_id')

#citation
cpc3_patn_citation_top5 = get_top5(cpc3_patn_df, 'patent_forward_citations', 'class_id')
cpc4_patn_citation_top5 = get_top5(cpc4_patn_df, 'patent_forward_citations', 'class_id')

save_as_tsv(cpc3_patn_grant_date_top5, "cpc3_patent_info_grant_date_top5.tsv")
save_as_tsv(cpc3_patn_citation_top5, "cpc3_patent_info_citation_top5.tsv")
save_as_tsv(cpc4_patn_grant_date_top5, "cpc4_patent_info_grant_date_top5.tsv")
save_as_tsv(cpc4_patn_citation_top5, "cpc4_patent_info_citation_top5.tsv")

#cnts
#assignee
cpc3_asg_patn_cnts_df = calculate_cpc_object_patent_count(cpc3_patn_df, patn_asg_df, 'asg_id')
cpc3_asg_patn_cnts_top5_df = get_top5(cpc3_asg_patn_cnts_df, 'cnts', 'class_id')
cpc3_asg_cnts = cpc3_asg_patn_cnts_df.merge(asg_df, on='asg_id')[['class_id', 'class', 'asg_id', 'assignee', 'cnts']]
cpc3_asg_cnts_top5 = cpc3_asg_patn_cnts_top5_df.merge(asg_df, on='asg_id')[['class_id', 'class', 'asg_id', 'assignee', 'cnts']]
save_as_tsv(cpc3_asg_cnts, "cpc3_assignee_cnts.tsv")
save_as_tsv(cpc3_asg_cnts_top5, "cpc3_assignee_cnts_top5.tsv")

cpc4_asg_patn_cnts_df = calculate_cpc_object_patent_count(cpc4_patn_df, patn_asg_df, 'asg_id')
cpc4_asg_patn_cnts_top5_df = get_top5(cpc4_asg_patn_cnts_df, 'cnts', 'class_id')
cpc4_asg_cnts = cpc4_asg_patn_cnts_df.merge(asg_df, on='asg_id')[['class_id', 'class', 'asg_id', 'assignee', 'cnts']]
cpc4_asg_cnts_top5 = cpc4_asg_patn_cnts_top5_df.merge(asg_df, on='asg_id')[['class_id', 'class', 'asg_id', 'assignee', 'cnts']]
save_as_tsv(cpc4_asg_cnts, "cpc4_assignee_cnts.tsv")
save_as_tsv(cpc4_asg_cnts_top5, "cpc4_assignee_cnts_top5.tsv")

#inventor
cpc3_inv_patn_cnts_df = calculate_cpc_object_patent_count(cpc3_patn_df, patn_inv_df, 'inv_id')
cpc3_inv_cnts = cpc3_inv_patn_cnts_df.merge(inv_df, on='inv_id')[['class_id', 'class', 'inv_id', 'name_first', 'name_last', 'cnts']]
cpc3_inv_patn_cnts_top5_df = get_top5(cpc3_inv_patn_cnts_df, 'cnts', 'class_id')
cpc3_inv_cnts_top5 = cpc3_inv_patn_cnts_top5_df.merge(inv_df, on='inv_id')[['class_id', 'class', 'inv_id', 'name_first', 'name_last', 'cnts']]

save_as_tsv(cpc3_inv_cnts, "cpc3_inventor_cnts.tsv")
save_as_tsv(cpc3_inv_cnts_top5, "cpc3_inventor_cnts_top5.tsv")

cpc4_inv_patn_cnts_df = calculate_cpc_object_patent_count(cpc4_patn_df, patn_inv_df, 'inv_id')
cpc4_inv_cnts = cpc4_inv_patn_cnts_df.merge(inv_df, on='inv_id')[['class_id', 'class', 'inv_id', 'name_first', 'name_last', 'cnts']]
cpc4_inv_patn_cnts_top5_df = get_top5(cpc4_inv_patn_cnts_df, 'cnts', 'class_id')
cpc4_inv_cnts_top5 = cpc4_inv_patn_cnts_top5_df.merge(inv_df, on='inv_id')[['class_id', 'class', 'inv_id', 'name_first', 'name_last', 'cnts']]

save_as_tsv(cpc4_inv_cnts, "cpc4_inventor_cnts.tsv")
save_as_tsv(cpc4_inv_cnts_top5, "cpc4_inventor_cnts_top5.tsv")

#country
cpc3_cnty_patn_cnts_df = calculate_cpc_object_patent_count(cpc3_patn_df, patn_first_inv_loc_df, 'patent_first_inv_country')
cpc3_cnty_patn_cnts_top5_df = get_top5(cpc3_cnty_patn_cnts_df, 'cnts', 'class_id')
save_as_tsv(cpc3_cnty_patn_cnts_df, "cpc3_country_cnts.tsv")
save_as_tsv(cpc3_cnty_patn_cnts_top5_df, "cpc3_country_cnts_top5.tsv")

cpc4_cnty_patn_cnts_df = calculate_cpc_object_patent_count(cpc4_patn_df, patn_first_inv_loc_df, 'patent_first_inv_country')
cpc4_cnty_patn_cnts_top5_df = get_top5(cpc4_cnty_patn_cnts_df, 'cnts', 'class_id')
save_as_tsv(cpc4_cnty_patn_cnts_df, "cpc4_country_cnts.tsv")
save_as_tsv(cpc4_cnty_patn_cnts_top5_df, "cpc4_country_cnts_top5.tsv")






