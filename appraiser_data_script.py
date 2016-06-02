# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Recipe inputs
# The first input, tapeoutcsf_residential2, is from the county appraisers office.
# Tapoutscf is their entire appraiser dataset. 
# I created tapeoutcsf_residential by filtering their use_code
# column on 0's and 1's only.
# To get more column fields in this doc, refer to:
# TAPEOUTCSF.pdf, which also came from the appraiser's office.

# Next, the new constuction csv's (12-15) also came from the appraiser's office.
# These are all the new construction for single family homes from 2012-2015.

# We need to join these with tapeoutcsf_residential2 on parcel_id in order
# to have access to all of the tapeout's fields, namely its address.

# Honestly, this stuff was more important when I thought I was mapping the data. 
# But it's still good to have a complete data set.

# I'll still also run through how to create a total address field for geo-tagging the data as well,
# in case anyone wants to run this. 
# One thing to remember with geo-tagging new-construction data is that 
# some of these homes are so new that they won't geo-tag properly.

dataset_tapeoutcsf_residential2 = dataiku.Dataset("TAPEOUTCSF_residential2")
new_con12 = dataiku.Dataset("new_construction_2012")
new_con13 = dataiku.Dataset("new_construction_2013")
new_con14 = dataiku.Dataset("new_construction_2014")
new_con15 = dataiku.Dataset("new_construction_2015")

# Read the datasets as Pandas dataframe
dataset_tapeoutcsf_residential2_df = dataset_tapeoutcsf_residential2.get_dataframe(limit=1000000)
pdu.audit(dataset_tapeoutcsf_residential2_df)

new_con12_df = new_con12.get_dataframe(limit=1000000)
new_con13_df = new_con13.get_dataframe(limit=1000000)
new_con14_df = new_con14.get_dataframe(limit=1000000)
new_con15_df = new_con15.get_dataframe(limit=1000000)

app_res_df = dataset_tapeoutcsf_residential2_df

# clean the data so that the column headers are are lower case:
app_res_df.columns = [x.lower() for x in app_res_df.columns]
new_con12_df.columns = [x.lower() for x in new_con12_df.columns]
new_con13_df.columns = [x.lower() for x in new_con13_df.columns]
new_con14_df.columns = [x.lower() for x in new_con14_df.columns]
new_con15_df.columns = [x.lower() for x in new_con15_df.columns]

# replace spaces in column headers with _
app_res_df = app_res_df.rename(columns=lambda x: x.replace(" ", "_"))

# strip extra space from the column header
app_res_df = app_res_df.rename(columns=lambda x: x.strip())

# do a left merge on parcel_id one so that it only keeps the row that join
# for each of the new_con dataframes. 
# Every row from the new_con dataframes should merge.
# Note: year_built column is the year the house was built.
# So, you will still have the year the house was built in the total dataset.
merge1 = pd.merge(app_res_df, new_con12_df, left_on='parcel_id', right_on='rm_parcel_id', how='inner')

merge2 = pd.merge(app_res_df, new_con13_df, left_on='parcel_id', right_on='rm_parcel_id', how='inner')

merge3 = pd.merge(app_res_df, new_con14_df, left_on='parcel_id', right_on='rm_parcel_id', how='inner')

merge4 = pd.merge(app_res_df, new_con15_df, left_on='parcel_id', right_on='rm_parcel_id', how='inner')

# make a list of the merge data frames
frames = [merge1, merge2, merge3, merge4]

# concatenate the list to create one large dataframe
merged_appraisers_df = pd.concat(frames)

# Okay, so, merged_appraisers_df will be one of the outputs. 
# This is all you need to move on if you don't want to geotag the dataframes for mapping.

####

# If you do want to geotag, keep going:

# Remove rows where there is no street address and put them into their own dataframe:

merged_appraisers_no_street_df = merged_appraisers_df[merged_appraisers_df['str_name'].isnull()]

# Make a new dataframe where the rows without a street address are taken out:

merged_appraisers_df1 = merged_appraisers_df[merged_appraisers_df['str_name'].notnull()]

# Remove rows where there is no street ordinance and put them into their own dataframe:

merged_appraisers_no_ord_df = merged_appraisers_df1[merged_appraisers_df1['str_ord'].isnull()]

# Make a new df without these rows:

merged_appraisers_df2 = merged_appraisers_df1[merged_appraisers_df1['str_ord'].notnull()]

# For the dataframe with ordinances:
#Keep making new dfs where you remove row that don't have:
# 1. Street number, 2. street name, 3. street type, 4. site_zip, 5. city

merged_appraisers_df3 = merged_appraisers_df2[merged_appraisers_df2['str_num'].notnull()]

merged_appraisers_df4 = merged_appraisers_df3[merged_appraisers_df3['str_name'].notnull()]

merged_appraisers_df5 = merged_appraisers_df4[merged_appraisers_df4['str_type'].notnull()]

merged_appraisers_df6 = merged_appraisers_df5[merged_appraisers_df5['sitezip'].notnull()]

merged_appraisers_df7 = merged_appraisers_df6[merged_appraisers_df6['city_cd'].notnull()]

# For the dataframe without ordinances:
# Keep making new dfs where you remove row that don't have:
# 1. Street number, 2. street name, 3. street type, 4. site_zip, 5. city

merged_appraisers_no_ord_df1 = merged_appraisers_no_ord_df[merged_appraisers_no_ord_df['str_num'].notnull()]

merged_appraisers_no_ord_df2 = merged_appraisers_no_ord_df1[merged_appraisers_no_ord_df1['str_name'].notnull()]

merged_appraisers_no_ord_df3 = merged_appraisers_no_ord_df2[merged_appraisers_no_ord_df2['str_type'].notnull()]

merged_appraisers_no_ord_df4 = merged_appraisers_no_ord_df3[merged_appraisers_no_ord_df3['sitezip'].notnull()]

merged_appraisers_no_ord_df5 = merged_appraisers_no_ord_df4[merged_appraisers_no_ord_df4['city_cd'].notnull()]

# Save the particularly troubling data that are both without street ordinances and zip codes in their on df:

merged_appraisers_no_zip_df4 = merged_appraisers_no_ord_df3[merged_appraisers_no_ord_df3['sitezip'].isnull()]

# Change street numbers, street names and street types for this last dataframe as strings:

merged_appraisers_no_zip_df4['str_num'] = merged_appraisers_no_zip_df4['str_num'].astype(int)
merged_appraisers_no_zip_df4['str_num'] = merged_appraisers_no_zip_df4['str_num'].astype(str)

merged_appraisers_no_zip_df4['str_name'] = merged_appraisers_no_zip_df4['str_name'].astype(str)

merged_appraisers_no_zip_df4['str_type'] = merged_appraisers_no_zip_df4['str_type'].astype(str)

# Save the even more troubling data that are without street ord, zip, and city as their own df:

merged_appraisers_no_zip_df5 = merged_appraisers_no_zip_df4[merged_appraisers_no_zip_df4['city_cd'].notnull()]

# Change the street numbers, street names and street types for the dataframe without ordinance, but with street number,
# street type, sitezip and city to strings:

merged_appraisers_no_ord_df4['str_num'] = merged_appraisers_no_ord_df4['str_num'].astype(int)
merged_appraisers_no_ord_df4['str_num'] = merged_appraisers_no_ord_df4['str_num'].astype(str)

merged_appraisers_no_ord_df4['str_name'] = merged_appraisers_no_ord_df4['str_name'].astype(str)

merged_appraisers_no_ord_df4['str_type'] = merged_appraisers_no_ord_df4['str_type'].astype(str)

merged_appraisers_no_ord_df4['sitezip'] = merged_appraisers_no_ord_df4['sitezip'].astype(int)
merged_appraisers_no_ord_df4['sitezip'] = merged_appraisers_no_ord_df4['sitezip'].astype(str)

# Change the street numbers, street names and street types for the dataframe with everything but city to strings:

merged_appraisers_df6['str_num'] = merged_appraisers_df6['str_num'].astype(int)
merged_appraisers_df6['str_num'] = merged_appraisers_df6['str_num'].astype(str)

merged_appraisers_df6['str_name'] = merged_appraisers_df6['str_name'].astype(str)

merged_appraisers_df6['str_type'] = merged_appraisers_df6['str_type'].astype(str)

merged_appraisers_df6['str_ord'] = merged_appraisers_df6['str_ord'].astype(str)

merged_appraisers_df6['sitezip'] = merged_appraisers_df6['sitezip'].astype(int)
merged_appraisers_df6['sitezip'] = merged_appraisers_df6['sitezip'].astype(str)

# Make a complete address column for the dataframe without ordinance, but with street number,
# street type, sitezip and city to strings:

merged_appraisers_no_ord_df4['address_full'] = merged_appraisers_no_ord_df4['str_num'] + " " + merged_appraisers_no_ord_df4['str_name'] + " " + merged_appraisers_no_ord_df4['str_type'] + "," + " " + merged_appraisers_no_ord_df4['sitezip']

# Make a complete address column for the dataframe with everything but city to strings:

merged_appraisers_df6['address_full'] = merged_appraisers_df6['str_num'] + " " + merged_appraisers_df6['str_name'] + " " + merged_appraisers_df6['str_type'] + " " + merged_appraisers_df6['str_ord'] + "," + " " + merged_appraisers_df6['sitezip']

# Make three complete address columns for the df without ordinance and without zip where you are making the city
# equal to Naples, Immokalee and Ave Maria in each respectively:

merged_appraisers_no_zip_df4['address_full_naples'] = merged_appraisers_no_zip_df4['str_num'] + " " + merged_appraisers_no_zip_df4['str_name'] + " " + merged_appraisers_no_zip_df4['str_type'] + "," + " " + "Naples," + " " + "Florida"

merged_appraisers_no_zip_df4['address_full_immokalee'] = merged_appraisers_no_zip_df4['str_num'] + " " + merged_appraisers_no_zip_df4['str_name'] + " " + merged_appraisers_no_zip_df4['str_type'] + "," + " " + "Immokalee," + " " + "Florida"

merged_appraisers_no_zip_df4['address_full_ave_maria'] = merged_appraisers_no_zip_df4['str_num'] + " " + merged_appraisers_no_zip_df4['str_name'] + " " + merged_appraisers_no_zip_df4['str_type'] + "," + " " + "Ave Maria," + " " + "Florida"

# Create a list for the dataframe with everything but city and the dataframe without ordinance, but with street number,
# street type, sitezip and city to strings:

frames1 = [merged_appraisers_df6, merged_appraisers_no_ord_df4]

# Concatenate these dataframes into a dataframe that has one address field

merged_app_w_address_df = pd.concat(frames1)

# Now try to geo-tag these:

api_key = "AIzaSyB2wO9ysLqnmty6EmZfj86otnRSS2UlK1k"

#loop:

import time
import requests
import json

for index, row in merged_app_w_address_df.iterrows():
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + row['address_full'] + 'FL&key=' + api_key
    #time.sleep(.22)
    request = requests.get(url)
    #data = json.loads(request)
    data = request.json()
    
    # I can't remember what this does
    result_key = data.keys()[1]
    
    # We are looping into that previous thing to isolate lat and long
    for result in data[result_key]:
        lat_value = result['geometry']['location']['lat']
        lng_value = result['geometry']['location']['lng']
        
        # Create new lat and long columns and save the lat and long values there
        merged_app_w_address_df.loc[index, 'lat'] = lat_value
        merged_app_w_address_df.loc[index, 'lng'] = lng_value
        
# Do the same loop for the dataframe without zips, starting with the immokalee address line:
for index, row in merged_appraisers_no_zip_df4.iterrows():
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + row['address_full_immokalee'] + 'FL&key=' + api_key
    #time.sleep(.22)
    request = requests.get(url)
    #data = json.loads(request)
    data = request.json()
    
    result_key = data.keys()[1]
    
    for result in data[result_key]:
        lat_value = result['geometry']['location']['lat']
        lng_value = result['geometry']['location']['lng']
        
    
        merged_appraisers_no_zip_df4.loc[index, 'lat'] = lat_value
        merged_appraisers_no_zip_df4.loc[index, 'lng'] = lng_value
        
# Save that data that didn't geo-code as a new dataframe:

merged_appraisers_no_zip_df5 = merged_appraisers_no_zip_df4[merged_appraisers_no_zip_df4['lat'].isnull()]

# Do the same loop for the rows that didn't geo-code last time, using with the ave maria address line:

for index, row in merged_appraisers_no_zip_df5.iterrows():
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + row['address_full_ave_maria'] + 'FL&key=' + api_key
    #time.sleep(.22)
    request = requests.get(url)
    #data = json.loads(request)
    data = request.json()
    
    result_key = data.keys()[1]
    
    for result in data[result_key]:
        lat_value = result['geometry']['location']['lat']
        lng_value = result['geometry']['location']['lng']
        
    
        merged_appraisers_no_zip_df5.loc[index, 'lat'] = lat_value
        merged_appraisers_no_zip_df5.loc[index, 'lng'] = lng_value
        
# Save that data that didn't geo-code as a new dataframe:

merged_appraisers_no_zip_df6 = merged_appraisers_no_zip_df5[merged_appraisers_no_zip_df5['lat'].isnull()]


# Do the same loop for the rows that didn't geocode, using with the naples address line:

import time
import requests
import json

for index, row in merged_appraisers_no_zip_df6.iterrows():
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + row['address_full_naples'] + 'FL&key=' + api_key
    #time.sleep(.22)
    request = requests.get(url)
    #data = json.loads(request)
    data = request.json()
    
    result_key = data.keys()[1]
    
    for result in data[result_key]:
        lat_value = result['geometry']['location']['lat']
        lng_value = result['geometry']['location']['lng']
        
    
        merged_appraisers_no_zip_df6.loc[index, 'lat'] = lat_value
        merged_appraisers_no_zip_df6.loc[index, 'lng'] = lng_value
        
        
# Save that data that didn't geo-code as a new dataframe:

merged_appraisers_no_zip_df7 = merged_appraisers_no_zip_df6[merged_appraisers_no_zip_df6['lat'].isnull()]

### Save data that didn't geo-code as output

# Save the data from each of these loops that did geo-code as separate dataframes:

merged_appraisers_no_zip_df4_export = merged_appraisers_no_zip_df4[merged_appraisers_no_zip_df4['lat'].notnull()]

merged_appraisers_no_zip_df5_export = merged_appraisers_no_zip_df5[merged_appraisers_no_zip_df5['lat'].notnull()]

merged_appraisers_no_zip_df6_export = merged_appraisers_no_zip_df6[merged_appraisers_no_zip_df6['lat'].notnull()]

# Put them into a new list:

frames2 = [merged_appraisers_no_zip_df4_export, merged_appraisers_no_zip_df5_export, merged_appraisers_no_zip_df6_export, merged_app_w_address_df]

# Concatenate them:

merged_app_w_address_df2 = pd.concat(frames2)
### Save as new output


# Recipe outputs

# Output for the merged data that isn't geo-tagged:
appraiser_new_construction_merged_2012_to_present = dataiku.Dataset("appraiser_new_construction_merged_2012_to_present")
appraiser_new_construction_merged_2012_to_present.write_with_schema(merged_appraisers_df)

# Output for first round of merged data that is geo-tagged:
appraiser_new_construction_merged_2012_to_present_geotagged1 = dataiku.Dataset("appraiser_new_construction_merged_2012_to_present_geotagged1")
appraiser_new_construction_merged_2012_to_present.write_with_schema(merged_app_w_address_df)

# Output for second round of merged data that is geo-tagged:
appraiser_new_construction_merged_2012_to_present_geotagged2 = dataiku.Dataset("appraiser_new_construction_merged_2012_to_present_geotagged2")
appraiser_new_construction_merged_2012_to_present.write_with_schema(merged_app_w_address_df2)

# Output for second round of merged data that can't geo-tag:
appraiser_new_construction_merged_2012_to_present_cant_geotag = dataiku.Dataset("appraiser_new_construction_merged_2012_to_present_cant_geotag")
appraiser_new_construction_merged_2012_to_present_cant_geotag.write_with_schema(merged_appraisers_no_zip_df7)
