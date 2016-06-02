# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Recipe inputs
zillow_prices_new_construction = dataiku.Dataset("zillow_prices_new_construction")
new_con_df = zillow_prices_new_construction.get_dataframe()
pdu.audit(new_con_df)

# Change columns headers to lowercase and replace column headers with _
new_con_df.columns = [x.lower() for x in new_con_df.columns]
new_con_df = new_con_df.rename(columns=lambda x: x.replace("-", "_"))
new_con_df = new_con_df.rename(columns=lambda x: x.replace("/", "_"))
new_con_df = new_con_df.rename(columns=lambda x: x.strip())

#make new dataframes with ranges based on square footage:
new_con1 = new_con_df[new_con_df['square_foot'] <=1500]
new_con2 = new_con_df[(new_con_df['square_foot']>1500) & (new_con_df['square_foot']<=2499)]
new_con3 = new_con_df[(new_con_df['square_foot']>2500) & (new_con_df['square_foot']<=3999)]
new_con4 = new_con_df[(new_con_df['square_foot']>=4000)]

#set the range values in the new data frames between 1 and 4. 
#<=1500 = 1, 1500<x<=2500 = 2, 2500<x<=3999 = 3, 4000<=x = 4

new_con1['range'] = 1
new_con2['range'] = 2
new_con3['range'] = 3
new_con4['range'] = 4

#make a list of the dataframes so the you can concat them back together
frames = [new_con1, new_con2, new_con3, new_con4]

#concat them
new_con_range = pd.concat(frames)

#make new data frames based on neighborhood and range:

estates1 = new_con_range[(new_con_range['neighborhood'] == 'Estates') & (new_con_range['range'] == 1)]
estates2 = new_con_range[(new_con_range['neighborhood'] == 'Estates') & (new_con_range['range'] == 2)]
estates3 = new_con_range[(new_con_range['neighborhood'] == 'Estates') & (new_con_range['range'] == 3)]
estates4 = new_con_range[(new_con_range['neighborhood'] == 'Estates') & (new_con_range['range'] == 4)]

golden_gate1 = new_con_range[(new_con_range['neighborhood'] == 'Golden Gate') & (new_con_range['range'] == 1)]
golden_gate2 = new_con_range[(new_con_range['neighborhood'] == 'Golden Gate') & (new_con_range['range'] == 2)]
golden_gate3 = new_con_range[(new_con_range['neighborhood'] == 'Golden Gate') & (new_con_range['range'] == 3)]
golden_gate4 = new_con_range[(new_con_range['neighborhood'] == 'Golden Gate') & (new_con_range['range'] == 4)]

immokalee1 = new_con_range[(new_con_range['neighborhood'] == 'Immokalee') & (new_con_range['range'] == 1)]
immokalee2 = new_con_range[(new_con_range['neighborhood'] == 'Immokalee') & (new_con_range['range'] == 2)]
immokalee3 = new_con_range[(new_con_range['neighborhood'] == 'Immokalee') & (new_con_range['range'] == 3)]
immokalee4 = new_con_range[(new_con_range['neighborhood'] == 'Immokalee') & (new_con_range['range'] == 4)]

ave_maria1 = new_con_range[(new_con_range['neighborhood'] == 'Ave Maria') & (new_con_range['range'] == 1)]
ave_maria2 = new_con_range[(new_con_range['neighborhood'] == 'Ave Maria') & (new_con_range['range'] == 2)]
ave_maria3 = new_con_range[(new_con_range['neighborhood'] == 'Ave Maria') & (new_con_range['range'] == 3)]
ave_maria4 = new_con_range[(new_con_range['neighborhood'] == 'Ave Maria') & (new_con_range['range'] == 4)]

east_naples1 = new_con_range[(new_con_range['neighborhood'] == 'East Naples') & (new_con_range['range'] == 1)]
east_naples2 = new_con_range[(new_con_range['neighborhood'] == 'East Naples') & (new_con_range['range'] == 2)]
east_naples3 = new_con_range[(new_con_range['neighborhood'] == 'East Naples') & (new_con_range['range'] == 3)]
east_naples4 = new_con_range[(new_con_range['neighborhood'] == 'East Naples') & (new_con_range['range'] == 4)]

north_naples1 = new_con_range[(new_con_range['neighborhood'] == 'North Naples') & (new_con_range['range'] == 1)]
north_naples2 = new_con_range[(new_con_range['neighborhood'] == 'North Naples') & (new_con_range['range'] == 2)]
north_naples3 = new_con_range[(new_con_range['neighborhood'] == 'North Naples') & (new_con_range['range'] == 3)]
north_naples4 = new_con_range[(new_con_range['neighborhood'] == 'North Naples') & (new_con_range['range'] == 4)]

south1 = new_con_range[(new_con_range['neighborhood'] == 'South') & (new_con_range['range'] == 1)]
south2 = new_con_range[(new_con_range['neighborhood'] == 'South') & (new_con_range['range'] == 2)]
south3 = new_con_range[(new_con_range['neighborhood'] == 'South') & (new_con_range['range'] == 3)]
south4 = new_con_range[(new_con_range['neighborhood'] == 'South') & (new_con_range['range'] == 4)]

fiddler1 = new_con_range[(new_con_range['neighborhood'] == "Fiddler's Creek") & (new_con_range['range'] == 1)]
fiddler2 = new_con_range[(new_con_range['neighborhood'] == "Fiddler's Creek") & (new_con_range['range'] == 2)]
fiddler3 = new_con_range[(new_con_range['neighborhood'] == "Fiddler's Creek") & (new_con_range['range'] == 3)]
fiddler4 = new_con_range[(new_con_range['neighborhood'] == "Fiddler's Creek") & (new_con_range['range'] == 4)]

#calculate median_price_per_square_foot and median square foot for each range
#in each neighborhood:

estates1['avg_price_ft'] = estates1['price_sq_ft'].median()
estates1['avg_square_foot'] = estates1['square_foot'].median()

estates2['avg_price_ft'] = estates2['price_sq_ft'].median()
estates2['avg_square_foot'] = estates2['square_foot'].median()

estates3['avg_price_ft'] = estates3['price_sq_ft'].median()
estates3['avg_square_foot'] = estates3['square_foot'].median()

estates4['avg_price_ft'] = estates4['price_sq_ft'].median()
estates4['avg_square_foot'] = estates4['square_foot'].median()

golden_gate1['avg_price_ft'] = golden_gate1['price_sq_ft'].median()
golden_gate1['avg_square_foot'] = golden_gate1['square_foot'].median()

golden_gate2['avg_price_ft'] = golden_gate2['price_sq_ft'].median()
golden_gate2['avg_square_foot'] = golden_gate2['square_foot'].median()

golden_gate3['avg_price_ft'] = golden_gate3['price_sq_ft'].median()
golden_gate3['avg_square_foot'] = golden_gate3['square_foot'].median()

golden_gate4['avg_price_ft'] = golden_gate4['price_sq_ft'].median()
golden_gate4['avg_square_foot'] = golden_gate4['square_foot'].median()


immokalee1['avg_price_ft'] = immokalee1['price_sq_ft'].median()
immokalee1['avg_square_foot'] = immokalee1['square_foot'].median()

immokalee2['avg_price_ft'] = immokalee2['price_sq_ft'].median()
immokalee2['avg_square_foot'] = immokalee2['square_foot'].median()

immokalee3['avg_price_ft'] = immokalee3['price_sq_ft'].median()
immokalee3['avg_square_foot'] = immokalee3['square_foot'].median()

immokalee4['avg_price_ft'] = immokalee4['price_sq_ft'].median()
immokalee4['avg_square_foot'] = immokalee4['square_foot'].median()

ave_maria1['avg_price_ft'] = ave_maria1['price_sq_ft'].median()
ave_maria1['avg_square_foot'] = ave_maria1['square_foot'].median()

ave_maria2['avg_price_ft'] = ave_maria2['price_sq_ft'].median()
ave_maria2['avg_square_foot'] = ave_maria2['square_foot'].median()

ave_maria3['avg_price_ft'] = ave_maria3['price_sq_ft'].median()
ave_maria3['avg_square_foot'] = ave_maria3['square_foot'].median()

ave_maria4['avg_price_ft'] = ave_maria4['price_sq_ft'].median()
ave_maria4['avg_square_foot'] = ave_maria4['square_foot'].median()

east_naples1['avg_price_ft'] = east_naples1['price_sq_ft'].median()
east_naples1['avg_square_foot'] = east_naples1['square_foot'].median()

east_naples2['avg_price_ft'] = east_naples2['price_sq_ft'].median()
east_naples2['avg_square_foot'] = east_naples2['square_foot'].median()

east_naples3['avg_price_ft'] = east_naples3['price_sq_ft'].median()
east_naples3['avg_square_foot'] = east_naples3['square_foot'].median()

east_naples4['avg_price_ft'] = east_naples4['price_sq_ft'].median()
east_naples4['avg_square_foot'] = east_naples4['square_foot'].median()

north_naples1['avg_price_ft'] = north_naples1['price_sq_ft'].median()
north_naples1['avg_square_foot'] = north_naples1['square_foot'].median()

north_naples2['avg_price_ft'] = north_naples2['price_sq_ft'].median()
north_naples2['avg_square_foot'] = north_naples2['square_foot'].median()

north_naples3['avg_price_ft'] = north_naples3['price_sq_ft'].median()
north_naples3['avg_square_foot'] = north_naples3['square_foot'].median()

north_naples4['avg_price_ft'] = north_naples4['price_sq_ft'].median()
north_naples4['avg_square_foot'] = north_naples4['square_foot'].median()

south1['avg_price_ft'] = south1['price_sq_ft'].median()
south1['avg_square_foot'] = south1['square_foot'].median()

south2['avg_price_ft'] = south2['price_sq_ft'].median()
south2['avg_square_foot'] = south2['square_foot'].median()

south3['avg_price_ft'] = south3['price_sq_ft'].median()
south3['avg_square_foot'] = south3['square_foot'].median()

south4['avg_price_ft'] = south4['price_sq_ft'].median()
south4['avg_square_foot'] = south4['square_foot'].median()

fiddler1['avg_price_ft'] = fiddler1['price_sq_ft'].median()
fiddler1['avg_square_foot'] = fiddler1['square_foot'].median()

fiddler2['avg_price_ft'] = fiddler2['price_sq_ft'].median()
fiddler2['avg_square_foot'] = fiddler2['square_foot'].median()

fiddler3['avg_price_ft'] = fiddler3['price_sq_ft'].median()
fiddler3['avg_square_foot'] = fiddler3['square_foot'].median()

fiddler4['avg_price_ft'] = fiddler4['price_sq_ft'].median()
fiddler4['avg_square_foot'] = fiddler4['square_foot'].median()

# make a new list of the data frames so that you can re-concatenate them:
frames2 = [ave_maria1, ave_maria2, ave_maria3, ave_maria4, estates1, estates2, estates3, estates4, golden_gate1, golden_gate2, golden_gate3, golden_gate3, immokalee1, immokalee2, immokalee3,
           immokalee4, ave_maria1, ave_maria2, ave_maria3, ave_maria4, east_naples1, east_naples2, east_naples3, east_naples4,
           north_naples1, north_naples2, north_naples3, north_naples4, south1, south2, south3, south4, fiddler1, fiddler2, fiddler3, fiddler4]

#make the new concatenated dataframe:
new_con3 = pd.concat(frames2)

#now that you have you average price/sq ft and average sized home for each
#range in each neighborhood, calculate the average price based on these values:
new_con3['avg_price'] = new_con3['avg_price_ft'] * new_con3['avg_square_foot']

#save it as an int bc it's so long:
new_con3['avg_price'] = new_con3['avg_price'].astype('int')

#Now, calculate the impact fees for 2012 and 2016 for each sq ft range:
for index, row in new_con3.iterrows():
    if row['range'] == 1:
        new_con3.loc[index, '2012_impact_fee'] = 20246.95
        new_con3.loc[index, '2016_impact_fee'] = 23833.72
        
    if row['range'] == 2:
        new_con3.loc[index, '2012_impact_fee'] = 22801.20
        new_con3.loc[index, '2016_impact_fee'] = 23966.32
        
    if row['range'] == 3:
        new_con3.loc[index, '2012_impact_fee'] = 24562.64
        new_con3.loc[index, '2016_impact_fee'] = 24139.97    
        
    if row['range'] == 4:
        new_con3.loc[index, '2012_impact_fee'] = 24562.64
        new_con3.loc[index, '2016_impact_fee'] = 26110.87
        
#Add those impact fees to average price in order to get the average home price for each
#square foot range for each neighborhood given the different impact fees:

new_con3['2012_impact_plus_avg_price'] = new_con3['avg_price'] + new_con3['2012_impact_fee']
new_con3['2016_impact_plus_avg_price'] = new_con3['avg_price'] + new_con3['2016_impact_fee']

#For the hell of it, calculate the price difference from 2012 and 2016 given diff fees, and 
#then the percentage change. But you don't really use these in the charts.

new_con3['price_diff'] = (new_con3['2016_impact_plus_avg_price'] - new_con3['2012_impact_plus_avg_price'])
new_con3['percentage_change'] = (new_con3['price_diff']/new_con3['2012_impact_plus_avg_price']) * 100

#Calculate the 2012 and 2016 fee per sq ft which is what you'll really use:

new_con3['2012_fee_per_sq_ft'] = new_con3['2012_impact_fee']/new_con3['square_foot']
new_con3['2016_fee_per_sq_ft'] = new_con3['2016_impact_fee']/new_con3['square_foot']

#Calculate the 2012 and 2016 fee difference per sq ft, which you will also use:
new_con3['fee_per_sq_ft_diff'] = new_con3['2016_fee_per_sq_ft'] - new_con3['2012_fee_per_sq_ft']


# Recipe outputs
final_construction = dataiku.Dataset("final_construction")
final_construction.write_with_schema(new_con3)

