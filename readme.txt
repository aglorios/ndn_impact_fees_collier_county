Steps for running impact fee analysis.

Get datasets here from dropbox here: https://www.dropbox.com/s/auwkg30g3n3kezo/impact_fee_final_datasets_and_pdfs.zip?dl=0

1. Save the "Master" tab of zillow_prices_new_construction_single_detached_for_impact_fee_analysis.xlsx 
as zillow_prices_new_construction.csv

2. Run zillow_prices_script.txt in python

This will add ranges based on square foot for all the houses on the market in 2016.

3. Import the final_construction output into tableau or whatever visualization software you use. 

Make a chart using Range as the column and avg 2012 fee per square foot and avg 2016 fee per square foot as the rows.

An image of this chart is uploaded to github for your reference.

4. Run appraiser data script. For this script, you will need:

a. new_construction_2012.csv
b. new_construction_2013.csv
c. new_construction_2014.csv
d. new_construction_2015.csv
e. TAPEOUTCSF_residential2.csv

The new construction csvs are all the new constructions in Collier county since 2012
for single family homes.

The Tapeoutcsf is all the new construction period for Collier county since 2012.

We want to merge these files to make one large database since each has info the other does not.

In this script, we'll also geo-tag the data but you won't end up using it for this analysis.

If you want to skip that process, comment out that section of the code.

5. Export the first output and finish the analysis on Excel:

a. Make a pivot table with year built as the row labels and count of range and average sal1_amt as the column values
b. Take percentage that 2012 impact fees are of 2012 average sale amount for each range, what 2012 impact fees would have been for 2015 average sale amount for each range and what 2016 impact were for 2015 average sale amount for each range.
c. Calculate the percentage change between 2016 impact fee percentage for 2015 sale amount and 2012 impact percentage for 2012 sale amount.
