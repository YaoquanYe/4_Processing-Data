import pandas as pd
import numpy as np

def getdata(files, ColumnList):
     
    #Read data from CSV files
    df = pd.read_csv(files, usecols=ColumnList, na_values=['NAN'])

    return df

def run():
    
    #Column name in the real dataset
    ZipCode_CSI = 'Host Customer Physical Zip Code' 
    NameplateRating_CSI = 'Nameplate Rating'
    ColumnList_CSI = [ZipCode_CSI, NameplateRating_CSI]
    
    ZipCode_CB = 'ZipCode'
    AverageHouseValue_CB = 'AverageHouseValue'
    IncomePerHousehold_CB = 'IncomePerHousehold'
    ColumnList_CB = [ZipCode_CB, AverageHouseValue_CB, IncomePerHousehold_CB]
    
    InstalledStatus_CSI = 'Installed Status'
    ColumnList_CSI_IS = [ZipCode_CSI, InstalledStatus_CSI]
    
    ####################################################################################
    #Table 1
    #Import the data from CSI Data.csv
    df_CSI = getdata('CSI Data.csv', ColumnList_CSI)
    
    #Change the column name
    df_CSI.rename(columns={'Host Customer Physical Zip Code': 'ZipCode'}, inplace=True)
    
    #Used groupby function to calculate the average nameplate rating
    df_CSI = df_CSI.groupby('ZipCode').mean()
    
    #Change the column name
    df_CSI.rename(columns={'Nameplate Rating': 'Average Nameplate Rating'}, inplace=True)

    ####################################################################################
    #Table 2
    #Import Average House Value and Income Per Household from
    #zip-codes-database-DELUXE-BUSINESS.csv and change column name
    df_CB = getdata('zip-codes-database-DELUXE-BUSINESS.csv', ColumnList_CB)
    df_CB.rename(columns={AverageHouseValue_CB: 'Average House Value',
                          IncomePerHousehold_CB: 'Income Per Household'}, inplace=True)
    
    #Drop the duplicates record of this two column in zip-codes-database-DELUXE-BUSINESS.csv
    df_CB = df_CB.drop_duplicates(subset=ZipCode_CB, keep='first')
    df_CB.set_index(ZipCode_CB, inplace=True)
    
    ####################################################################################
    #Table 3
    #Import Installed Status from CSI Data.csv and change column name
    df_CSI_IS = getdata('CSI Data.csv', ColumnList_CSI_IS)
    
    #Only choosed the 'Installed' data and drop the NAN value showed in the Zipcode column
    df_CSI_IS = df_CSI_IS.loc[df_CSI_IS['Installed Status'] == 'Installed']
    df_CSI_IS.rename(columns={ZipCode_CSI: 'ZipCode'}, inplace=True)
    df_CSI_IS = df_CSI_IS.dropna()
    
    #Used groupby function to calculate the number of completed solar installations
    df_CSI_IS = df_CSI_IS.groupby('ZipCode').size()
    df_CSI_IS = df_CSI_IS.reset_index(name='# of completed solar installations')
    df_CSI_IS.set_index('ZipCode', inplace=True)
    
    ####################################################################################
    #Join table 1, 2 and 3 
    #Used Inner Join to join these three table.
    #P.S. No NAN value in any columns
    #Output the CSV file named Result(No NAN value)
    df_All_noNAN = df_CSI.join(df_CB, how = 'inner')
    df_All_noNAN = df_All_noNAN.join(df_CSI_IS, how = 'inner')    
    df_All_noNAN.to_csv('C:\Users\ericy\Desktop\Result(No NAN value).csv')
    
    #Used Left Join to join these three table.
    #P.S. I keep NAN value in columns
    #Output the CSV file named Result(Has NAN value)
    df_All_hasNAN = df_CSI.join(df_CB, how = 'left')
    df_All_hasNAN = df_All_hasNAN.join(df_CSI_IS, how = 'left')
    df_All_hasNAN.to_csv('C:\Users\ericy\Desktop\Result(Has NAN value).csv')
    

if __name__ == "__main__":
    run()