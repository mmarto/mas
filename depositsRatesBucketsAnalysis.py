# ## Deposits Rates Buckets Test

# ### 1. Import the libraries needed
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import numpy as np
from bs4 import BeautifulSoup
import requests
# pd.__version__


def main():
    # 2. Define the input files and urls
    depositAmountsFile = 'files/Deposit_amounts.xlsx'
    depositsRatesBuckets = 'files/Deposits_rates_and_buckets.xlsx'
    url = '''http://www.cbr.ru/eng/statistics/print.aspx?file=credit_statistics/ex_rate_ind_16_e.htm&pid=svs&sid=analit'''

    # 3. Read file Deposit_amounts.xlsx and mangle and align the data for further calculations,
    # skip first 7 rows, don't put headers
    dfDepositAmounts = pd.read_excel(depositAmountsFile, sheetname=0, skiprows=7, header=None)

    # Explore the data
    # dfDepositAmounts.info()
    # dfDepositAmounts.head()

    # #### Take only the first and the last 4 columns from the dataset
    dfDepositAmounts = dfDepositAmounts[[0, 7, 8, 9, 10]]
    # dfDepositAmounts.columns

    # Rename the columns in the dataset
    dfDepositAmounts.columns = ['dt', 'entities_rub', 'entities_usd', 'individual_rub', 'individual_usd']
    # dfDepositAmounts.head()

    # Filter out rows which have date older then 1 Jan 2016
    # dfDepositAmounts[dfDepositAmounts.dt >= '2016-01-01']
    dfDepositAmounts = dfDepositAmounts[dfDepositAmounts.dt >= '2016-01-01']

    # Create a column with full month name from the dt column
    dfDepositAmounts['month'] = dfDepositAmounts.dt.apply('{:%B}'.format)
    # dfDepositAmounts.sample(2)

    # Set column month to be an index for the dataset
    # (To be used to join with datasets from the second file and from the url)
    dfDepositAmounts = dfDepositAmounts.set_index('month')
    # del(dfDepositAmounts['dt'])
    # dfDepositAmounts.sample(2)

    # 4. Read file Deposits_rates_and_buckets.xlsx and mangle and align the data for further calculations
    # Since the file contains multiple spreadsheets read the first 2 sheets
    # and populate them into a dictionary of dataframes. Skip the first 6 rows and don't use headers
    dfdepositsRatesBuckets = pd.read_excel(depositsRatesBuckets, sheetname=[0, 1], skiprows=6, header=None)

    # Create a MultIindex columns because the dataframe in each sheet has repeating column names
    idx = pd.MultiIndex.from_tuples([('individual', c) for c in dfdepositsRatesBuckets[0].columns[:12]] +
                                    [('entity', c) for c in dfdepositsRatesBuckets[0].columns[12:]])

    # Define the column names for level 1 of the MultiIndex
    cols = ['month',
            'demand deposits',
            'up to 30 days (including demand deposits)',
            'up to 30 days (except demand deposits)',
            '31 to 90 days',
            '91 to 180 days',
            '181 days to 1 year',
            'up to 1 year (including demand deposits)',
            'up to 1 year (except demand deposits',
            '1 to 3 years',
            'over 3 years',
            'over 1 year',
            'up to 30 days (including demand deposits)',
            '31 to 90 days',
            '91 to 180 days',
            '181 days to 1 year',
            'up to 1 year (including demand deposits)',
            '1 to 3 years',
            'over 3 years',
            'over 1 year']

    # Loop over the dictionary of dataframes and extract the data needed into 2 lists for further calculations
    # Data mangling in the loop:
    # 1. Remove new lines and white space from the month (first) column
    # 2. Filter out lines which are not months defined in the first dataset index based on Deposit_amounts.xlsx file
    # 3. Replace - with NaNs
    # 4. Assign the MultiIndex columns to the dataset
    # 5. Extract the first table from the dataset (weighted average annual interest rates on deposits in the reporting
    # month and are calculated based on annual interest rates, which are stated in deposit agreement
    # and volume of borrowed funds in the reporting month.)
    # 6. Extract the second table from the dataset (Share of deposits on each maturity, borrowed in the reporting month,
    #  in total volume of deposits in the reporting month, borrowed from individuals and nonfinancial organizations.)
    # 7. Multiply the numbers in the second table with their respective deposit amount (individual/entity)
    # for each month
    dfDepositsAnnIntRate = list()
    dfDepositsStructureIntRate = list()
    for k in dfdepositsRatesBuckets:
        dfdepositsRatesBuckets[k][0] = dfdepositsRatesBuckets[k][0].str.strip()  # 1
        dfdepositsRatesBuckets[k] = dfdepositsRatesBuckets[k][
            dfdepositsRatesBuckets[k][0].isin(dfDepositAmounts.index)]  # 2
        dfdepositsRatesBuckets[k] = dfdepositsRatesBuckets[k].replace('-', np.nan)  # 3
        dfdepositsRatesBuckets[k].columns = idx  # 4
        dfdepositsRatesBuckets[k].columns = dfdepositsRatesBuckets[k].columns.set_levels(cols, level=1)  # 4
        tmpDf1 = dfdepositsRatesBuckets[k].ix[:6]  # 5
        tmpDf1.set_index(tmpDf1.columns[0], inplace=True)
        tmpDf1.index.name = 'month'
        dfDepositsAnnIntRate.append(tmpDf1 / 100)
        tmpDf2 = dfdepositsRatesBuckets[k].ix[7:]  # 6
        tmpDf2.set_index(tmpDf2.columns[0], inplace=True)
        tmpDf2 = tmpDf2 / 100
        if k == 0:
            curr = 'rub'
        else:
            curr = 'usd'
        # 7:
        tmpDf2_1 = tmpDf2.iloc[:, tmpDf2.columns.get_level_values(0) == 'individual'].multiply(
            dfDepositAmounts['individual_{}'.format(curr)], axis=0)
        tmpDf2_2 = tmpDf2.iloc[:, tmpDf2.columns.get_level_values(0) == 'entity'].multiply(
            dfDepositAmounts['entities_{}'.format(curr)], axis=0)
        dfDepositsStructureIntRate.append(tmpDf2_1.join(tmpDf2_2).round())

    # Calculate the number of days in a month based on the deposit amounts dataset date
    daysInMonth = pd.to_numeric((dfDepositAmounts.dt + MonthEnd(1)).apply('{:%d}'.format))
    daysInMonth = daysInMonth.to_frame()

    # Add order column to the dataset of days in months (Used later for sorting)
    daysInMonth['order'] = [r + 1 for r in range(len(daysInMonth))]
    # daysInMonth

    # 5. Extract the monthly avg weighted fx rates from the url and create a dataset out of them
    webRates = get_dataframe_from_url(url)

    # Filter out months starting with Q (as they are not months but quaters) then convert the rates to a number.
    # Conversion is needed because everything coming from a website is considered text
    webRates = webRates[~webRates.month_short.str.startswith('Q')]
    webRates['rate'] = pd.to_numeric(webRates.rate)
    # for z in zip(webRates.index, daysInMonth.index):
    #     print(z)
    # daysInMonth.index[daysInMonth.index.str.startswith('Jan')][0]

    # Map the full month name from the first dataset with the months short name from the url,
    # set it as index and delete the moths short name
    webRates['month'] = webRates.month_short.apply(lambda x: daysInMonth.index[daysInMonth.index.str.startswith(x)][0])
    webRates.set_index('month', inplace=True)
    del(webRates['month_short'])
    # webRates

    # 6. Create and excel with calculations applied
    writer = pd.ExcelWriter('files/depositsResult.xlsx')

    # Loop over the deposits structures datasets in different currencies (rub, usd)
    # 1. Take the annual weighted interest rate and divide it by 365 then multiply it by the days in a month
    # 2. Multiply the deposit structures with the result of the above operation
    # 3. If currency is RUB divide by the usd exchange rate to get the USD equivalent
    # 4. Add order column to the dataset and order by it then drop it and write dataset as a sheet to the excel file
    # 5. Save the excel file
    for n, df in enumerate(dfDepositsStructureIntRate):
        t = None
        if n == 0:
            t = 'RUB'
        else:
            t = 'USD'
        df2 = dfDepositsAnnIntRate[n].divide(365, axis=0).multiply(daysInMonth.dt, axis=0)  # 1
        df = df.multiply(df2, axis=0).round()  # 2
        if t == 'RUB':
            df = df.divide(webRates.rate, axis=0).round()  # 3
        df['entity', 'order'] = daysInMonth['order']  # 4
        df = df.sort_values(('entity', 'order'))  # 4
        df.drop('order', axis=1, level=1, inplace=True)  # 4
        df.to_excel(writer, 'sheet %s' % t)
    writer.save()  # 5
    # Et voila!


def get_dataframe_from_url(url):
    r = requests.get(url)
    data = r.text
    soup = BeautifulSoup(data, 'lxml')

    # The relevant data we need is in a table with id table28
    table28 = soup.find(id='table28')
    # type(table28)

    # Loop over the 1 and the 6 rows of the table and extract the months names and their respective fx rates
    header = list()
    info = list()
    for n, tr in enumerate(table28.findAll('tr')):
        if n in (0,):
            # print(n, tr)
            for td in tr.findAll('td'):
                header.append(td.getText())
        elif n in (5,):
            for td in tr.findAll('td'):
                info.append(td.getText())

    # Remove . from months name and zip together the months and their fx rates and create a dataframe out of the zip
    # then replace empty strings with NaN and drop the empty fields
    zipped = list(zip([h.strip().replace('.', '') for h in header], [h.strip() for h in info]))
    return pd.DataFrame.from_records(zipped, columns=['month_short', 'rate']).replace('', np.nan).dropna()

if __name__ == "__main__":
    main()