import requests
import pandas as pd
import os
import matplotlib.pyplot as plt

def extractFunc(year):
    params = dict(
    offset=0,
    start=f"{year}-01-01T00:00",
    end=f"{year}-12-31T00:00",
    sort="Minutes5UTC DESC",
    limit=0
    )
    url = 'https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime'
    response = requests.get(url=url,params=params)
    records = response.json().get('records', [])
    return pd.DataFrame(records)
    
def loadFunc(energy_df,year):
    print('Loading data to csv...')

    output_dir = 'Data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = os.path.join(output_dir, f'energi_{year}.csv')
    
    print(f'energi_{year}.csv')
    energy_df.to_csv(filename, index=False)

    print('Data loaded to csv...')
    return filename

def transformFunc(filename):
    print('Transform data...')
    df = pd.read_csv(filename)

    totalPowerLt100MW = df[df["ProductionLt100MW"] != 0]["ProductionLt100MW"].sum()
    totalPowerGe100MW = df[df["ProductionGe100MW"]!= 0]["ProductionGe100MW"].sum()
    totalPower = totalPowerLt100MW + totalPowerGe100MW

    totalOffshoreWind = df[df["OffshoreWindPower"]!= 0]["OffshoreWindPower"].sum()
    totalOnshoreWind = df[df["OnshoreWindPower"]!= 0]["OnshoreWindPower"].sum()
    totalWindPower = totalOffshoreWind + totalOnshoreWind

    totalSolarPower = df[df["SolarPower"]!= 0]["SolarPower"].sum()

    totalPowerGenerated = totalPowerLt100MW + totalPowerGe100MW + totalOffshoreWind + totalOnshoreWind + totalSolarPower

    exportGreatBeltDK1 = df[df['PriceArea'] == 'DK1']["ExchangeGreatBelt"].sum()
    exportGreatBeltDK2 = df[df['PriceArea'] == 'DK2']["ExchangeGreatBelt"].sum()
    exportGreatBeltTotal = exportGreatBeltDK1 + exportGreatBeltDK2

    exportGermanyDK1 = df[df['PriceArea'] == 'DK1']["ExchangeGermany"].sum()
    exportGermanyDK2 = df[df['PriceArea'] == 'DK2']["ExchangeGermany"].sum()
    exportGermanyTotal = exportGermanyDK1 + exportGermanyDK2

    exportNetherlandsDK1 = df[df['PriceArea'] == 'DK1']["ExchangeNetherlands"].sum()
    exportNetherlandsDK2 = df[df['PriceArea'] == 'DK2']["ExchangeNetherlands"].sum()
    exportNetherlandsTotal = exportNetherlandsDK1 + exportNetherlandsDK2
    
    exportGreatBritainDK1 = df[df['PriceArea'] == 'DK1']["ExchangeGreatBritain"].sum()
    exportGreatBritainDK2 = df[df['PriceArea'] == 'DK2']["ExchangeGreatBritain"].sum()
    exportGreatBritainTotal = exportGreatBritainDK1 + exportGreatBritainDK2

    exportNorwayDK1 = df[df['PriceArea'] == 'DK1']["ExchangeNorway"].sum()
    exportNorwayDK2 = df[df['PriceArea'] == 'DK2']["ExchangeNorway"].sum()
    exportNorwayTotal = exportNorwayDK1 + exportNorwayDK2

    exportSwedenDK1 = df[df['PriceArea'] == 'DK1']["ExchangeSweden"].sum()
    exportSwedenDK2 = df[df['PriceArea'] == 'DK2']["ExchangeSweden"].sum()
    exportSwedenTotal = exportSwedenDK1 + exportSwedenDK2

    totalPowerExported = exportGermanyTotal + exportGreatBritainTotal + exportNetherlandsTotal + exportNorwayTotal + exportSwedenTotal
    totalPowerRemaining = totalPowerGenerated - totalPowerExported

    return {
        "Solar power generated (MW)": totalSolarPower,
        "Wind power generated (MW)": totalWindPower,
        "Power generated (MW)": totalPower,
        "Total power generated (MW)": totalPowerGenerated,
        "Total power exported (MW)": totalPowerExported,
        
        "Power remaining after export (MW)": totalPowerRemaining,
        
        #"Net Export through Great Belt from DK1 (MW)": exportGreatBeltDK1,
        #"Net Export through Great Belt from DK2 (MW)": exportGreatBeltDK2,
        #"Total Net Export through Great Belt": exportGreatBeltTotal,

        "Net Export to Germany from DK1 (MW)": exportGermanyDK1,
        "Net Export to Germany from DK2 (MW)": exportGermanyDK2,
        "Total Net Export to Germany (MW)": exportGermanyTotal,

        "Net Export to Netherlands from DK1 (MW)": exportNetherlandsDK1,
        "Net Export to Netherlands from DK2 (MW)": exportNetherlandsDK2,
        "Total Net Export to the Netherlands (MW)": exportNetherlandsTotal,

        "Net Export to Great Britain from DK1 (MW)": exportGreatBritainDK1,
        "Net Export to Great Britain from DK2 (MW)": exportGreatBritainDK2,
        "Total Net Export to Great Britain (MW)": exportGreatBritainTotal,

        "Net Export to Norway from DK1 (MW)": exportNorwayDK1,
        "Net Export to Norway from DK2 (MW)": exportNorwayDK2,
        "Total Net Export to Norway (MW)": exportNorwayTotal,

        "Net Export to Sweden from DK1 (MW)": exportSwedenDK1,
        "Net Export to Sweden from DK2 (MW)": exportSwedenDK2,
        "Total Net Export to Sweden (MW)": exportSwedenTotal
        
    }

def loadExportData(year,result):
    df_result = pd.DataFrame(list(result.items()), columns=['Description', 'Value'])

    df_result.to_csv(f'energiExport_{year}.csv', index=False)

    print('Exporting Export data to CSV...')

    df_result = pd.read_csv(f'energiExport_{year}.csv')
    
    print('Graphing data...')

    # Pie chart to show distribution of power generated
    totalPowerLt100MW = result.get('Power generated (MW)', 0)
    totalPowerGe100MW = result.get('Power generated (MW)', 0)
    totalOffshoreWind = result.get('Wind power generated (MW)', 0)
    totalOnshoreWind = result.get('Wind power generated (MW)', 0)
    totalSolarPower = result.get('Solar power generated (MW)', 0)

    power_sources = [totalPowerLt100MW, totalPowerGe100MW, totalOffshoreWind, totalOnshoreWind, totalSolarPower]
    power_labels = ['< 100 MW', '≥ 100 MW', 'Offshore Wind', 'Onshore Wind', 'Solar Power']
    power_colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ffb3e6']

    plt.figure(figsize=(8, 8))
    plt.pie(power_sources, labels=power_labels, colors=power_colors, autopct='%1.1f%%', startangle=140)
    
    plt.title(f'Power Sources Distribution in {year}', fontsize=14)
    plt.savefig(f'power_sources_distribution_{year}.png', format='png', dpi=300)
    plt.show()

    exports = [
        result.get("Total Net Export to Germany (MW)", 0),
        result.get("Total Net Export to the Netherlands (MW)", 0),
        result.get("Total Net Export to Great Britain (MW)", 0),
        result.get("Total Net Export to Norway (MW)", 0),
        result.get("Total Net Export to Sweden (MW)", 0),
        result.get("Power remaining after export (MW)", 0)
    ]
    
    # Labels for the bar chart
    labels = ['Germany', 'Netherlands', 'Great Britain', 'Norway', 'Sweden', 'Denmark (Remaining power)']

    # Create the bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(labels, exports, color=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#c2c2f0', '#ffb3e6'])

    # Add title and labels
    plt.title(f'Export Distribution as Total Power Generated in {year}', fontsize=14)
    plt.xlabel('Countries and Remaining Power', fontsize=12)
    plt.ylabel('Export Power (MW)', fontsize=12)

    # Display the chart
    plt.tight_layout()  # Adjust layout to prevent clipping of tick-labels
    plt.savefig(f'export_distribution_{year}.png', format='png', dpi=300)
    plt.show()


def runData():
    # Extract
    input_year = input('Enter year: ')
    df = extractFunc(input_year)

    #Load
    filename = loadFunc(df, input_year)
    
    #Transform
    result = transformFunc(filename)

    #LoadExportData
    filename = loadExportData(input_year, result)

    for key, value in result.items():
        print(f"{key}: {value}")