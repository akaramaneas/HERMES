import pandas as pd
import os
import sys

finput = sys.argv[1]

def transform_names_with_modes(df, df_with_modes):
    df_c = df.copy()
    for i, tech in enumerate(df_with_modes['TECHNOLOGY']):
        fuel = df_with_modes.loc[i, 'FUEL']
        df_c.loc[(df_c['TECHNOLOGY'] == tech) & (df_c['FUEL'] == fuel), 'TECHNOLOGY'] = f'{tech}_{fuel}' # **
    for t in set(df_with_modes['TECHNOLOGY']):
        df_c = df_c[df_c['TECHNOLOGY'] != t]
    return df_c

def aggregate(df, tech_to_aggr):
    aggr = df.iloc[0:0].copy()
    for tech_aggr in sorted(tech_to_aggr.values()):
        aggr.loc[tech_aggr] = 0.0
    df_c = df.reset_index().copy()
    for i, tech in enumerate(df_c['TECHNOLOGY']):
        aggr.loc[tech_to_aggr[tech]] += df_c.loc[i]

    #aggr.loc['Total'] = aggr.sum()

    return aggr

def get_numb(s):
    n = 100
    if s[0].isdigit():
        if not s[1].isdigit():
            n = int(s[0])
        else:
            n = int(s[:2])
    else:
        n = 100000*(ord(s[0].lower())-ord('a')) + 1000*(ord(s[1].lower())-ord('a'))+ (ord(s[2].lower())-ord('a'))
        
    return n



foutput = 'results'

Variables_dict = {'UndiscountedFOM': ['REGION', 'TECHNOLOGY', 'YEAR'], 'DiscountedFOM': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    'UndiscountedVOM': ['REGION', 'TECHNOLOGY', 'YEAR'], 'DiscountedVOM': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    #'UndiscountedCapInv': ['REGION', 'TECHNOLOGY', 'YEAR'], 'DiscountedCapInv': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    'UndiscountedTechnologyEmissionsPenalty': ['REGION', 'TECHNOLOGY', 'YEAR'], 
                    'DiscountedTechnologyEmissionsPenalty': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    #'UndiscountedSalvVal': ['REGION', 'TECHNOLOGY', 'YEAR'], 'DiscountedSalvVal': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    'ObjFunction': ['REGION', 'TECHNOLOGY', 'YEAR'], 'ObjFunctionStor': ['REGION', 'STORAGE', 'YEAR'], 'ObjFunctionInter': ['REGION', 'INTERCONNECTION', 'YEAR'],
                    #'CapitalInvestment': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    #'UndiscountedStorCost': ['REGION', 'STORAGE', 'YEAR'], 'DiscountedStorCost': ['REGION', 'STORAGE', 'YEAR'],
                    #'UndiscountedStorSalvVal': ['REGION', 'STORAGE', 'YEAR'], 'DiscountedStorSalvVal': ['REGION', 'STORAGE', 'YEAR'],
                    #'CapRecFactor': ['REGION', 'TECHNOLOGY'], 'PvAnn': ['REGION', 'TECHNOLOGY'],
                    #'CapRecFactMulPvAnn': ['REGION', 'TECHNOLOGY'],
                    #'DRI': ['REGION', 'TECHNOLOGY'], 'OR': ['REGION', 'TECHNOLOGY'],
                    'vAnnCapex': ['REGION', 'TECHNOLOGY', 'YEAR'], 'vAnnCapexStor': ['REGION', 'STORAGE', 'YEAR'], #'MulCapex': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    'DiscountedvAnnCapex': ['REGION', 'TECHNOLOGY', 'YEAR'], 'DiscountedvAnnCapexStor': ['REGION', 'STORAGE', 'YEAR'], 'TotalCapacityAnnual': ['REGION', 'TECHNOLOGY', 'YEAR'],
                    'ProductionByTechnologyAnnual': ['REGION', 'TECHNOLOGY', 'FUEL', 'YEAR'],
                    'AnnualEmissions':['REGION', 'EMISSION', 'YEAR'],
                    'NewCapacity':['REGION', 'TECHNOLOGY', 'YEAR'],
                    'ProductionByTechnology':['REGION','TIMESLICE','TECHNOLOGY','FUEL','YEAR'],
                    'VRECurtailment':['REGION','TIMESLICE','YEAR'],'ObjFunctionCurtailment':['REGION','YEAR'],'ObjFunctionSurplus':['REGION','YEAR'],
                    #'NetChargeWithinDay':['REGION','STORAGE','SEASON','DAYTYPE','DAILYTIMEBRACKET','YEAR'],
                    'Demand':['REGION','TIMESLICE','FUEL','YEAR'],'Surplus':['REGION','TIMESLICE','FUEL','YEAR'],'AnnualTechnologyEmission': ['REGION','TECHNOLOGY','EMISSION','YEAR'],
                    'StorageLevel':['REGION','STORAGE','SEASON','DAYTYPE','DAILYTIMEBRACKET','YEAR'],
                    'NewStorageCapacity':['REGION','STORAGE','YEAR'], 'InterUndiscountedTradeCost': ['REGION', 'INTERCONNECTION', 'FUEL', 'YEAR'], 'InterDiscountedTradeCost': ['REGION', 'INTERCONNECTION', 'FUEL', 'YEAR'], 'TotalImportsperTS': ['REGION', 'FUEL', 'TIMESLICE', 'YEAR'], 'TotalExportsperTS': ['REGION', 'FUEL', 'TIMESLICE', 'YEAR'], 'YearlyImports': ['REGION', 'INTERCONNECTION', 'FUEL', 'YEAR'], 'YearlyExports': ['REGION', 'INTERCONNECTION', 'FUEL', 'YEAR'], 'TotalCapacityInterconnection': ['REGION', 'INTERCONNECTION', 'FUEL', 'YEAR'], 'ImportsperTS': ['REGION', 'INTERCONNECTION', 'FUEL', 'TIMESLICE', 'YEAR'], 'ExportsperTS': ['REGION', 'INTERCONNECTION', 'FUEL', 'TIMESLICE', 'YEAR'], 'Trade': ['REGION1', 'REGION2','TIMESLICE', 'FUEL', 'YEAR']
                    }          

os.mkdir(foutput)

with open(finput, mode='r') as f:
    lines = f.readlines()

for variable in Variables_dict:
    processed_lines = [line.split() for line in lines if line.split()[0] == variable]
    cols_df = pd.read_csv(f'Inputs/{Variables_dict[variable][-1]}.csv')
    cols = sorted(cols_df['VALUE'].unique())
    df_var = pd.DataFrame(processed_lines, columns=['VARIABLE', *Variables_dict[variable][:-1], *cols])
    df_sel_var = df_var.copy()
    df_sel_var2 = df_sel_var.melt(id_vars=['VARIABLE', *Variables_dict[variable][:-1]],
                                  var_name=Variables_dict[variable][-1], value_name='VALUE')
    df_sel_var3 = df_sel_var2.sort_values(by=Variables_dict[variable])
    df_sel_var3.to_csv(f'{foutput}/{variable}.csv', index=False)

# Aggregation

tech_to_aggr_df = pd.read_csv('Mapping_Tech_to_Aggr_Tech.csv')
Aggr_Techs = {}
for index, technology in enumerate(tech_to_aggr_df['TECH']):
    Aggr_Techs[technology] = tech_to_aggr_df.loc[index, 'AGRR_TECH']

Aggr_Techs_mode = Aggr_Techs.copy()
techs_with_modes_to_aggr_df = pd.read_csv('Technologies_With_Modes.csv')
for index, technology in enumerate(techs_with_modes_to_aggr_df['TECHNOLOGY']):
    name_of_tech = f'{technology}_{techs_with_modes_to_aggr_df.loc[index, "FUEL"]}' # **
    Aggr_Techs_mode[name_of_tech] = techs_with_modes_to_aggr_df.loc[index, 'AGRR_TECH']
for t in set(techs_with_modes_to_aggr_df['TECHNOLOGY']):
    Aggr_Techs_mode.pop(t)

writer = pd.ExcelWriter(f'{foutput}/Results_Our_Way_V2.xlsx', engine='xlsxwriter')
for variable in Variables_dict:
    df = pd.read_csv(f'{foutput}/{variable}.csv')
    #if variable in ('vAnnCapex', 'DiscountedvAnnCapex', 'NewCapacity'):
    #    df_to_excel = df.pivot_table(index='TECHNOLOGY', aggfunc='sum', values='VALUE',
    #                                     columns=Variables_dict[variable][-1])
    #    df_to_excel.to_excel(writer, sheet_name=f'diss_{variable[:23]}')
    #    
    #    aggr_df = aggregate(df_to_excel, Aggr_Techs)
    #
    #    aggr_df.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
        
    #elif Variables_dict[variable] == ['REGION', 'TECHNOLOGY']:
    #    aggr_df = df[['TECHNOLOGY', 'VALUE']]
    #    aggr_df.to_csv(f'{foutput}/{variable}.csv', index='False')
    #    aggr_df.to_excel(writer, sheet_name=f'{variable[:28]}')
    #elif Variables_dict[variable] == ['REGION', 'YEAR']:
    #    aggr_df = df[['YEAR', 'VALUE']]
    #    aggr_df.to_csv(f'{foutput}/{variable}.csv', index='False')
    #    aggr_df.to_excel(writer, sheet_name=f'{variable[:28]}')
    if variable in ('ProductionByTechnology', 'VRECurtailment','Demand', 'StorageLevel', 'Trade','Surplus'):
        pass
    elif variable in ('ObjFunctionCurtailment', 'ObjFunctionSurplus'):
        df_to_excel = df.pivot_table(index='REGION', aggfunc='sum', values='VALUE',
                                     columns='YEAR')
    elif variable in ('NewStorageCapacity','ResidualStorageCapacity','ObjFunctionStor','vAnnCapexStor','DiscountedvAnnCapexStor'):
        df_to_excel = df.pivot_table(index='STORAGE', aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

#        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/aggr_{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    elif variable in ('ImportsperTS', 'ExportsperTS'):
        df_to_excel = df.pivot_table(index=['INTERCONNECTION','FUEL','TIMESLICE'], aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

        #        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/aggr_{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    elif variable in ('InterUndiscountedTradeCost', 'InterDiscountedTradeCost', 'YearlyImports', 'YearlyExports', 'TotalCapacityInterconnection'):
        df_to_excel = df.pivot_table(index=['INTERCONNECTION','FUEL'], aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

        #        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/aggr_{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    elif variable in ('TotalImportsperTS', 'TotalExportsperTS'):
        df_to_excel = df.pivot_table(index=['FUEL', 'TIMESLICE'], aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

        #        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/aggr_{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    elif variable == 'ObjFunctionInter':
        df_to_excel = df.pivot_table(index='INTERCONNECTION', aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

        #        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/aggr_{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    elif Variables_dict[variable] == ['REGION', 'EMISSION', 'YEAR']:
        df_to_excel = df.pivot_table(index='EMISSION', aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])

#        aggr_df = aggregate(df_to_excel, Aggr_Techs)

        df_to_excel.to_csv(f'{foutput}/{variable}.csv')
        df_to_excel.to_excel(writer, sheet_name=f'{variable[:28]}')
    elif variable == 'ProductionByTechnologyAnnual':
        df_to_excel = transform_names_with_modes(df, techs_with_modes_to_aggr_df)
        
        df_to_excel = df_to_excel.pivot_table(index='TECHNOLOGY', aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])
            
        aggr_df = aggregate(df_to_excel, Aggr_Techs_mode)
        
        # PJ to TWh
        if variable == 'ProductionByTechnologyAnnual':
            aggr_df = aggr_df * 0.277777778


        #if variable in ('ProductionByTechnologyAnnual', 'TotalCapacityAnnual'):
        aggr_df = aggr_df.sort_values(by='TECHNOLOGY')
                                                 
        aggr_df.loc['Total'] = aggr_df.sum()
        
        #Demand - VRE Curtailment
        dem = pd.read_csv(f'{foutput}/Demand.csv')
        dem_to_excel = dem.pivot_table(index='FUEL', aggfunc='sum', values='VALUE',
                                     columns='YEAR')
                                     
        VRE_curt = pd.read_csv(f'{foutput}/VRECurtailment.csv')
        VRE_curt_to_excel = VRE_curt.pivot_table(index='REGION', aggfunc='sum', values='VALUE',
                                     columns='YEAR')

        sur = pd.read_csv(f'{foutput}/Surplus.csv')
        sur_to_excel = sur.pivot_table(index='FUEL', aggfunc='sum', values='VALUE',
                                     columns='YEAR')
        # PJ to TWh
        dem_to_excel = dem_to_excel * 0.277777778      
        VRE_curt_to_excel = VRE_curt_to_excel * 0.277777778
        sur_to_excel = sur_to_excel * 0.277777778
        
        # to df
        aggr_df.loc['Demand'] = dem_to_excel.loc['Elec_Demand'].values
        #aggr_df.loc['VRECurtailment'] = VRE_curt_to_excel.loc['Greece'].values
        total_vre_curt = VRE_curt_to_excel.sum(axis=0)
        aggr_df.loc['VRECurtailment'] = total_vre_curt.values
        aggr_df.loc['Surplus'] = sur_to_excel.loc['Elec_Demand'].values
        
        aggr_df.to_csv(f'{foutput}/aggr_{variable}.csv')
        aggr_df.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')
    else:
        df_to_excel = df.pivot_table(index='TECHNOLOGY', aggfunc='sum', values='VALUE',
                                     columns=Variables_dict[variable][-1])
        
        #if variable in ('vAnnCapex', 'MulCapex', 'Subsidies_a', 'Subsidies_b', 'NewCapacity'):
        #    df_to_excel.to_excel(writer, sheet_name=f'diss_{variable[:23]}')
            
        aggr_df = aggregate(df_to_excel, Aggr_Techs)
        
        # PJ to TWh
        #if variable == 'ProductionByTechnologyAnnual':
        #    aggr_df = aggr_df * 0.277777778
        
        #if variable in ('ProductionByTechnologyAnnual', 'TotalCapacityAnnual'):
        aggr_df = aggr_df.sort_values(by='TECHNOLOGY')
                                                 
        aggr_df.loc['Total'] = aggr_df.sum()
        
        aggr_df.to_csv(f'{foutput}/aggr_{variable}.csv')
        aggr_df.to_excel(writer, sheet_name=f'aggr_{variable[:23]}')

writer.close()
