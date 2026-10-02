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
        aggr_df.loc['Surplus'] = sur_to_excel.loc['Elec_Transmission'].values
        
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

# ----------------------------------------------------
# NEW: Aggregate Capacity & Generation per REGION
# (consistent with national logic + proper storage handling)
# ----------------------------------------------------

print("Building per-region aggregated capacity & generation files...")

os.makedirs(f"{foutput}/RegionResults", exist_ok=True)

# Load required result CSVs produced earlier
cap_df     = pd.read_csv(f'{foutput}/TotalCapacityAnnual.csv')
prod_df    = pd.read_csv(f'{foutput}/ProductionByTechnologyAnnual.csv')
imports_df = pd.read_csv(f'{foutput}/YearlyImports.csv')
exports_df = pd.read_csv(f'{foutput}/YearlyExports.csv')
trade_df   = pd.read_csv(f'{foutput}/Trade.csv')
dem_df     = pd.read_csv(f'{foutput}/Demand.csv')
vrec_df    = pd.read_csv(f'{foutput}/VRECurtailment.csv')

# Region list based on Production (fallback to Capacity)
regions = sorted(prod_df['REGION'].dropna().unique())

def reindex_to_years(series, year_cols):
    s = series.copy()
    s.index = s.index.astype(str)
    yc = [str(c) for c in year_cols]
    return s.reindex(yc).fillna(0)

for region in regions:
    region_clean = str(region).strip()

    # ==========================================================
    # 1) CAPACITY PER REGION
    # ==========================================================
    cap_reg = cap_df[cap_df['REGION'] == region_clean].copy()
    if not cap_reg.empty:
        cap_pivot = cap_reg.pivot_table(index='TECHNOLOGY', columns='YEAR',
                                        values='VALUE', aggfunc='sum')
        cap_aggr = cap_aggr = aggregate(cap_pivot, Aggr_Techs)
        cap_aggr = cap_aggr.sort_index()
    else:
        cap_aggr = pd.DataFrame()

    cap_aggr.to_excel(f"{foutput}/RegionResults/{region_clean}_Capacity.xlsx")

    # ==========================================================
    # 2) GENERATION PER REGION (WITH CURT, DEM, STORAGE, TRADE)
    # ==========================================================
    prod_reg = prod_df[prod_df['REGION'] == region_clean].copy()
    if prod_reg.empty:
        pd.DataFrame().to_excel(f"{foutput}/RegionResults/{region_clean}_Generation.xlsx")
        continue

    # aggregate technologies (same as national)
    prod_reg = transform_names_with_modes(prod_reg, techs_with_modes_to_aggr_df)
    gen_pivot = prod_reg.pivot_table(index='TECHNOLOGY', columns='YEAR',
                                     values='VALUE', aggfunc='sum')
    gen_aggr = aggregate(gen_pivot, Aggr_Techs_mode)
    gen_aggr = gen_aggr * 0.277777778   # PJ → TWh
    gen_aggr = gen_aggr.sort_index()
    year_cols = gen_aggr.columns.tolist()

    # ==========================================================
    # CURTAILMENT (from VRECurtailment)
    # ==========================================================
    vrec_reg = vrec_df[vrec_df['REGION'] == region_clean].copy()
    if not vrec_reg.empty:
        curt_by_year = vrec_reg.groupby('YEAR')['VALUE'].sum() * 0.277777778
        curt_by_year = reindex_to_years(curt_by_year, year_cols)
        gen_aggr.loc['Curtailment'] = curt_by_year.values
    else:
        gen_aggr.loc['Curtailment'] = 0

    # ==========================================================
    # DEMAND (Elec_Demand)
    # ==========================================================
    dem_reg = dem_df[(dem_df['REGION'] == region_clean) &
                     (dem_df['FUEL'] == 'Elec_Demand')].copy()
    if not dem_reg.empty:
        dem_by_year = dem_reg.groupby('YEAR')['VALUE'].sum() * 0.277777778
        dem_by_year = reindex_to_years(dem_by_year, year_cols)
        gen_aggr.loc['Demand'] = dem_by_year.values
    else:
        gen_aggr.loc['Demand'] = 0

    # ==========================================================
    # STORAGE (based on Technologies_With_Modes.csv mapping)
    # ==========================================================
    twm = techs_with_modes_to_aggr_df.copy()
    storage_rows = twm[twm['FUEL'].astype(str).str.lower() == 'elec_storage'].copy()

    charge_aggr_set = set(storage_rows[storage_rows['AGRR_TECH'].str.contains('charge', case=False)]['AGRR_TECH'])
    discharge_aggr_set = set(storage_rows[storage_rows['AGRR_TECH'].str.contains('discharge', case=False)]['AGRR_TECH'])

    # fallback: search aggregated names instead
    if not charge_aggr_set:
        charge_aggr_set = {name for name in gen_aggr.index if 'charge' in str(name).lower()}
    if not discharge_aggr_set:
        discharge_aggr_set = {name for name in gen_aggr.index if 'discharge' in str(name).lower()}

    # sum aggregated rows
    present_charge = [c for c in charge_aggr_set if c in gen_aggr.index]
    present_discharge = [d for d in discharge_aggr_set if d in gen_aggr.index]

    if present_charge:
        gen_aggr.loc['Storage_Charge'] = gen_aggr.loc[present_charge].sum(axis=0).values
    else:
        gen_aggr.loc['Storage_Charge'] = 0

    if present_discharge:
        gen_aggr.loc['Storage_Discharge'] = gen_aggr.loc[present_discharge].sum(axis=0).values
    else:
        gen_aggr.loc['Storage_Discharge'] = 0

    # net
    try:
        gen_aggr.loc['Storage_Net'] = gen_aggr.loc['Storage_Discharge'].astype(float) - gen_aggr.loc['Storage_Charge'].astype(float)
    except:
        gen_aggr.loc['Storage_Net'] = 0

    # ==========================================================
    # INTERNAL NET TRADE (Positive = Imports, Negative = Exports)
    # OSeMOSYS native sign: export is +, import is -
    # We invert the sign so that: + = net importer, - = net exporter
    # ==========================================================

    tr = trade_df[
        (trade_df['REGION1'] == region_clean) &
        (trade_df['FUEL'] == 'Elec_Transmission')
        ].copy()

    if not tr.empty:
        # Sum native OSeMOSYS values (export positive, import negative)
        native_sum = tr.groupby('YEAR')['VALUE'].sum()

        # Convert PJ → TWh
        native_sum = native_sum * 0.277777778

        # Invert sign so: positive = importer, negative = exporter
        net_internal = -native_sum

        # Ensure year alignment
        net_internal = net_internal.reindex(year_cols, fill_value=0)

        gen_aggr.loc['Net_Trade'] = net_internal.values
    else:
        gen_aggr.loc['Net_Trade'] = 0

    # ==========================================================
    # INTERNATIONAL NET IMPORTS (YearlyImports / Exports)
    # ==========================================================
    imp = imports_df[(imports_df['REGION'] == region_clean) &
                     (imports_df['FUEL'] == 'Elec_Transmission')].copy()
    exp = exports_df[(exports_df['REGION'] == region_clean) &
                     (exports_df['FUEL'] == 'Elec_Transmission')].copy()

    if not imp.empty or not exp.empty:
        imp_y = imp.groupby('YEAR')['VALUE'].sum()
        exp_y = exp.groupby('YEAR')['VALUE'].sum()
        net_intl = (imp_y.reindex(year_cols, fill_value=0) -
                    exp_y.reindex(year_cols, fill_value=0)) * 0.277777778
        gen_aggr.loc['Net_Imports_All_Interconnections'] = net_intl.values
    else:
        gen_aggr.loc['Net_Imports_All_Interconnections'] = 0

    # save file
    gen_aggr.to_excel(f"{foutput}/RegionResults/{region_clean}_Generation.xlsx")

print("Finished creating per-region capacity and generation outputs.")
