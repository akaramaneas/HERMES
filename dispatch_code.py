import pandas as pd
import os


def transform_names_with_modes(df, df_with_modes):
    """
    Transforms technology names in a DataFrame by appending fuel names
    for technologies listed in df_with_modes, and removes original tech names.
    """
    df_c = df.copy()
    for i, tech in enumerate(df_with_modes['TECHNOLOGY']):
        fuel = df_with_modes.loc[i, 'FUEL']
        df_c.loc[(df_c['TECHNOLOGY'] == tech) & (df_c['FUEL'] == fuel), 'TECHNOLOGY'] = f'{tech}_{fuel}'
    for t in set(df_with_modes['TECHNOLOGY']):
        df_c = df_c[df_c['TECHNOLOGY'] != t]
    return df_c


def standardize_timeslice_col(df, timeslice_col='TIMESLICE'):
    """
    Standardizes TIMESLICE column format to 'mXX_dYY_tZZ'.
    Handles variations like 's' prefix, lack of prefix, or single digit numbers.
    Assumes TIMESLICE can be split by '_'.
    """
    if timeslice_col not in df.columns:
        # print(f"DEBUG standardize_timeslice_col: '{timeslice_col}' not found in DataFrame columns. Skipping standardization.")
        return df  # No TIMESLICE column to standardize

    df_copy = df.copy()  # Work on a copy to avoid SettingWithCopyWarning

    # Convert to string and lower case
    df_copy[timeslice_col] = df_copy[timeslice_col].astype(str).str.lower()

    def format_timeslice_part(part_str, prefix):
        # Remove any existing prefix, convert to int, then format with new prefix and padding
        if part_str and part_str[0].isalpha():  # Check if it starts with a letter (e.g., 'm', 'd', 't', 's')
            num_str = part_str[1:]
        else:
            num_str = part_str  # Assume it's just a number or empty if malformed

        try:
            return f"{prefix}{int(num_str):02d}"
        except ValueError:
            # Handle cases where conversion to int fails (e.g., malformed string like 'm_d_t')
            # This should ideally be caught by model inputs being clean, but defensively.
            # print(f"Warning: Could not parse timeslice part '{part_str}' into a number for standardization. Returning original.")
            return part_str  # Return original if cannot parse safely

    # Apply standardization to each part of the TIMESLICE string
    def apply_standardization(ts_str):
        parts = ts_str.split('_')
        if len(parts) == 3:
            m_part = format_timeslice_part(parts[0], 'm')  # Always standardize to 'm' for month/season
            d_part = format_timeslice_part(parts[1], 'd')
            h_part = format_timeslice_part(parts[2], 't')
            return f"{m_part}_{d_part}_{h_part}"
        else:
            # This handles cases where TIMESLICE might be missing or malformed
            # print(f"Warning: Unexpected TIMESLICE format '{ts_str}'. Cannot standardize fully.")
            return ts_str  # Return original if not 3 parts

    df_copy[timeslice_col] = df_copy[timeslice_col].apply(apply_standardization)
    return df_copy


def process_region_data(
        region_name,
        year,
        prod_raw_source_all,
        demand_all,
        vre_curtailment_all,
        total_imports_all,
        total_exports_all,
        storage_level_all,
        trade_data_all,
        tech_to_aggr_df,
        techs_with_modes_to_aggr_df,
        year_split_all,
        storages_all
):
    """
    Processes OSeMOSYS output data for a specific region and year,
    including detailed bilateral trade data.
    Returns two DataFrames: one with original values and one with adjusted values.
    """
    print(f"\n--- Processing data for Region: {region_name}, Year: {year} ---")

    # Helper function to filter DataFrame by region and year if 'REGION' column exists
    def filter_df(df, region, year_filter):
        if 'REGION' in df.columns:
            return df[(df['REGION'] == region) & (df['YEAR'] == year_filter)].copy()
        else:  # If 'REGION' column doesn't exist, assume data is for the whole system (or implicitly for the region)
            return df[df['YEAR'] == year_filter].copy()

    # Filter and standardize data for the current region and year
    # Apply standardize_timeslice_col after filtering but before further processing
    prod_raw_region = standardize_timeslice_col(filter_df(prod_raw_source_all, region_name, year))
    demand_region = standardize_timeslice_col(filter_df(demand_all, region_name, year))
    demand_region = demand_region[demand_region['FUEL'] == 'Elec_Demand'].copy()

    vre_curtailment_region = standardize_timeslice_col(filter_df(vre_curtailment_all, region_name, year))
    total_imports_region = standardize_timeslice_col(filter_df(total_imports_all, region_name, year))
    total_imports_region = total_imports_region[total_imports_region['FUEL'] == 'Elec_Transmission'].copy()

    total_exports_region = standardize_timeslice_col(filter_df(total_exports_all, region_name, year))
    total_exports_region = total_exports_region[total_exports_region['FUEL'] == 'Elec_Transmission'].copy()

    # --- Storage Level data handling ---
    # Filter storage_level_all for region and year first
    storage_level_region_filtered = filter_df(storage_level_all, region_name, year)

    # Ensure a 'TIMESLICE' column exists and is standardized in storage_level_region
    if 'TIMESLICE' not in storage_level_region_filtered.columns:
        # Create a synthetic TIMESLICE column if it doesn't exist (typical for StorageLevel.csv)
        if all(col in storage_level_region_filtered.columns for col in ['SEASON', 'DAYTYPE', 'DAILYTIMEBRACKET']):
            storage_level_region_filtered['TIMESLICE'] = (
                    's' + storage_level_region_filtered['SEASON'].astype(str) +
                    '_d' + storage_level_region_filtered['DAYTYPE'].astype(str) +
                    '_t' + storage_level_region_filtered['DAILYTIMEBRACKET'].astype(str)
            )
            # print(f"DEBUG Storage: Created 'TIMESLICE' column from S/D/T for storage data for {region_name}.")
        else:
            print(
                f"WARNING: StorageLevel data for {region_name}, {year} does not have 'TIMESLICE' or S/D/T columns. Cannot process storage data correctly.")
            # Set to empty DataFrame to prevent errors later if essential columns are missing
            storage_level_region = pd.DataFrame()

            # Now standardize the TIMESLICE column regardless of whether it was original or newly created
    storage_level_region = standardize_timeslice_col(storage_level_region_filtered, timeslice_col='TIMESLICE')
    # --- End Storage Level data handling ---

    # YearSplit handling: filter by year only, as it's universal.
    year_split_region = standardize_timeslice_col(year_split_all[year_split_all['YEAR'] == year].copy())

    # Handle technologies with modes
    prod_raw_region = transform_names_with_modes(prod_raw_region, techs_with_modes_to_aggr_df)

    # Check if prod_raw_region is empty after filtering
    if prod_raw_region.empty:
        print(f"Warning: No production data for Region: {region_name}, Year: {year}. Skipping this region/year.")
        empty_df = pd.DataFrame(index=pd.MultiIndex.from_product([[], [], []], names=['Month', 'Day', 'Hour']))
        return empty_df, empty_df

    # Pivot production data
    prod_raw_pivot = prod_raw_region.pivot_table(index='TIMESLICE', aggfunc='sum', values='VALUE',
                                                 columns='TECHNOLOGY').reset_index()

    # Create Timeslice components (now simpler due to TIMESLICE standardization)
    prod_raw_pivot['Month'] = prod_raw_pivot['TIMESLICE'].apply(lambda x: x.split('_')[0])
    prod_raw_pivot['Day'] = prod_raw_pivot['TIMESLICE'].apply(lambda x: x.split('_')[1])
    prod_raw_pivot['Hour'] = prod_raw_pivot['TIMESLICE'].apply(lambda x: x.split('_')[2])

    # Aggregation_1 (Technologies)
    Aggr_Techs = {}
    for index, technology in enumerate(tech_to_aggr_df['TECH']):
        Aggr_Techs[technology] = tech_to_aggr_df.loc[index, 'AGRR_TECH']

    Aggr_Techs_mode = Aggr_Techs.copy()
    for index, technology in enumerate(techs_with_modes_to_aggr_df['TECHNOLOGY']):
        name_of_tech = f'{technology}_{techs_with_modes_to_aggr_df.loc[index, "FUEL"]}'
        Aggr_Techs_mode[name_of_tech] = techs_with_modes_to_aggr_df.loc[index, 'AGRR_TECH']
    for t in set(techs_with_modes_to_aggr_df['TECHNOLOGY']):
        Aggr_Techs_mode.pop(t, None)

    prod_pr = prod_raw_pivot[['TIMESLICE', 'Month', 'Day', 'Hour']].copy()
    for aggr_tech in sorted(Aggr_Techs_mode.values()):
        prod_pr[aggr_tech] = 0.0

    for tech in Aggr_Techs_mode.keys():
        if tech in prod_raw_pivot.columns:
            prod_pr[Aggr_Techs_mode[tech]] += prod_raw_pivot[tech]

    # Sort by TIMESLICE (This is the canonical order for all subsequent merges/assignments)
    # The sort_key_func still relies on parsing the standardized string
    sort_key_func = lambda col: col.apply(
        lambda e: 100000 * int(e.split('_')[0][1:]) + 100 * int(e.split('_')[1][1:]) + 1 * int(e.split('_')[2][1:]))
    prod_pr = prod_pr.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(
        drop=True)  # Reset index after sort for cleaner merges

    # Add Demand
    if not demand_region.empty:
        demand_region = demand_region.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(drop=True)
        prod_pr['Demand'] = \
        pd.merge(prod_pr[['TIMESLICE']], demand_region[['TIMESLICE', 'VALUE']], on='TIMESLICE', how='left')[
            'VALUE'].fillna(0).values
    else:
        prod_pr['Demand'] = 0.0

    # Add VRE Curtailment
    # print(f"DEBUG VRECurtailment: Is vre_curtailment_region empty after filter? {vre_curtailment_region.empty}")
    if not vre_curtailment_region.empty:
        vre_curtailment_region = vre_curtailment_region.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(
            drop=True)

        # DEBUG: Check TIMESLICE consistency for VRECurtailment
        # print(f"DEBUG VRECurtailment: First 5 TIMESLICE from prod_pr (main data):\n{prod_pr['TIMESLICE'].head().to_string()}")
        # print(f"DEBUG VRECurtailment: First 5 TIMESLICE from vre_curtailment_region:\n{vre_curtailment_region['TIMESLICE'].head().to_string()}")

        common_vrec_timeslices = set(prod_pr['TIMESLICE']).intersection(set(vre_curtailment_region['TIMESLICE']))
        # print(f"DEBUG VRECurtailment: Number of common TIMESLICE for VRECurtailment: {len(common_vrec_timeslices)}")
        if len(common_vrec_timeslices) == 0 and not vre_curtailment_region.empty:
            print(
                "ERROR: No common TIMESLICE found between main data and VRECurtailment. This is the likely cause of mismatch.")
            # print("       Check TIMESLICE formats: e.g., 'm1_d1_t1' vs 'S1_D1_T1' or extra spaces in VRECurtailment.csv.")

        prod_pr['VRECurtailment'] = \
        pd.merge(prod_pr[['TIMESLICE']], vre_curtailment_region[['TIMESLICE', 'VALUE']], on='TIMESLICE', how='left')[
            'VALUE'].fillna(0).values
        # print(f"DEBUG VRECurtailment: First 5 VRECurtailment values after merge:\n{prod_pr['VRECurtailment'].head().to_string()}")

        # Sum of VRECurtailment
        # print(f"DEBUG VRECurtailment Sum: Sum of input VRECurtailment for {region_name}, {year}: {vre_curtailment_region['VALUE'].sum():.10f}")
        # print(f"DEBUG VRECurtailment Sum: Sum of VRECurtailment in prod_pr after merge for {region_name}, {year}: {prod_pr['VRECurtailment'].sum():.10f}")

    else:
        prod_pr['VRECurtailment'] = 0.0

    # Add Total Imports and Exports
    if not total_imports_region.empty:
        total_imports_region = total_imports_region.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(
            drop=True)
        prod_pr['TotalImports'] = \
        pd.merge(prod_pr[['TIMESLICE']], total_imports_region[['TIMESLICE', 'VALUE']], on='TIMESLICE', how='left')[
            'VALUE'].fillna(0).values
    else:
        prod_pr['TotalImports'] = 0.0

    if not total_exports_region.empty:
        total_exports_region = total_exports_region.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(
            drop=True)
        prod_pr['TotalExports'] = \
        pd.merge(prod_pr[['TIMESLICE']], total_exports_region[['TIMESLICE', 'VALUE']], on='TIMESLICE', how='left')[
            'VALUE'].fillna(0).values
    else:
        prod_pr['TotalExports'] = 0.0

    # --- Detailed Trade Data Handling ---
    # print(f"DEBUG TRADE: Starting detailed trade data handling for {region_name}, {year}")

    clean_region_name = region_name.strip()

    regional_trade_data_filtered = trade_data_all[
        ((trade_data_all['REGION1'] == clean_region_name) | (trade_data_all['REGION2'] == clean_region_name)) &
        (trade_data_all['YEAR'] == year) &
        (trade_data_all['FUEL'] == 'Elec_Transmission')
        ].copy()

    # Standardize TIMESLICE in trade data before further processing
    regional_trade_data = standardize_timeslice_col(regional_trade_data_filtered)

    # print(f"DEBUG TRADE: Shape of regional_trade_data after initial filter and standardization: {regional_trade_data.shape}")
    if regional_trade_data.empty:
        # print("DEBUG TRADE: regional_trade_data is empty for this region/year. No bilateral trade columns will be generated.")
        pass  # No need to pre-initialize columns if there's no trade data
    else:
        partners_as_region1 = regional_trade_data[regional_trade_data['REGION1'] == clean_region_name]['REGION2']
        partners_as_region2 = regional_trade_data[regional_trade_data['REGION2'] == clean_region_name]['REGION1']

        all_trading_partners = pd.concat([partners_as_region1, partners_as_region2]).unique().tolist()
        all_trading_partners = [p.strip() for p in all_trading_partners if
                                p is not None and p.strip() != clean_region_name]

        # print(f"DEBUG TRADE: All unique trading partners identified for {clean_region_name}: {all_trading_partners}")
        # print(f"DEBUG TRADE: Number of unique trading partners: {len(all_trading_partners)}")

        # Populate trade columns
        temp_trade_df = pd.DataFrame(index=prod_pr['TIMESLICE'].unique())
        temp_trade_df.index.name = 'TIMESLICE'

        # `regional_trade_data_processed` is no longer needed as `regional_trade_data` is already standardized
        # print("DEBUG TRADE: Populating trade columns...")
        for _, row in regional_trade_data.iterrows():  # Use regional_trade_data directly
            timeslice = row['TIMESLICE']
            value = row['VALUE']
            region1 = row['REGION1']
            region2 = row['REGION2']

            col_name = None

            # Only create trade columns if region1 and region2 are different
            if region1 != region2:
                if region2 == clean_region_name:  # Incoming trade (import) from a *different* region
                    col_name = f'Trade_From_{region1}'
                elif region1 == clean_region_name:  # Outgoing trade (export) to a *different* region
                    col_name = f'Trade_To_{region2}'

            if col_name and timeslice in temp_trade_df.index:
                if col_name not in temp_trade_df.columns:
                    temp_trade_df[col_name] = 0.0
                temp_trade_df.loc[timeslice, col_name] += value
            # else:
            # print(f"DEBUG TRADE: Skipping trade row (col_name is None or timeslice not in prod_pr index): {row.to_dict()}")

        # print(f"DEBUG TRADE: temp_trade_df columns generated: {temp_trade_df.columns.tolist()}")

        prod_pr = pd.merge(prod_pr, temp_trade_df.reset_index(), on='TIMESLICE', how='left')
        trade_cols_from_temp = [col for col in temp_trade_df.columns if col != 'TIMESLICE']
        prod_pr[trade_cols_from_temp] = prod_pr[trade_cols_from_temp].fillna(0)

    # After merge, prod_pr now has all columns.
    # print(f"DEBUG TRADE: All columns in prod_pr before final sort and pivot: {prod_pr.columns.tolist()}")
    # print(f"DEBUG TRADE: Number of columns in prod_pr before final sort and pivot: {len(prod_pr.columns)}")

    prod_pr_sorted = prod_pr.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(drop=True)

    # Timeslice adjustment
    if year_split_region.empty:
        print(f"Warning: No YearSplit data for year {year}. Cannot apply time slice adjustment.")
        prod_pr_sorted_adj = prod_pr_sorted.copy()
    else:
        year_split_region = year_split_region.sort_values(by='TIMESLICE', key=sort_key_func).reset_index(drop=True)

        def time_slice_func(x):
            h = x * 8760
            if h <= 1:
                return 1
            else:
                return h

        year_split_region['VALUE_ADJ'] = year_split_region['VALUE'].apply(time_slice_func)

        prod_pr_sorted_adj = prod_pr_sorted.copy()

        fixed_cols = ['TIMESLICE', 'Month', 'Day', 'Hour']

        do_not_adjust_cols_prefixes = ('StorageLevel_',)
        do_not_adjust_exact_cols = ()

        all_numeric_cols = [col for col in prod_pr_sorted_adj.columns if col not in fixed_cols]
        cols_to_adjust = [
            col for col in all_numeric_cols
            if not col.startswith(do_not_adjust_cols_prefixes) and col not in do_not_adjust_exact_cols
        ]

        # print(f"DEBUG YearSplit: Columns that WILL be adjusted by YearSplit: {cols_to_adjust}")
        # print(f"DEBUG YearSplit: Columns that will NOT be adjusted: {[c for c in all_numeric_cols if c not in cols_to_adjust]}")

        prod_pr_sorted_adj = pd.merge(prod_pr_sorted_adj, year_split_region[['TIMESLICE', 'VALUE_ADJ']],
                                      on='TIMESLICE', how='left').fillna(1)

        prod_pr_sorted_adj['VALUE_ADJ'] = pd.to_numeric(prod_pr_sorted_adj['VALUE_ADJ'], errors='coerce').fillna(1)
        prod_pr_sorted_adj[cols_to_adjust] = prod_pr_sorted_adj[cols_to_adjust].div(prod_pr_sorted_adj['VALUE_ADJ'],
                                                                                    axis=0)
        prod_pr_sorted_adj = prod_pr_sorted_adj.drop(columns=['VALUE_ADJ'])

    # Storage Level (These values are state variables, not flows, so they remain unadjusted by YearSplit)
    # This loop now relies on `storage_level_region` being correctly processed above
    if not storage_level_region.empty:  # Only proceed if storage data was successfully prepared
        for stor in storages_all['VALUE'].unique():
            df_s = storage_level_region[storage_level_region['STORAGE'] == stor].copy()

            if not df_s.empty:
                timeslice_col_for_merge = 'TIMESLICE'  # This is now guaranteed to exist and be standardized

                df_s = df_s.sort_values(by=timeslice_col_for_merge, key=sort_key_func).reset_index(drop=True)

                common_timeslices = set(prod_pr_sorted['TIMESLICE']).intersection(set(df_s[timeslice_col_for_merge]))
                if len(common_timeslices) == 0:  # and not df_s.empty: # df_s.empty check is redundant here
                    print(
                        f"ERROR: No common TIMESLICE found between main data and StorageLevel for {stor} in {region_name}, {year}. This is the likely cause of zeros for this storage.")
                    # print(f"       Check TIMESLICE formats: e.g., 'm1_d1_t1' vs 'S1_D1_T1' or extra spaces.")

                merged_storage = pd.merge(
                    prod_pr_sorted[['TIMESLICE']],
                    df_s[[timeslice_col_for_merge, 'VALUE']],
                    left_on='TIMESLICE',
                    right_on=timeslice_col_for_merge,
                    how='left'
                )
                prod_pr_sorted[f'StorageLevel_{stor}'] = merged_storage['VALUE'].fillna(0).values

                merged_storage_adj = pd.merge(
                    prod_pr_sorted_adj[['TIMESLICE']],
                    df_s[[timeslice_col_for_merge, 'VALUE']],
                    left_on='TIMESLICE',
                    right_on=timeslice_col_for_merge,
                    how='left'
                )
                prod_pr_sorted_adj[f'StorageLevel_{stor}'] = merged_storage_adj['VALUE'].fillna(0).values

            else:
                # If df_s is empty for a specific storage, ensure column is created with zeros
                prod_pr_sorted[f'StorageLevel_{stor}'] = 0.0
                prod_pr_sorted_adj[f'StorageLevel_{stor}'] = 0.0
    else:  # If storage_level_region was initially empty or couldn't be processed
        for stor in storages_all['VALUE'].unique():
            prod_pr_sorted[f'StorageLevel_{stor}'] = 0.0
            prod_pr_sorted_adj[f'StorageLevel_{stor}'] = 0.0

    # Final pivoting for output - Adjust column order here
    all_current_cols = [col for col in prod_pr_sorted.columns if col not in ['TIMESLICE', 'Month', 'Day', 'Hour']]

    # These column ordering lists also need to reflect the new adjustment logic
    tech_cols_for_order = sorted([col for col in all_current_cols if not col.startswith(
        ('Trade_', 'StorageLevel_', 'Demand', 'VRECurtailment', 'TotalImports', 'TotalExports'))])
    fixed_order_cols_for_order = ['Demand', 'VRECurtailment', 'TotalImports', 'TotalExports']
    storage_cols_for_order = sorted([col for col in all_current_cols if col.startswith('StorageLevel_')])
    trade_cols_for_order = sorted([col for col in all_current_cols if col.startswith('Trade_')])

    # Combine for the desired order for the individual sheet
    desired_values_cols_order = tech_cols_for_order + fixed_order_cols_for_order + storage_cols_for_order + trade_cols_for_order

    values_cols_set = set(all_current_cols)
    final_values_cols = [col for col in desired_values_cols_order if col in values_cols_set]
    final_values_cols.extend([col for col in values_cols_set if col not in final_values_cols])

    prod_pr_final = prod_pr_sorted.pivot_table(index=['Month', 'Day', 'Hour'], values=final_values_cols, aggfunc='sum',
                                               sort=False)
    prod_pr_final_adj = prod_pr_sorted_adj.pivot_table(index=['Month', 'Day', 'Hour'], values=final_values_cols,
                                                       aggfunc='sum', sort=False)

    return prod_pr_final, prod_pr_final_adj


# --- Main Script Logic ---
# Load all necessary static and input files once
try:
    # Aggressively strip spaces from region names upon loading Trade.csv
    trade_data_raw = pd.read_csv('Trade.csv')
    if 'REGION1' in trade_data_raw.columns:
        trade_data_raw['REGION1'] = trade_data_raw['REGION1'].astype(str).str.strip()
    if 'REGION2' in trade_data_raw.columns:
        trade_data_raw['REGION2'] = trade_data_raw['REGION2'].astype(str).str.strip()
    trade_data_all = trade_data_raw  # Assign the cleaned DataFrame

    prod_raw_source_all = pd.read_csv('ProductionByTechnology.csv')
    demand_all = pd.read_csv('Demand.csv')
    vre_curtailment_all = pd.read_csv('VRECurtailment.csv')
    total_imports_all = pd.read_csv('TotalImportsperTS.csv')
    total_exports_all = pd.read_csv('TotalExportsperTS.csv')
    storage_level_all = pd.read_csv('StorageLevel.csv')
    year_split_all = pd.read_csv('../Inputs/YearSplit.csv')

    tech_to_aggr_df = pd.read_csv('../Mapping_Tech_to_Aggr_Tech.csv')
    techs_with_modes_to_aggr_df = pd.read_csv('../Technologies_With_Modes.csv')
    storages_all = pd.read_csv('../Inputs/STORAGE.csv')

except FileNotFoundError as e:
    print(f"Error: Required file not found - {e}. Please ensure all CSV files are in the correct directory.")
    exit()

# Identify all unique regions and active years
if 'REGION' not in prod_raw_source_all.columns:
    print(
        "WARNING: 'REGION' column not found in ProductionByTechnology.csv. Assuming a single default region 'Global'.")
    regions = ['Global']
    prod_raw_source_all['REGION'] = 'Global'

    # Add 'REGION' column to other dataframes if missing and assuming 'Global'
    if 'REGION' not in demand_all.columns: demand_all['REGION'] = 'Global'
    if 'REGION' not in vre_curtailment_all.columns: vre_curtailment_all['REGION'] = 'Global'
    if 'REGION' not in total_imports_all.columns: total_imports_all['REGION'] = 'Global'
    if 'REGION' not in total_exports_all.columns: total_exports_all['REGION'] = 'Global'
    if 'REGION' not in storage_level_all.columns: storage_level_all['REGION'] = 'Global'
else:
    regions_from_prod = prod_raw_source_all['REGION'].unique()
    # Ensure regions themselves are stripped for consistency
    regions = [r.strip() for r in regions_from_prod]

active_years = sorted(prod_raw_source_all['YEAR'].unique())

output_folder = 'outputs'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

aggregated_prod_final_yearly = {}
aggregated_prod_final_adj_yearly = {}

for active_year in active_years:
    regional_results_for_aggregation = []
    regional_adj_results_for_aggregation = []
    first_region_columns = None  # To store column order from the first region

    for region in regions:
        # Pass the stripped region name to the function
        region_prod_final, region_prod_final_adj = process_region_data(
            region,
            active_year,
            prod_raw_source_all,
            demand_all,
            vre_curtailment_all,
            total_imports_all,
            total_exports_all,
            storage_level_all,
            trade_data_all,
            tech_to_aggr_df,
            techs_with_modes_to_aggr_df,
            year_split_all,
            storages_all
        )

        if not region_prod_final.empty and not region_prod_final_adj.empty:
            if first_region_columns is None:
                first_region_columns = region_prod_final.columns.tolist()

            region_output_filename = os.path.join(output_folder, f'Dispatch_{region}_{active_year}.xlsx')
            writer = pd.ExcelWriter(region_output_filename, engine='xlsxwriter')
            region_prod_final.to_excel(writer, sheet_name='Prod(PJ)')
            region_prod_final_adj.to_excel(writer, sheet_name='Prod(PJ)_Adj')
            writer.close()
            print(f"Saved regional dispatch for {region} in {active_year} to {region_output_filename}")

            regional_results_for_aggregation.append(region_prod_final)
            regional_adj_results_for_aggregation.append(region_prod_final_adj)
        else:
            print(f"Skipping saving empty dispatch for {region} in {active_year}.")

    if regional_results_for_aggregation:
        all_columns_union = pd.Index([])
        # Identify bilateral trade columns that should NOT be summed directly in aggregation
        trade_columns_to_exclude_from_sum = set()

        for df in regional_results_for_aggregation:
            all_columns_union = all_columns_union.union(df.columns)
            for col in df.columns:
                if col.startswith('Trade_From_') or col.startswith('Trade_To_'):
                    trade_columns_to_exclude_from_sum.add(col)

        # Reindex all regional dataframes to have the same columns
        reindexed_results = [df.reindex(columns=all_columns_union, fill_value=0) for df in
                             regional_results_for_aggregation]
        reindexed_adj_results = [df.reindex(columns=all_columns_union, fill_value=0) for df in
                                 regional_adj_results_for_aggregation]

        # Concatenate and group by timeslice
        # This will sum all columns including bilateral trade for now
        aggregated_prod_final_raw = pd.concat(reindexed_results, axis=0).groupby(level=['Month', 'Day', 'Hour']).sum()
        aggregated_prod_final_adj_raw = pd.concat(reindexed_adj_results, axis=0).groupby(
            level=['Month', 'Day', 'Hour']).sum()

        # NOW, drop the bilateral trade columns from the aggregated dataframe
        # because their direct sum is not meaningful for system-wide dispatch
        aggregated_prod_final = aggregated_prod_final_raw.drop(columns=list(trade_columns_to_exclude_from_sum),
                                                               errors='ignore')
        aggregated_prod_final_adj = aggregated_prod_final_adj_raw.drop(columns=list(trade_columns_to_exclude_from_sum),
                                                                       errors='ignore')

        # Recalculate column order for the aggregated file based on the *remaining* columns
        master_all_current_cols = [col for col in aggregated_prod_final.columns if col not in ['Month', 'Day', 'Hour']]

        # These lists define the preferred order for the aggregated dispatch chart
        master_tech_cols = sorted([col for col in master_all_current_cols if not col.startswith(
            ('StorageLevel_', 'Demand', 'VRECurtailment', 'TotalImports', 'TotalExports'))])
        master_fixed_order_cols = ['Demand', 'VRECurtailment', 'TotalImports', 'TotalExports']
        master_storage_cols = sorted([col for col in master_all_current_cols if col.startswith('StorageLevel_')])
        # Note: master_trade_cols are deliberately omitted here as they were dropped.

        master_desired_values_cols_order = master_tech_cols + master_fixed_order_cols + master_storage_cols

        aggregated_cols_set = set(aggregated_prod_final.columns)

        final_master_order_for_aggregation = [col for col in master_desired_values_cols_order if
                                              col in aggregated_cols_set]
        final_master_order_for_aggregation.extend(
            [col for col in aggregated_cols_set if col not in final_master_order_for_aggregation])

        aggregated_prod_final = aggregated_prod_final.reindex(columns=final_master_order_for_aggregation, fill_value=0)
        aggregated_prod_final_adj = aggregated_prod_final_adj.reindex(columns=final_master_order_for_aggregation,
                                                                      fill_value=0)

        aggregated_prod_final_yearly[active_year] = aggregated_prod_final
        aggregated_prod_final_adj_yearly[active_year] = aggregated_prod_final_adj

        aggregated_output_filename = os.path.join(output_folder, f'Dispatch_Aggregated_{active_year}.xlsx')
        writer = pd.ExcelWriter(aggregated_output_filename, engine='xlsxwriter')
        aggregated_prod_final.to_excel(writer, sheet_name='Prod(PJ)')
        aggregated_prod_final_adj.to_excel(writer, sheet_name='Prod(PJ)_Adj')
        writer.close()
        print(f"Saved aggregated dispatch for all regions in {active_year} to {aggregated_output_filename}")
    else:
        print(f"No regional data found to aggregate for year {active_year}.")

print("\nScript execution complete. Check the 'outputs' folder for results.")