"""

Goal: Read in raw Nielsen Retail Scanner files, downloadable from the
Kilts File Selection System
https://kiltsfiles.chicagobooth.edu/Requests/Create-New-Request.aspx

NielsenRetail.py is an auxiliary file that defines the following classes
(1) NielsenRetail

See Example.py for implementation of the NielsenRetail functions
"""




import pandas as pd
import dask.dataframe as dd
from dask import delayed
import dask
import dask.array as da
import pathlib as path
import time


# To reset start time (with current time)
_start_time = time.time()
def tick():
    """
    Start Timer
    """
    global _start_time
    _start_time = time.time()
# To calculate elapsed time (since tick() function)
def tock():
    """
    Stop Timer and Print Time Passed
    """
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec,60)
    (t_hour,t_min) = divmod(t_min,60)
    print('Time passed: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))




dict_types = {
    'upc': pd.UInt64Dtype(),
    'upc_ver_uc': pd.UInt8Dtype(),
    'product_module_code': pd.UInt16Dtype(),
    'brand_code_uc': pd.UInt32Dtype(),
    'multi': pd.UInt16Dtype(),
    'size1_code_uc': pd.UInt16Dtype(),
    'year': pd.UInt16Dtype(),
    'panel_year': pd.UInt16Dtype(),
    'dma_code': pd.UInt16Dtype(),
    'retailer_code': pd.UInt16Dtype(),
    'parent_code': pd.UInt16Dtype(),
    'store_zip3': pd.UInt16Dtype(),
    'fips_county_code': pd.UInt16Dtype(),
    'fips_state_code': pd.UInt8Dtype(),
    'store_code_uc': pd.UInt32Dtype(),
    'week_end': pd.UInt32Dtype(),
    'units': pd.UInt64Dtype(),
    'prmult': pd.UInt8Dtype(),
    'feature': pd.Int8Dtype(),
    'display': pd.Int8Dtype(),
    'price': 'float64',
    'flavor_code': pd.UInt64Dtype(),
    'flavor_descr': 'object',
    'form_code': pd.UInt64Dtype(),
    'form_descr': 'object',
    'formula_code': pd.UInt64Dtype(),
    'formula_descr': 'object',
    'container_code': pd.UInt64Dtype(),
    'container_descr': 'object',
    'salt_content_code': pd.UInt64Dtype(),
    'salt_content_descr': 'object',
    'style_code': pd.UInt64Dtype(),
    'style_descr': 'object',
    'type_code': pd.UInt64Dtype(),
    'type_descr': 'object',
    'product_code': pd.UInt64Dtype(),
    'product_descr': 'object',
    'variety_code': pd.UInt64Dtype(),
    'variety_descr': 'object',
    'organic_claim_code': pd.UInt64Dtype(),
    'organic_claim_descr': 'object',
    'usda_organic_seal_code': pd.UInt64Dtype(),
    'usda_organic_seal_descr': 'object',
    'common_consumer_name_code': pd.UInt64Dtype(),
    'common_consumer_name_descr': 'object',
    'strength_code': pd.UInt64Dtype(),
    'strength_descr': 'object',
    'scent_code': pd.UInt64Dtype(),
    'scent_descr': 'object',
    'dosage_code': pd.UInt64Dtype(),
    'dosage_descr': 'object',
    'gender_code': pd.UInt64Dtype(),
    'gender_descr': 'object',
    'target_skin_condition_code': pd.UInt64Dtype(),
    'target_skin_condition_descr': 'object',
    'use_code': pd.UInt64Dtype(),
    'use_descr': 'object',
    'size2_code': pd.UInt64Dtype(),
    'size2_amount': 'float64',
    'size2_units': 'object',
    'deal_flag_uc': pd.UInt8Dtype(),
    'quantity': pd.UInt16Dtype(),
    'household_code': pd.UInt32Dtype(),
    'Household_Cd': pd.UInt32Dtype(),
    'Fips_County_Desc': 'object',
    'Fips_State_Desc': 'object',
    'Scantrack_Market_Identifier_Desc': 'object',
    'DMA_Name': 'object',
}


dict_column_map = {'Household_Cd':'household_code',
                   'Panel_Year':'panel_year'}


# function to get all files within a folder
def get_files(self):
    """
    Input: NielsenRetail object
    Get all Files for NielsenRetail object
    """
    files = [i for i in self.dir_read.glob('**/*.*sv') if '._' not in i.stem]
    if len(files) == 0:
        str_err= ('Found no files! Check folder name ' +
        'and make sure folder is unzipped.')
        raise Exception(str_err)
    return files


def get_year(file, type='Sales'):
    """
    Arguments:
        file: filename
        type: 'Sales' or 'Ann' (see below)

    Get year a particular file corresponds to
    Will throw error if not an annual file
    if type = 'Sales': the last half of the filename is the year.
    Use type = 'Sales' if in Retail

    if type = 'Ann': the grandparent folder contains the year name
    Use type = 'Ann' if in Panel
    """
    if type == 'Ann':  # these have one less root
        return int(file.parent.parent.name)

    # Extract potential year and validate it
    try:
        year_part = file.stem.split('_')[-1]
        return int(year_part)
    except ValueError:
        print(f"Skipping file with invalid year format: {file}")
        return None


def get_products(self, upc_list=None,
                 keep_groups=None, drop_groups=None,
                 keep_modules=None, drop_modules=None,
                 keep_departments=None, drop_departments=None):
    """
    Arguments:
        Required: NielsenRetail object
        Optional: keep_groups, drop_groups, keep_modules, drop_modules
        Each takes a list of group codes or module codes

    Select the Product file and read it in Nielsen Retail Reader
    Many Filter Options:
    upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
    keep_groups, drop_groups: selects or drops product group codes
    keep_modules, drop_modules: selects or drops product module codes
    """
    if self.files_product:
        self.file_products = self.files_product[0]
    else:
        str_err = ('Could not find a valid products.tsv under ',
                   'Master_Files/Latest. Check folder name ',
                   'and make sure folder is unzipped.')
        raise Exception(str_err)

    ddf_products = dd.read_csv(self.file_products, delimiter='\t', encoding='latin', dtype={
        'department_code': 'float64',
        'product_group_code': 'float64',
        'size1_code_uc': 'float64'},
                               assume_missing=True
                               )

    # Filter DataFrame based on conditions
    if keep_groups:
        ddf_products = ddf_products[ddf_products['product_group_code'].isin(keep_groups)]
    if drop_groups:
        ddf_products = ddf_products[~ddf_products['product_group_code'].isin(drop_groups)]
    if keep_modules:
        ddf_products = ddf_products[ddf_products['product_module_code'].isin(keep_modules)]
    if drop_modules:
        ddf_products = ddf_products[~ddf_products['product_module_code'].isin(drop_modules)]
    if keep_departments:
        ddf_products = ddf_products[ddf_products['department_code'].isin(keep_departments)]
    if drop_departments:
        ddf_products = ddf_products[~ddf_products['department_code'].isin(drop_departments)]

    ddf_products['size1_units'] = ddf_products['size1_units'].astype('category')
    ddf_products['product_module_descr'] = ddf_products['product_module_descr'].astype('category')
    ddf_products['product_group_code'] = ddf_products['product_group_code'].astype('category')

    # Filter by UPC list
    if upc_list:
        ddf_products = ddf_products[ddf_products['upc'].isin(upc_list)]

    # Sort by UPC and reset index
    ddf_products = ddf_products.sort_values('upc').reset_index(drop=True)

    # Assign to class attribute
    self.df_products = ddf_products.compute().copy()

    if self.verbose:
        print('Successfully Read in Products with Shape ', self.df_products.shape)

    del (ddf_products)

    return


def get_extra(self, years=None, upc_list=None):
    if years == None:
        years = self.all_years

    files_extra_in = [f for f in self.files_extra
                      if get_year(f) in years]

    dd_extra = dd.read_csv(
        files_extra_in,
        delimiter='\t',
        dtype=dict_types,
        assume_missing=True)

    if upc_list:
        dd_extra = dd_extra[dd_extra['upc'].isin(upc_list)]

    dd_extra = dd_extra.sort_values(['upc', 'panel_year']).reset_index(drop=True)

    self.df_extra = dd_extra.compute().copy()
    # not going to edit the missings or anything of the sort
    if self.verbose == True:
        print('Successfully Read in Extra Files with Shape', self.df_extra.shape)

    del (dd_extra)
    # sort by UPC
    return


# Define class NielsenRetai will contain all methods we use to read in the Retail Scanner Data
class NielsenRetail(object):
    """
    Object class to read in Nielsen Retail Scanner Data
    Files created:
        df_extra: from annual product_extra files
        df_products: from Master product file
        df_rms: from Annual rms_versions files
        df_stores: from Annual stores files
        df_sales: from Movement files
    Can filter based on store locations (DMAs), product groups,
    product modules, and years
    """

    def __init__(self, dir_read, verbose=True):
        self.verbose = verbose
        self.dir_read = path.Path(dir_read) # convert the input string into a Path object

        # Get all files in the relevant folder
        self.files = get_files(self)

        self.files_product = [f for f in self.files if
                              (f.name == 'products.tsv') &
                              (f.parent.name == 'Latest') &
                              (f.parent.parent.name == 'Master_Files')]

        self.files_annual = [f for f in self.files
                             if 'Annual_Files' in f.parts]

        self.files_sales = [f for f in self.files
                            if 'Movement_Files' in f.parts]
        if not self.files_sales:
            raise Exception('Could not find Movement Files!')

        try:
            self.all_groups = set(self.get_group(f) for f in self.files_sales)
        except:
            raise Exception('Could not get Group Code from Movement Files. Use original Nielsen naming conventions')

        try:
            self.all_modules = set(self.get_module(f) for f in self.files_sales)
        except:
            raise Exception('Could not get Module Code from Movement Files. Use original Nielsen naming conventions')

        try:
            self.all_years = set(get_year(f) for f in self.files_sales if get_year(f) is not None)
        except:
            raise Exception('Could not get Year from Movement Files. Use original Nielsen naming conventions')

        self.files_stores = [f for f in self.files_annual if 'stores' in f.name]
        self.files_rms = [f for f in self.files_annual if 'rms_versions' in f.name]
        self.files_extra = [f for f in self.files_annual if 'products_extra' in f.name]

        self.dict_stores = {get_year(f): f for f in self.files_stores if get_year(f) is not None}
        self.dict_rms = {get_year(f): f for f in self.files_rms if get_year(f) is not None}
        self.dict_extra = {get_year(f): f for f in self.files_extra if get_year(f) is not None}

        self.dict_sales = {y: [f for f in self.files_sales if get_year(f) == y]
                           for y in self.all_years}

        self.df_products = dd.from_pandas(pd.DataFrame(), npartitions=1)
        self.df_sales = dd.from_pandas(pd.DataFrame(), npartitions=16)
        self.df_stores = dd.from_pandas(pd.DataFrame(), npartitions=1)
        self.df_rms = {}
        self.df_extra = dd.from_pandas(pd.DataFrame(), npartitions=1)

        return



    
    # given the Path of a sales file, find its module code
    def get_module(self, file_sales):
        """
        Given the Path of a sales file, find its module code
        """
        return int(file_sales.stem.split('_')[0])

    # given the Path of a sales file, find its group code
    def get_group(self, file_sales):
        """
        Given the Path of a sales file, find its group code
        """
        return int(file_sales.parent.stem.split('_')[0])


    # Begin a Proper Cleanup: filter years, groups, modules, etc.
    def filter_years(self, keep = None, drop = None):
        """
        Function: selects years of sales to include
        Arguments: keep, drop: both take lists of years
        Re-runs of this method are cumulative: cannot retrieve dropped years
        without re-initializing your Retail Reader object
        """
        # go through each of the four file types, and keep only the keys that correspond to the years we want

        def aux_filter_years(orig_dict, keep = None, drop = None):
            new_dict = orig_dict
            if keep:
                new_dict = {y: f for y, f in new_dict.items() if y in keep}
            if drop:
                new_dict = {y: f for y, f in new_dict.items() if y not in drop}
            return new_dict

        dict_stores_delayed = delayed(aux_filter_years)(self.dict_stores, keep = keep, drop = drop)
        dict_rms_delayed = delayed(aux_filter_years)(self.dict_rms, keep = keep, drop = drop)
        dict_extra_delayed = delayed(aux_filter_years)(self.dict_extra, keep = keep, drop = drop)
        dict_sales_delayed = delayed(aux_filter_years)(self.dict_sales, keep = keep, drop = drop)

        self.dict_stores = dict_stores_delayed.compute()
        self.dict_rms = dict_rms_delayed.compute()
        self.dict_extra = dict_extra_delayed.compute()
        self.dict_sales = dict_sales_delayed.compute()

        new_years = self.all_years

        if keep:
            new_years = {y for y in new_years if y in keep}
        if drop:
            new_years = {y for y in new_years if y not in drop}

        self.all_years = new_years
        if self.verbose == True:
            print('Years Left: ', self.all_years)
        return


    # Filter Groups & Modules structured Similarly enough to Combine
    def filter_sales(self, keep_groups = None, drop_groups = None,
                     keep_modules = None, drop_modules = None):
        """
        Function: filters sales by group or module before reading in files
        to save space and memory
        Arguments: keep_groups, drop_groups: take lists of product group codes
        keep_modules_drop_modules: take lists of product module codes

        Filter sales: keep certain product groups and/or modules
        Re-runs of this method are cumulative: cannot retrieve dropped categories
        without re-initializing your Retail Reader object
        """

        def aux_filter_sales(orig_dict, func = self.get_group,
                             keep = None, drop = None):
            new_dict = orig_dict
            if keep:
                new_dict = {y: [ f  for f in new_dict[y]
                                if func(f) in keep]
                            for y in new_dict.keys()
                            }
            if drop:
                new_dict = {y: [ f  for f in new_dict[y]
                                if func(f) not in drop]
                            for y in new_dict.keys()
                            }
            return new_dict

        self.dict_sales = delayed(aux_filter_sales)(self.dict_sales,
                                           self.get_group,
                                           keep = keep_groups,
                                           drop = drop_groups).compute()

        self.dict_sales = delayed(aux_filter_sales)(self.dict_sales,
                                           self.get_module,
                                           keep = keep_modules,
                                           drop = drop_modules).compute()

        self.all_groups = {self.get_group(f)
                           for y in self.dict_sales.keys()
                           for f in self.dict_sales[y]
                           }
        self.all_modules = {self.get_module(f)
                           for y in self.dict_sales.keys()
                           for f in self.dict_sales[y]
                           }
        if self.verbose == True:
            print('Groups Left: ', self.all_groups)
            print('Modules Left: ', self.all_modules)

        return


    # Filter years and sales before fee
    def read_rms(self):
        """
        Function: populates self.df_rms

        Read in the RMS versions files
        Some UPCs are reuses of UPCs from previous years
        Nielsen notes the products may have changed sufficiently
        And codes these as a new product "version" in the later years
        Columns: upc, upc_ver_uc, panel_year
        See Nielsen documentation for a full description of these variables.
        """
        
        # Create a list of delayed reads for each CSV file
        delayed_read_csv = [dd.read_csv(file_path, delimiter='\t', dtype=dict_types, assume_missing=True)
                            for file_path in self.dict_rms.values()]
    
        # Concatenate the delayed reads into a single Dask DataFrame
        self.df_rms = dd.concat(delayed_read_csv).compute()

        if self.verbose == True:
            print(f"Successfully Read in the RMS Files for {len(self.dict_rms.values())} years")
        return



    def read_products(self, upc_list=None,
                  keep_groups=None, drop_groups=None,
                  keep_modules=None, drop_modules=None,
                  keep_departments=None, drop_departments=None):
        """
        Function: populates self.df_products
    
        Arguments: 
            Required: RetailReader or PanelReader object
            Optional: keep_groups, drop_groups, keep_modules, drop_modules,
            upc_list
            Each takes a list of group codes, module codes, or upcs
    
        Select the Product file and read it in
        Common to both the Retail Reader and Panel Reader files
        
        Options:
        upc_list: a list of integer UPCs to select, ignores versioning by Nielsen
        keep_groups, drop_groups: selects or drops product group codes
        keep_modules, drop_modules: selects or drops product module codes
        
        Columns: upc, upc_ver_uc, upc_descr, product_module_code, product_module_descr,
        product_group_code, product_group_descr, department_code,
        department_descr, brand_code_uc, brand_descr, multi,
        size1_code_uc, size1_amount, size1_units, dataset_found_uc, 
        size1_change_flag_uc
        See Nielsen documentation for a full description of these variables.
        """
    
        # Call get_products function
        get_products(self, upc_list=upc_list,keep_groups=keep_groups, drop_groups=drop_groups,
                                   keep_modules=keep_modules, drop_modules=drop_modules,
                                   keep_departments=keep_departments, drop_departments=drop_departments)
        
    
        return

    
    
    def read_extra(self, years=None, upc_list=None):
        """
        Function: populates self.df_extra
    
        Select the Extra [characteristics] file and read it in
        Common to both the Retail Reader and Panel Reader files
        Filter Options:
        Sometimes UPCs have repeat entries, but these tend
        to be due to missing data and reporting issues, not changes. Nielsen
        codes product changes as different product versions.
        
        Module and Group selections not possible for the extra files. 
        One option is to select modules and groups in the product data and then
        merge. 
    
        Columns: upc, upc_ver_uc, panel_year, flavor_code, flavor_descr, 
        form_code, form_descr, formula_code, formula_descr, container_code, 
        container_descr, salt_content_code, salt_content_descr, style_code, 
        style_descr, type_code, type_descr, product_code, product_descr, 
        variety_code, variety_descr, organic_claim_code, organic_claim_descr, 
        usda_organic_seal_code, usda_organic_seal_descr, 
        common_consumer_name_code, common_consumer_name_descr, 
        strength_code, strength_descr, scent_code, scent_descr, 
        dosage_code, dosage_descr, gender_code, gender_descr,
        target_skin_condition_code, target_skin_condition_descr, 
        use_code, use_descr, size2_code, size2_amount, size2_units
    
        See Nielsen documentation for a full description of these variables.
        """


        get_extra(self, years=years, upc_list=upc_list)
    
 

        return


    # Read in the Stores File again, common to all groups and modules, so if you are filtering products keep in mind the stores files will be common

    def read_stores(self):
        """
        Function: populates self.df_stores
        Output: self.df_stores will be populated
        Read in stores files, which are common to all groups and modules
        If you are filtering products, note that stores will be common
    
        Columns: store_code_uc, year, parent_code, retailer_code,
        channel_code, store_zip3, fips_state_code, fips_state_descr,
        fips_county_code, fips_county_descr
    
        See Nielsen documentation for a full description of these variables.
        """
        ddf_stores = dd.concat([dd.read_csv(f,  sep='\t', dtype=dict_types) 
                                for f in self.dict_stores.values()])
        
        # dd.read_csv(list(self.dict_stores.values()), sep='\t', dtype=dict_types)
    
        # harmonize the column name for years
        ddf_stores = ddf_stores.rename(columns={'year': 'panel_year'})

    
        # fill blanks with zeroes
        # df_stores = df_stores.fillna(0)
        self.df_stores = ddf_stores

    
        if self.verbose:
            print('Successfully Read in Stores Files')

        return


    # Filter Stores by DMA, States, and Channel
    def filter_stores(self, keep_dmas = None, drop_dmas = None,
                  keep_states = None, drop_states = None,
                  keep_channels = None, drop_channels = None):
        """
        Function: filters self.df_stores based on DMA, state, or channel
        Must have read in df_stores first (cannot be empty)
        Filters stores based on DMA, State, and Channel
        
        See Nielsen documentation for a full description of these variables.
        
        """
    
        # make sure you have read in the stores files first
        if len(self.df_stores) == 0:
            self.read_stores()
        
        if self.verbose == True:
            print('Initial Store Count: ', len(self.df_stores))
        
        df_stores = self.df_stores
        
        
        if keep_dmas:
            df_stores = df_stores[df_stores['dma_code'].isin(keep_dmas)]
        
        if drop_dmas:
            df_stores = df_stores[~df_stores['dma_code'].isin(drop_dmas)]
        
        if keep_channels:
            df_stores = df_stores[df_stores['channel_code'].isin(keep_channels)]
            
        if drop_channels:
            df_stores = df_stores[~df_stores['channel_code'].isin(drop_channels)]

        if keep_states:
            df_stores = df_stores[df_stores['fips_state_descr'].isin(keep_states)]

        if drop_states:
            df_stores = df_stores[~df_stores['fips_state_descr'].isin(drop_states)]

        
        self.df_stores = df_stores.compute().copy()
        
        if self.verbose == True:
            print('Final Store Count: ', len(self.df_stores))
        
        del(df_stores)    
        
        return



    def read_sales(self, incl_promo = True, add_dates=False, agg_function=None, **kwargs):
        """
        Function: populates self.df_sales
        Note the method takes very long!

        Reads in the sales data, post filter if you have applied any
        Uses pyarrow methods to filter and read the data without
        taking up huge amounts of memory. But it still requires a large amount
        of memory and CPU 
        depending on the selected stores, years, groups, and modules
        Columns: store_code_uc, upc, week_end, units, prmult, price, feature,
        display

        See Nielsen documentation for a full description of these variables.        
        """

        # Get the relevant stores
        if len(self.df_stores) == 0:
            self.read_stores()

        if len(self.df_rms) ==0:
            self.read_rms()

        # select columns
        my_cols = ['store_code_uc', 'upc', 'week_end', 'units', 'prmult', 'price']

        if incl_promo == True:
            my_cols = my_cols + ['feature', 'display']

        # for each module-year, clean up the data frame
        # optional: add_dates: calculate the month and quarter        
        def aux_clean(df_tab, add_dates=False):
            """
            Cleans up the data frame for each module-year.
            Optionally adds date-related calculations (month and quarter).
            """
            # Convert 'week_end' to datetime
            df_tab['week_end'] = dd.to_datetime(df_tab['week_end'], format='%Y%m%d')
    
            # Fill null values for 'feature' and 'display'
            if 'feature' in df_tab.columns:
                df_tab['feature'] = df_tab['feature'].fillna(-1).astype('int8')
                df_tab['display'] = df_tab['display'].fillna(-1).astype('int8')

            # Compute unit price, panel year, and revenue
            df_tab['unit_price'] = df_tab['price'] / df_tab['prmult']
            df_tab['panel_year'] = df_tab['week_end'].dt.year.astype('uint16')
            df_tab['revenue'] = df_tab['units'] * df_tab['unit_price']
            

            
            if add_dates:
                unique_week_ends = df_tab['week_end'].drop_duplicates().compute()
                my_dates = pd.DataFrame({'week_end': unique_week_ends})
                my_dates['quarter'] = my_dates['week_end'] + pd.offsets.QuarterEnd(0)
                my_dates['month'] = my_dates['week_end'].astype('datetime64[M]')
                df_tab = dd.merge(df_tab, dd.from_pandas(my_dates, npartitions=df_tab.npartitions), on="week_end", how='left')
    
            return df_tab
    
            # Read one module-year at a time as a pandas table, which will be later concatenated
    
        def aux_read_mod_year(filename, list_stores_df=None, add_dates=False, agg_function=None, **kwargs):

            # Read CSV file into Dask DataFrame
            ddf = dd.read_csv(filename, delimiter='\t', dtype=dict_types, blocksize = '64MB').repartition(npartitions=256)[my_cols]

            # Filter Dask DataFrame if list_stores is provided
            if len(list_stores_df):
            #if list_stores.any() is not None:
                print(type(ddf))
                print(type(list_stores_df))
                ddf = ddf.merge(list_stores_df, on='store_code_uc', how='inner')
                #ddf = ddf[ddf['store_code_uc'].isin(list_stores)]



            # Apply cleaning function and add dates if required
            ddf_cleaned = aux_clean(ddf, add_dates)
        
            # If an aggregation function is provided, apply it
            if agg_function:
                # Convert Dask DataFrame to Dask Array for aggregation
                dask_array = da.from_array(ddf_cleaned.to_dask_array(lengths=True))
                # Apply aggregation function
                result = dask.compute(agg_function(dask_array, **kwargs))[0]
            else:
                result = ddf_cleaned
        
            return result
    
            
        # read all the modules (and groups) for one year
        def aux_read_year(year, add_dates, agg_function=None, **kwargs):

            # get the list of stores that were present in the year of choice
            list_stores = self.df_stores[self.df_stores['panel_year'] == year][['store_code_uc']]
            list_stores_df = list_stores.drop_duplicates()

        
            dask_frames = []
            for f in self.dict_sales[year]:

                ddf = aux_read_mod_year(f, list_stores_df, add_dates, agg_function, **kwargs)

                dask_frames.append(ddf)
            ddf_y = dd.concat(dask_frames)

            return ddf_y
        
        if self.verbose:
            print('Reading Sales')
            tick()


        # This does the work -- keep as Dask DataFrame
        dask_frames = []
        for year in self.dict_sales.keys():
            ddf_year = aux_read_year(year, add_dates, agg_function, **kwargs)

            dask_frames.append(ddf_year)

        self.df_sales = dd.concat(dask_frames)

        if self.verbose:
            print('Finished Sales')
            tock()


        return


############################ Completed #############################################################
