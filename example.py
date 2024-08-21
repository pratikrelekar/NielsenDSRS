from NielsenDSRS import NielsenIQRetail

# File path to RMS folder based on user's file structure. The base directory may vary depending on your setup.
# Ensure that the Nielsen/nielsen_extracts/RMS path is correctly followed within your directory hierarchy.
file_path = 'YOUR_FILE_PATH'

# Filters
KEEP_GROUPS = [] #Group code
KEEP_MODULES = [] #Module code
DROP_YEARS = []
KEEP_YEARS = [2017, 2018, 2019] #Select year
KEEP_STATES = [] #Select ANSI state code
KEEP_CHANNELS = [] #Select category (Supermarkets/Pharmacies)

# Specify which dmas /modules/columns to keep
dmas = #DMA code

# supermarkets stores only (no superstores/pharmacies/etc)
channels = []

NR = NielsenIQRetail(file_path)
NR.filter_years(keep=KEEP_YEARS)

# Read all Stores files for selected filters above. The output is pandas dataframe.
NR.read_stores()
# filter_stores takes keep_dmas, keep_states, and keep_channels as arguments.
NR.filter_stores()
NR.df_stores.head()

# Read all Product files for selected filters above. The output is pandas dataframe.
# read_products takes keep_modules, keep_groups, and keep_departments as arguments.
NR.read_products()
NR.df_products.head()

# Read all RMS files for selected filters above. The output is pandas dataframe.
NR.read_rms()
NR.df_rms.head()

# Filtering sales data based on selected filters. 
NR.filter_sales(keep_groups = KEEP_GROUPS, keep_modules = KEEP_MODULES)
NR.read_sales()
# Sales data is assigned to df_sales attribute of the class. The output is dask dataframe. 
# If you want to convert it to pandas dataframe, you can use .compute() function.
NR.df_sales.head()

