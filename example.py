

file_path = path.Path('FILE_PATH')

KEEP_GROUPS = [] #Group code
KEEP_MODULES = [] #Module code
DROP_YEARS = []
KEEP_YEARS = [] #Select year
KEEP_STATES = [] #Select ANSI state code
KEEP_CHANNELS = [] #Select category (Supermarkets/Pharmacies)

# Specify which dmas /modules/columns to keep
dmas = #DMA code
# cereal = 1344

# supermarkets stores only (no superstores/pharmacies/etc)
channels = []

NR = NielsenRetail(file_path)
# Test out all the potential filters:


NR.filter_years(keep=KEEP_YEARS)



# Read all Product files for selected years above
NR.read_products()
NR.df_products.head()

# Read all RMS files for selected states,years,channels above
NR.read_rms()
NR.df_rms.head()

# Read all Stores files for selected states,years,channels above
NR.read_stores()
NR.filter_stores()
NR.df_stores.head()
