"""
Example usage of NielsenIQRetail (v1.0.0).

Reads the NielsenIQ Retail Scanner data for the 2006-2020 and/or 2021-onward
formats through one interface.

Before running, create and fill your schema:

    import NielsenDSRS
    NielsenDSRS.write_schema_template()   # writes a blank schema.yaml here
    # then fill schema.yaml from your OWN licensed NielsenIQ documentation
"""

from NielsenDSRS import NielsenIQRetail

# --- paths to your RMS roots (edit for your setup) ---------------------------
DIR_B1 = '/path/to/2006-2020/nielsen_extracts/RMS'      # 2006-2020  (optional)
DIR_B2 = '/path/to/2021-Onward/nielsen_extracts/RMS'    # 2021-onward (optional)
SCHEMA = 'schema.yaml'                                   # your filled-in schema

# --- selections (edit as needed; leave a list empty to not filter on it) -----
KEEP_YEARS            = [2018, 2023]
KEEP_GROUPS           = []        # Batch 1 product group codes
KEEP_MODULES          = []        # product module codes
KEEP_SUPER_CATEGORIES = []        # Batch 2 super categories
KEEP_DMAS             = []        # DMA codes
KEEP_STATES           = []        # state codes / abbreviations
KEEP_CHANNELS         = []        # store channels

# --- build the reader --------------------------------------------------------
# For a single era, pass only dir_read (the format is auto-detected).
reader = NielsenIQRetail(dir_read=DIR_B1, dir_read_v2=DIR_B2, schema=SCHEMA)

# choose years across both eras
reader.filter_years(keep=KEEP_YEARS)

# --- stores (pandas) ---------------------------------------------------------
reader.read_stores()
reader.filter_stores(keep_dmas=KEEP_DMAS or None,
                     keep_states=KEEP_STATES or None,
                     keep_channels=KEEP_CHANNELS or None)
print(reader.df_stores.head())

# --- products: Batch 1 -> df_products, Batch 2 -> df_products_b2 -------------
reader.read_products(keep_groups=KEEP_GROUPS or None,
                     keep_modules=KEEP_MODULES or None)
print(reader.df_products.head())

# --- 2021-onward product hierarchy and attributes (Batch 2 only) ------------
reader.read_hierarchy()
reader.read_attributes()

# --- RMS versions (Batch 1 only) --------------------------------------------
reader.read_rms()

# --- sales: filter first, then read. df_sales is a lazy Dask DataFrame. -----
reader.filter_sales(keep_groups=KEEP_GROUPS or None,
                    keep_modules=KEEP_MODULES or None,
                    keep_super_categories=KEEP_SUPER_CATEGORIES or None)
reader.read_sales()
print(reader.df_sales.head())

# df_sales spans both eras and carries a 'batch' column (1 = 2006-2020, 2 = 2021+).
# Always filter/aggregate before .compute(). Use YOUR column names from schema.yaml, e.g.:
#   reader.df_sales.groupby('<upc column>').agg({'<units column>': 'sum'}).compute()
