"""
NielsenIQ Retail Scanner Reader — Unified (2006-2020 + 2021 onward)

Reads raw NielsenIQ Retail Scanner files (Kilts File Selection System) for
Batch 1 (2006-2020), Batch 2 (2021+), or both, through one interface.

IMPORTANT — NO SCHEMA IS HARDCODED IN THIS FILE.
Per the Kilts Center / NielsenIQ data-use policy, the file organization and
variable names are proprietary and available only to licensed researchers.
This reader therefore contains processing LOGIC only; every column name,
data type, file name, and folder name is supplied by the user at runtime
through a schema config (see `schema.example.yaml`). Copy that template to
`schema.yaml` and fill it in from YOUR OWN licensed NielsenIQ/Kilts
documentation. Nothing in this repository reveals the data structure.

Usage:
    reader = NielsenIQRetail(
        dir_read='/path/to/2006-2020/RMS',           # Batch 1 (optional)
        dir_read_v2='/path/to/2021-Onward/RMS',       # Batch 2 (optional)
        schema='schema.yaml',                         # your filled-in schema
    )
    reader.filter_years(keep=[2018, 2023])
    reader.read_stores()
    reader.read_sales()                               # lazy Dask DataFrame
"""

import os
import time
import pathlib as path

import pandas as pd
import dask.dataframe as dd


# ───────────────────────────────────────────────────────────────────────────
# Timer
# ───────────────────────────────────────────────────────────────────────────
_start_time = time.time()
def tick():
    global _start_time
    _start_time = time.time()
def tock():
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec, 60)
    (t_hour, t_min) = divmod(t_min, 60)
    print('Time passed: {}hour:{}min:{}sec'.format(t_hour, t_min, t_sec))


# ───────────────────────────────────────────────────────────────────────────
# Schema loading  (the schema carries ALL proprietary names/types/layout)
# ───────────────────────────────────────────────────────────────────────────
# Maps the dtype TOKENS used in the config to pandas dtypes. Tokens are generic
# (they reveal nothing about the data); the column names they apply to live in
# the user-supplied config, never here.
_DTYPE_TOKENS = {
    'object': 'object', 'string': 'string',
    'float32': 'float32', 'float64': 'float64',
    'Int8': pd.Int8Dtype(),   'Int16': pd.Int16Dtype(),
    'Int32': pd.Int32Dtype(), 'Int64': pd.Int64Dtype(),
    'UInt8': pd.UInt8Dtype(),   'UInt16': pd.UInt16Dtype(),
    'UInt32': pd.UInt32Dtype(), 'UInt64': pd.UInt64Dtype(),
    'bool': 'bool',
}

_REQUIRED_KEYS = ('columns', 'dtypes', 'files')


def _resolve_dtypes(d):
    """Turn a {column: token} map into a {column: pandas dtype} map."""
    if not d:
        return {}
    return {col: _DTYPE_TOKENS.get(tok, tok) for col, tok in d.items()}


def load_schema(source):
    """
    Load the schema config.

    `source` may be:
      - a dict already in memory, or
      - a path to a YAML file, or
      - None -> look at env var NIELSEN_SCHEMA, else 'schema.yaml' in cwd.

    The schema must define the top-level keys: columns, dtypes, files
    (see schema.example.yaml). This function does NOT contain any NielsenIQ
    names — it only reads what the licensed user provides.
    """
    if isinstance(source, dict):
        cfg = source
    else:
        if source is None:
            source = os.environ.get('NIELSEN_SCHEMA', 'schema.yaml')
        p = path.Path(source)
        if not p.exists():
            raise FileNotFoundError(
                f"Schema config not found at '{source}'. Copy schema.example.yaml "
                f"to schema.yaml and fill it in from your own licensed NielsenIQ/"
                f"Kilts documentation, then pass schema='schema.yaml'.")
        try:
            import yaml
        except ImportError as e:
            raise ImportError(
                "Reading a YAML schema requires PyYAML. Install it with "
                "`pip install pyyaml`, or pass the schema as a Python dict.") from e
        with open(p, 'r') as fh:
            cfg = yaml.safe_load(fh)

    missing = [k for k in _REQUIRED_KEYS if k not in (cfg or {})]
    if missing:
        raise ValueError(
            f"Schema is missing required section(s): {missing}. "
            f"See schema.example.yaml for the expected structure.")
    return cfg


def write_schema_template(dest='.'):
    """
    Copy the blank schema template next to this package out to `dest` as
    `schema.yaml`, for you to fill in from your own licensed NielsenIQ docs.
    """
    import shutil
    src = path.Path(__file__).with_name('schema.example.yaml')
    out = path.Path(dest) / 'schema.yaml'
    shutil.copyfile(src, out)
    print(f"Wrote {out} — fill it in from your licensed NielsenIQ documentation, "
          f"then pass schema='{out}'.")
    return str(out)


# ═══════════════════════════════════════════════════════════════════════════
# Unified reader
# ═══════════════════════════════════════════════════════════════════════════
class NielsenIQRetail(object):
    """
    Unified reader for NielsenIQ Retail Scanner data (Batch 1 and/or Batch 2).

    Args:
        dir_read:    Path to Batch 1 RMS root, OR (if dir_read_v2 is None) a
                     single root that is auto-detected as Batch 1 or Batch 2.
        dir_read_v2: (Optional) Path to Batch 2 RMS root, when using both.
        schema:      Path to your filled-in schema YAML, a schema dict, or None
                     (then env NIELSEN_SCHEMA or ./schema.yaml). REQUIRED to
                     read anything — it carries the file layout and column names
                     from your licensed subscription.
        verbose:     Print progress (default True).

    df_sales is always a lazy Dask DataFrame — filter/aggregate before compute.
    """

    def __init__(self, dir_read=None, dir_read_v2=None, schema=None, verbose=True):
        self.verbose = verbose
        self.has_batch1 = False
        self.has_batch2 = False

        # --- schema (all proprietary names/types/layout come from here) ------
        self.schema = load_schema(schema)
        self.COL   = self.schema['columns']            # logical role -> real column name
        self.FILES = self.schema['files']              # file/folder organization
        self._dt   = self.schema['dtypes']             # raw {col: token} maps
        self._attr = self.schema.get('attributes', {}) # B2 attribute variable names

        # --- locate batches --------------------------------------------------
        b1 = self.FILES.get('batch1', {})
        b2 = self.FILES.get('batch2', {})

        if dir_read is not None and dir_read_v2 is not None:
            self._init_batch1(path.Path(dir_read))
            self._init_batch2(path.Path(dir_read_v2))
        elif dir_read is not None:
            d = path.Path(dir_read)
            looks_b1 = any(d.glob(f"**/{b1.get('product_parent','')}/{b1.get('product_file','')}")) \
                       if b1.get('product_file') else False
            looks_b2 = any(d.glob(f"**/{b2.get('productdesc_filename','')}")) \
                       if b2.get('productdesc_filename') else False
            if looks_b1 and looks_b2:
                self._init_batch1(d); self._init_batch2(d)
            elif looks_b1:
                self._init_batch1(d)
            elif looks_b2:
                self._init_batch2(d)
            else:
                raise Exception(
                    f"Could not detect Batch 1 or Batch 2 data in {d} using the "
                    f"file markers in your schema. Check the 'files' section.")
        else:
            raise Exception("Provide dir_read (and optionally dir_read_v2).")

        self.all_years = set()
        if self.has_batch1: self.all_years |= self._b1_years
        if self.has_batch2: self.all_years |= self._b2_years

        # outputs
        self.df_products = pd.DataFrame()
        self.df_products_b2 = pd.DataFrame()
        self.df_hierarchy = pd.DataFrame()
        self.df_stores = dd.from_pandas(pd.DataFrame(), npartitions=1)
        self.df_sales = dd.from_pandas(pd.DataFrame(), npartitions=1)
        self.df_rms = pd.DataFrame()
        self.df_extra = pd.DataFrame()
        self.df_attributes = pd.DataFrame()

        if self.verbose:
            print('NielsenIQRetail initialized.')
            parts = []
            if self.has_batch1: parts.append(f'Batch 1 (years: {sorted(self._b1_years)})')
            if self.has_batch2: parts.append(f'Batch 2 (years: {sorted(self._b2_years)})')
            print('  Batches:', ', '.join(parts))
            print('  All years:', sorted(self.all_years))
            if self.has_batch1:
                print(f'  B1 groups: {len(self._b1_all_groups)}, '
                      f'modules: {len(self._b1_all_modules)}, '
                      f'movement files: {len(self._b1_files_sales)}')
            if self.has_batch2:
                print(f'  B2 groupings: {len(self._b2_all_kfss)}, '
                      f'super categories: {len(self._b2_all_sc)}, '
                      f'movement files: {len(self._b2_files_sales)}')

    # ── helpers: dtype maps resolved from the schema ────────────────────────
    def _dtypes(self, key):
        return _resolve_dtypes(self._dt.get(key, {}))

    def _attr_dtypes(self):
        """Build the B2 attributes dtype map from user-supplied attribute names."""
        names  = self._attr.get('names', [])
        suffix = self._attr.get('code_suffix', '_cd')
        upc, yr = self.COL['upc'], self.COL.get('year_in', 'year')
        d = {upc: 'object', yr: pd.UInt16Dtype()}
        for n in names:
            d[n] = 'object'
            d[f"{n}{suffix}"] = pd.Int64Dtype()
        return d

    # ── generic file discovery (folder/file names come from the schema) ─────
    @staticmethod
    def _get_files(dir_read):
        files = [f for f in path.Path(dir_read).glob('**/*.*sv') if '._' not in f.stem]
        if not files:
            raise Exception(f'Found no files in {dir_read}!')
        return files

    @staticmethod
    def _stem_token(stem, which):
        parts = stem.split('_')
        return parts[0] if which == 'first' else parts[-1]

    def _year_b1(self, f):
        try:
            return int(self._stem_token(f.stem, 'last'))
        except ValueError:
            return None

    def _module_b1(self, f):
        return int(self._stem_token(f.stem, 'first'))

    def _group_b1(self, f):
        return int(self._stem_token(f.parent.stem, 'first'))

    @staticmethod
    def _year_b2(f):
        for part in f.parts:
            if len(part) == 4 and part.isdigit() and 2020 <= int(part) <= 2099:
                return int(part)
        return None

    @staticmethod
    def _kfss(f):
        return f.parent.name

    @staticmethod
    def _super_category(f):
        return f.stem

    # ══════════════════════════════════════════════════════════════════════
    # BATCH INITIALISATION
    # ══════════════════════════════════════════════════════════════════════
    def _init_batch1(self, dir_read):
        self.has_batch1 = True
        self._b1_dir = dir_read
        b1 = self.FILES['batch1']
        files = self._get_files(dir_read)

        self._b1_files_product = [
            f for f in files
            if f.name == b1['product_file']
            and f.parent.name == b1['product_parent']
            and f.parent.parent.name == b1['product_grandparent']
        ]
        annual = [f for f in files if b1['annual_marker'] in f.parts]
        self._b1_files_sales = [f for f in files if b1['movement_marker'] in f.parts]
        if not self._b1_files_sales:
            raise Exception('Batch 1: Could not find movement files (check schema markers).')

        self._b1_all_groups  = {self._group_b1(f)  for f in self._b1_files_sales}
        self._b1_all_modules = {self._module_b1(f) for f in self._b1_files_sales}
        self._b1_years = {self._year_b1(f) for f in self._b1_files_sales if self._year_b1(f)}

        self._b1_files_stores = [f for f in annual if b1['stores_token'] in f.name]
        self._b1_files_rms    = [f for f in annual if b1['rms_token'] in f.name]
        self._b1_files_extra  = [f for f in annual if b1['extra_token'] in f.name]

        self._b1_dict_stores = {self._year_b1(f): f for f in self._b1_files_stores if self._year_b1(f)}
        self._b1_dict_rms    = {self._year_b1(f): f for f in self._b1_files_rms if self._year_b1(f)}
        self._b1_dict_extra  = {self._year_b1(f): f for f in self._b1_files_extra if self._year_b1(f)}
        self._b1_dict_sales  = {y: [f for f in self._b1_files_sales if self._year_b1(f) == y]
                                for y in self._b1_years}

    def _init_batch2(self, dir_read):
        self.has_batch2 = True
        self._b2_dir = dir_read
        b2 = self.FILES['batch2']
        files = self._get_files(dir_read)

        annual = [f for f in files if b2['annual_marker'] in f.parts]
        self._b2_files_sales = [f for f in files if b2['movement_marker'] in f.parts]
        if not self._b2_files_sales:
            raise Exception('Batch 2: Could not find movement files (check schema markers).')

        self._b2_files_stores      = [f for f in annual if f.name == b2['stores_filename']]
        self._b2_files_productdesc = [f for f in annual if f.name == b2['productdesc_filename']]
        self._b2_files_hierarchy   = [f for f in annual if f.name == b2['hierarchy_filename']]
        self._b2_files_attributes  = [f for f in annual if f.name == b2['attributes_filename']]

        self._b2_all_kfss = {self._kfss(f) for f in self._b2_files_sales}
        self._b2_all_sc   = {self._super_category(f) for f in self._b2_files_sales}
        self._b2_years = {self._year_b2(f) for f in self._b2_files_sales if self._year_b2(f)}

        self._b2_dict_stores      = {self._year_b2(f): f for f in self._b2_files_stores if self._year_b2(f)}
        self._b2_dict_productdesc = {self._year_b2(f): f for f in self._b2_files_productdesc if self._year_b2(f)}
        self._b2_dict_hierarchy   = {self._year_b2(f): f for f in self._b2_files_hierarchy if self._year_b2(f)}
        self._b2_dict_attributes  = {self._year_b2(f): f for f in self._b2_files_attributes if self._year_b2(f)}
        self._b2_dict_sales       = {y: [f for f in self._b2_files_sales if self._year_b2(f) == y]
                                     for y in self._b2_years}

    # ══════════════════════════════════════════════════════════════════════
    # FILTER METHODS
    # ══════════════════════════════════════════════════════════════════════
    def filter_years(self, keep=None, drop=None):
        def _filt(d):
            if keep: d = {y: f for y, f in d.items() if y in keep}
            if drop: d = {y: f for y, f in d.items() if y not in drop}
            return d
        if self.has_batch1:
            self._b1_dict_stores = _filt(self._b1_dict_stores)
            self._b1_dict_rms    = _filt(self._b1_dict_rms)
            self._b1_dict_extra  = _filt(self._b1_dict_extra)
            self._b1_dict_sales  = _filt(self._b1_dict_sales)
            self._b1_years = set(self._b1_dict_sales.keys())
        if self.has_batch2:
            self._b2_dict_stores      = _filt(self._b2_dict_stores)
            self._b2_dict_productdesc = _filt(self._b2_dict_productdesc)
            self._b2_dict_hierarchy   = _filt(self._b2_dict_hierarchy)
            self._b2_dict_attributes  = _filt(self._b2_dict_attributes)
            self._b2_dict_sales       = _filt(self._b2_dict_sales)
            self._b2_years = set(self._b2_dict_sales.keys())
        self.all_years = set()
        if self.has_batch1: self.all_years |= self._b1_years
        if self.has_batch2: self.all_years |= self._b2_years
        if self.verbose: print('Years Left:', sorted(self.all_years))

    def filter_sales(self, keep_groups=None, drop_groups=None,
                     keep_modules=None, drop_modules=None,
                     keep_kfss=None, drop_kfss=None,
                     keep_super_categories=None, drop_super_categories=None):
        def _ff(d, func, keep, drop):
            if keep: d = {y: [f for f in d[y] if func(f) in keep] for y in d}
            if drop: d = {y: [f for f in d[y] if func(f) not in drop] for y in d}
            return d
        if self.has_batch1:
            if keep_groups or drop_groups:
                self._b1_dict_sales = _ff(self._b1_dict_sales, self._group_b1, keep_groups, drop_groups)
            if keep_modules or drop_modules:
                self._b1_dict_sales = _ff(self._b1_dict_sales, self._module_b1, keep_modules, drop_modules)
            self._b1_all_groups  = {self._group_b1(f)  for y in self._b1_dict_sales for f in self._b1_dict_sales[y]}
            self._b1_all_modules = {self._module_b1(f) for y in self._b1_dict_sales for f in self._b1_dict_sales[y]}
            if self.verbose and (keep_groups or drop_groups or keep_modules or drop_modules):
                print(f'B1 — Groups: {self._b1_all_groups}, Modules: {self._b1_all_modules}')
        if self.has_batch2:
            if keep_kfss or drop_kfss:
                self._b2_dict_sales = _ff(self._b2_dict_sales, self._kfss, keep_kfss, drop_kfss)
            if keep_super_categories or drop_super_categories:
                self._b2_dict_sales = _ff(self._b2_dict_sales, self._super_category,
                                          keep_super_categories, drop_super_categories)
            self._b2_all_kfss = {self._kfss(f) for y in self._b2_dict_sales for f in self._b2_dict_sales[y]}
            self._b2_all_sc   = {self._super_category(f) for y in self._b2_dict_sales for f in self._b2_dict_sales[y]}
            if self.verbose and (keep_kfss or drop_kfss or keep_super_categories or drop_super_categories):
                print(f'B2 — Groupings: {self._b2_all_kfss}, Super Categories: {self._b2_all_sc}')

    # ══════════════════════════════════════════════════════════════════════
    # READ METHODS
    # ══════════════════════════════════════════════════════════════════════
    def read_stores(self):
        yr, py = self.COL.get('year_in', 'year'), self.COL.get('year_out', 'year_out')
        frames = []
        if self.has_batch1 and self._b1_dict_stores:
            d1 = dd.concat([dd.read_csv(f, sep='\t', dtype=self._dtypes('batch1'), assume_missing=True)
                            for f in self._b1_dict_stores.values()]).rename(columns={yr: py})
            frames.append(d1)
        if self.has_batch2 and self._b2_dict_stores:
            d2 = dd.concat([dd.read_csv(f, sep='\t', dtype=self._dtypes('batch2_stores'), assume_missing=True)
                            for f in self._b2_dict_stores.values()]).rename(columns={yr: py})
            frames.append(d2)
        if not frames:
            raise Exception('No stores files found.')
        self.df_stores = dd.concat(frames) if len(frames) > 1 else frames[0]
        if self.verbose: print('Successfully read stores files')

    def filter_stores(self, keep_dmas=None, drop_dmas=None,
                      keep_states=None, drop_states=None,
                      keep_channels=None, drop_channels=None):
        if len(self.df_stores) == 0:
            self.read_stores()
        df = self.df_stores
        dma, ch, st = self.COL.get('dma'), self.COL.get('channel'), self.COL.get('state')
        if keep_dmas:     df = df[df[dma].isin(keep_dmas)]
        if drop_dmas:     df = df[~df[dma].isin(drop_dmas)]
        if keep_channels: df = df[df[ch].isin(keep_channels)]
        if drop_channels: df = df[~df[ch].isin(drop_channels)]
        if keep_states:   df = df[df[st].isin(keep_states)]
        if drop_states:   df = df[~df[st].isin(drop_states)]
        self.df_stores = df.compute().copy()
        if self.verbose: print(f'Store count after filter: {len(self.df_stores)}')

    def read_products(self, upc_list=None,
                      keep_groups=None, drop_groups=None,
                      keep_modules=None, drop_modules=None,
                      keep_departments=None, drop_departments=None,
                      keep_super_categories=None, drop_super_categories=None,
                      keep_categories=None, drop_categories=None):
        upc = self.COL['upc']
        # --- Batch 1 products master ---
        if self.has_batch1 and self._b1_files_product:
            ddf = dd.read_csv(self._b1_files_product[0], delimiter='\t', encoding='latin',
                              dtype=self._dtypes('batch1_products'), assume_missing=True)
            gcol, mcol, dcol = self.COL.get('group'), self.COL.get('module'), self.COL.get('department')
            if keep_groups:      ddf = ddf[ddf[gcol].isin(keep_groups)]
            if drop_groups:      ddf = ddf[~ddf[gcol].isin(drop_groups)]
            if keep_modules:     ddf = ddf[ddf[mcol].isin(keep_modules)]
            if drop_modules:     ddf = ddf[~ddf[mcol].isin(drop_modules)]
            if keep_departments: ddf = ddf[ddf[dcol].isin(keep_departments)]
            if drop_departments: ddf = ddf[~ddf[dcol].isin(drop_departments)]
            if upc_list:         ddf = ddf[ddf[upc].isin(upc_list)]
            self.df_products = ddf.compute().copy()
            if self.verbose: print(f'B1 products shape: {self.df_products.shape}')
        # --- Batch 2 product description ---
        if self.has_batch2 and self._b2_dict_productdesc:
            ddf = dd.concat([dd.read_csv(f, delimiter='\t', dtype=self._dtypes('batch2_productdesc'),
                                         assume_missing=True) for f in self._b2_dict_productdesc.values()])
            if upc_list: ddf = ddf[ddf[upc].isin(upc_list)]
            if any([keep_super_categories, drop_super_categories, keep_categories, drop_categories]):
                if len(self.df_hierarchy) == 0:
                    raise Exception('B2 hierarchy filters require read_hierarchy() first.')
                sc, cat = self.COL.get('super_category'), self.COL.get('category')
                h = self.df_hierarchy
                if keep_super_categories: h = h[h[sc].isin(keep_super_categories)]
                if drop_super_categories: h = h[~h[sc].isin(drop_super_categories)]
                if keep_categories:       h = h[h[cat].isin(keep_categories)]
                if drop_categories:       h = h[~h[cat].isin(drop_categories)]
                ddf = ddf[ddf[upc].isin(h[upc].unique())]
            self.df_products_b2 = ddf.compute().copy()
            if self.verbose: print(f'B2 products shape: {self.df_products_b2.shape}')

    def read_rms(self):
        if not self.has_batch1 or not self._b1_dict_rms:
            print('No RMS files (Batch 1 only).'); return
        self.df_rms = dd.concat([dd.read_csv(f, delimiter='\t', dtype=self._dtypes('batch1'),
                                             assume_missing=True) for f in self._b1_dict_rms.values()]).compute()
        if self.verbose: print(f'Read RMS for {len(self._b1_dict_rms)} years')

    def read_extra(self, years=None, upc_list=None):
        if not self.has_batch1:
            print('read_extra is Batch 1 only; use read_attributes for Batch 2.'); return
        if years is None: years = self._b1_years
        files_in = [f for f in self._b1_files_extra if self._year_b1(f) in years]
        ddf = dd.concat([dd.read_csv(f, delimiter='\t', dtype=self._dtypes('batch1'),
                                     assume_missing=True) for f in files_in])
        if upc_list: ddf = ddf[ddf[self.COL['upc']].isin(upc_list)]
        self.df_extra = ddf.compute().copy()
        if self.verbose: print(f'B1 extra shape: {self.df_extra.shape}')

    def read_hierarchy(self, years=None, upc_list=None,
                       keep_departments=None, drop_departments=None,
                       keep_super_categories=None, drop_super_categories=None,
                       keep_categories=None, drop_categories=None,
                       keep_segments=None, drop_segments=None,
                       keep_sub_categories=None, drop_sub_categories=None):
        if not self.has_batch2:
            print('Product hierarchy is Batch 2 only (2021+).'); return
        if years is None: years = self._b2_years
        files_in = [f for f in self._b2_files_hierarchy if self._year_b2(f) in years]
        if not files_in:
            raise Exception('No hierarchy files found (check schema).')
        ddf = dd.concat([dd.read_csv(f, delimiter='\t', dtype=self._dtypes('batch2_hierarchy'),
                                     assume_missing=False) for f in files_in])
        c = self.COL
        dep, sc, cat = c.get('h_department'), c.get('super_category'), c.get('category')
        seg, sub, upc = c.get('segment'), c.get('sub_category'), c['upc']
        if keep_departments:      ddf = ddf[ddf[dep].isin(keep_departments)]
        if drop_departments:      ddf = ddf[~ddf[dep].isin(drop_departments)]
        if keep_super_categories: ddf = ddf[ddf[sc].isin(keep_super_categories)]
        if drop_super_categories: ddf = ddf[~ddf[sc].isin(drop_super_categories)]
        if keep_categories:       ddf = ddf[ddf[cat].isin(keep_categories)]
        if drop_categories:       ddf = ddf[~ddf[cat].isin(drop_categories)]
        if keep_segments:         ddf = ddf[ddf[seg].isin(keep_segments)]
        if drop_segments:         ddf = ddf[~ddf[seg].isin(drop_segments)]
        if keep_sub_categories:   ddf = ddf[ddf[sub].isin(keep_sub_categories)]
        if drop_sub_categories:   ddf = ddf[~ddf[sub].isin(drop_sub_categories)]
        if upc_list:              ddf = ddf[ddf[upc].isin(upc_list)]
        self.df_hierarchy = ddf.compute()
        if self.verbose:
            print(f'Hierarchy shape: {self.df_hierarchy.shape}')

    def read_attributes(self, years=None, upc_list=None):
        if not self.has_batch2:
            print('read_attributes is Batch 2 only; use read_extra for Batch 1.'); return
        if years is None: years = self._b2_years
        files_in = [f for f in self._b2_files_attributes if self._year_b2(f) in years]
        if not files_in:
            raise Exception('No attributes files found (check schema).')
        ddf = dd.concat([dd.read_csv(f, delimiter='\t', dtype=self._attr_dtypes(),
                                     assume_missing=True) for f in files_in])
        if upc_list: ddf = ddf[ddf[self.COL['upc']].isin(upc_list)]
        self.df_attributes = ddf.compute().copy()
        if self.verbose: print(f'Attributes shape: {self.df_attributes.shape}')

    # ══════════════════════════════════════════════════════════════════════
    # READ SALES — unified, lazy
    # ══════════════════════════════════════════════════════════════════════
    def read_sales(self, incl_promo=True):
        c = self.COL
        my_cols = [c['store_id'], c['upc'], c['week'], c['units'], c['price_multiplier'], c['price']]
        if incl_promo:
            my_cols += [c['feature'], c['display']]
        upc, wk = c['upc'], c['week']

        if self.verbose:
            print('Building unified sales graph...'); tick()
        frames = []
        if self.has_batch1:
            b1_files = [str(f) for y in sorted(self._b1_dict_sales) for f in self._b1_dict_sales[y]]
            if b1_files:
                d1 = dd.read_csv(b1_files, delimiter='\t', dtype=self._dtypes('batch1'),
                                 blocksize='128MB', assume_missing=True)[my_cols]
                d1[upc] = d1[upc].astype(str)
                d1[wk]  = d1[wk].astype(str)
                d1['batch'] = 1
                frames.append(d1)
                if self.verbose: print(f'  B1: {len(b1_files)} files, {d1.npartitions} partitions')
        if self.has_batch2:
            b2_files = [str(f) for y in sorted(self._b2_dict_sales) for f in self._b2_dict_sales[y]]
            if b2_files:
                d2 = dd.read_csv(b2_files, delimiter='\t', dtype=self._dtypes('batch2_movement'),
                                 blocksize='128MB', assume_missing=True)[my_cols]
                d2['batch'] = 2
                frames.append(d2)
                if self.verbose: print(f'  B2: {len(b2_files)} files, {d2.npartitions} partitions')
        if not frames:
            raise Exception('No movement files found after filtering.')
        self.df_sales = dd.concat(frames)
        if self.verbose:
            print(f'  Total partitions: {self.df_sales.npartitions}'); tock()
            print('df_sales is a lazy Dask DataFrame — filter/aggregate before .compute().')

    # ══════════════════════════════════════════════════════════════════════
    # CONVENIENCE
    # ══════════════════════════════════════════════════════════════════════
    def compute_revenue(self):
        """New Dask frame with unit_price, year, revenue added (derived columns)."""
        c = self.COL
        ddf = self.df_sales.copy()
        ddf['week_end_dt'] = dd.to_datetime(ddf[c['week']], format='%Y%m%d', errors='coerce')
        ddf['unit_price']  = ddf[c['price']] / ddf[c['price_multiplier']]
        ddf['year']        = ddf['week_end_dt'].dt.year.astype('uint16')
        ddf['revenue']     = ddf[c['units']] * ddf['unit_price']
        return ddf

    def info(self):
        print('All years:', sorted(self.all_years))
        if self.has_batch1:
            print('\nBatch 1:', self._b1_dir)
            print(f'  Years: {sorted(self._b1_years)} | groups: {len(self._b1_all_groups)} | '
                  f'modules: {len(self._b1_all_modules)} | '
                  f'movement files: {sum(len(v) for v in self._b1_dict_sales.values())}')
        if self.has_batch2:
            print('\nBatch 2:', self._b2_dir)
            print(f'  Years: {sorted(self._b2_years)} | groupings: {len(self._b2_all_kfss)} | '
                  f'super categories: {len(self._b2_all_sc)} | '
                  f'movement files: {sum(len(v) for v in self._b2_dict_sales.values())}')
