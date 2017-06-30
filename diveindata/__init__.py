import pandas as pd

from collections import defaultdict

class DataInfo(object):

    def __init__(self, data_source, find_eqv=True, params={}):
        '''Load the data from data_source'''
        self.data_source = data_source
        self.data_source_type = 'csv'
        self.data = pd.read_csv(self.data_source, **params)
        self.num_entries = self.data.shape[0]
        self.num_columns = self.data.shape[1]

        self.count_uniques()
        self.group_col_by_uniques()
        self.infer_column_types()
        if find_eqv:
            self.find_equivalent_columns()
        self.calc_numerical_stats()

    def count_uniques(self):
        '''Count the number of uniques values in each column'''
        self.columns = {}

        for col in self.data.columns:
            self.columns[col] = {}
            self.columns[col]['uniques'] = len(self.data[col].unique())

    def group_col_by_uniques(self):
        '''Group columns with same uniques'''
        self.uniq_groups = defaultdict(list)

        for col, col_info in self.columns.items():
            col_uniques = col_info['uniques']
            self.uniq_groups[col_uniques] += [col]

    def infer_column_types(self):
        '''Infer types of all columns'''

        for col in self.data.columns:
            column = self.data[col]
            distinct_count = column.nunique(dropna=False)

            if distinct_count <= 1:
                self.columns[col]['type'] = 'CONST'
            elif distinct_count == self.num_entries:
                self.columns[col]['type'] = 'UNIQUE'
            elif pd.api.types.is_numeric_dtype(column):
                if pd.api.types.is_float_dtype(column):
                    self.columns[col]['type'] = 'FLOAT'
                else:
                    self.columns[col]['type'] = 'INT'
            elif pd.api.types.is_datetime64_dtype(column):
                self.columns[col]['type'] = 'DATE'
            else:
                self.columns[col]['type'] = 'CATEGORY'

    def find_equivalent_columns(self):
        '''Find columns with correlation one'''
        pairs = []

        def add_equivalent_info(col1, col2):
            self.columns[col1]['has_equivalent'] = True
            if not 'equivalents' in self.columns[col1]:
                self.columns[col1]['equivalents'] = []
            self.columns[col1]['equivalents'] += [col2]

        for num, cols in self.uniq_groups.items():
            for ci, c in enumerate(cols):
                for c2 in cols[ci+1:]:
                    grouped_pair = self.data.groupby([c,  c2])
                    if len(grouped_pair.indices.keys()) == self.columns[c]['uniques']:
                        pairs += [[c, c2]]
                        add_equivalent_info(c, c2)
                        add_equivalent_info(c2, c)

    def columns_by_type(self, ctype, exclude_equiv=False):
        if not exclude_equiv:
            return [col for col in self.data.columns if self.columns[col]['type'] == ctype]
        else:
            return [col for col in self.data.columns if self.columns[col]['type'] == ctype
                                        and 'has_equivalent' in self.columns[col]
                                        and self.columns[col]['has_equivalent']]

    def calc_numerical_stats(self):
        for col in (self.columns_by_type('INT') + self.columns_by_type('FLOAT')):
            self.columns[col]['min'] = self.data[col].min()
            self.columns[col]['idxmin'] = self.data[col].min()
            self.columns[col]['max'] = self.data[col].max()
            self.columns[col]['sum'] = self.data[col].sum()
            self.columns[col]['mean'] = self.data[col].mean()
            self.columns[col]['std'] = self.data[col].std()

    def bar_groups(self, categories=[], num_col=None, aggs=['sum'], level=0):

        def group_data(data, category):
            grouped_data = data.groupby(category)
            if num_col:
                return grouped_data[num_col].agg(aggs).sort_values(by=aggs[0], na_position='first')
            else:
                return grouped_data.size().sort_values(na_position='first')

        if len(categories) == 1:
            if type(categories[0][1]) is list:
                query_expr = ' | '.join(['{} == "{}"'.format(categories[0][0], c) for c in categories[0][1]])
                filtered_data = self.data.query(query_expr)
                return group_data(filtered_data, categories[0][0])
            else:
                return group_data(self.data, categories[0])

        query_expr = ''
        for cat in categories:
            if type(cat) is tuple:
                if type(cat[1]) is list:
                    if query_expr != '':
                        query_expr += '&'
                    query_expr = '({})'.format(' | '.join(['{} == "{}"'.format(cat[0], c) for c in cat[1]]))
                else:
                    query_expr = '({})'.format('{} == "{}"'.format(cat[0], cat[1]))

        if query_expr != '':
            filtered_data = self.data.query(query_expr)
        else:
            filtered_data = self.data

        def cats():
            return [c if type(c) is str else c[0] for c in categories]
        gd = group_data(filtered_data, cats()).unstack(level=level)
        return gd.sort_values(by=gd.columns[level])

