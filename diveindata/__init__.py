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
        self.pandas_query = ''

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
        if type(ctype) is str:
            ctype = [ctype]
        if not exclude_equiv:
            return [col for col in self.data.columns if self.columns[col]['type'] in ctype]
        else:
            return [col for col in self.data.columns if self.columns[col]['type'] in ctype
                                        and 'has_equivalent' in self.columns[col]
                                        and self.columns[col]['has_equivalent']]

    def calc_numerical_stats(self):
        for col in (self.columns_by_type('INT') + self.columns_by_type('FLOAT')):
            self.columns[col]['min'] = float(self.data[col].min())
            self.columns[col]['idxmin'] = float(self.data[col].min())
            self.columns[col]['max'] = float(self.data[col].max())
            self.columns[col]['sum'] = float(self.data[col].sum())
            self.columns[col]['mean'] = float(self.data[col].mean())
            self.columns[col]['std'] = float(self.data[col].std())

    def bar_groups(self, categories=[], num_col=None, aggs=['sum'], level=0):
        self.pandas_query = ''

        def group_data(data, category):
            grouped_data = data.groupby(category)
            self.pandas_query = self.pandas_query + '.groupby({})'.format(category)

            if num_col:
                self.pandas_query += '[{}].agg({}).sort_values(by={}, na_position=\'first\')'.format(num_col, aggs, aggs[0])
                return grouped_data[num_col].agg(aggs).sort_values(by=aggs[0], na_position='first')
            else:
                self.pandas_query += '.size().sort_values(na_position=\'first\')'
                return grouped_data.size().sort_values(na_position='first')

#        if len(categories) == 1:
#            if type(categories[0][1]) is list:
#                query_expr = ' | '.join(['{} == "{}"'.format(categories[0][0], c) for c in categories[0][1]])
#                filtered_data = self.data.query(query_expr)
#                self.pandas_query = 'data.query(\'{}\')'.format(query_expr)
#                return group_data(filtered_data, categories[0][0])
#            else:
#                return group_data(self.data, categories[0])

        query_expr = ''
        for cat in categories:
            if type(cat) is tuple:
                if query_expr != '':
                    query_expr += '&'

                if type(cat[1]) is list:
                   query_expr += '({})'.format(' | '.join(['{} == "{}"'.format(cat[0], c) for c in cat[1]]))
                else:
                   query_expr += '({})'.format('{} == "{}"'.format(cat[0], cat[1]))

        if query_expr != '':
            filtered_data = self.data.query(query_expr)
            self.pandas_query = 'data.query(\'{}\')'.format(query_expr)
        else:
            filtered_data = self.data
            self.pandas_query = 'data'

        def cats():
            return [c if type(c) is str else c[0] for c in categories]
        gd = group_data(filtered_data, cats()).unstack(level=level)
        self.pandas_query += '.unstack(level={}).sort_values(by={}, na_position=\'first\')'.format(level, list(gd.columns))
        print(self.pandas_query)
        return gd.sort_values(by=gd.columns[level])

