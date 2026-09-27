import pandas as pd

# Dict for data manipulation
dict_col_types = {
    'string': 'String',
    'int': 'IntegerType',
    'bigint': 'LongType'
}

dict_exp = {
    'type': 'expected_column_values_to_be_of_type',
    'not_null': 'expect_column_values_to_not_be_null',
    'unique': 'expect_column_values_to_be_unique'
}

# Prefix and Suffix
first_line = """{"expectations": [\n"""
# last_line = """\n], "data_asset_type": "SparkDFDataset", "expectation_suite_name": ""

exp_type = """    {"expectation_type": \""""
column_value = """, "kwargs": {"column": \""""
column_type = """, "type_": \""""
meta_value = """"}, "meta": {"ExpectationsDatasetProfiler": {"confidence": "very high"}}}"""

COL_NAME = 'Column_Name'
COL_DATA_TYPE = 'Data_Type'
COL_FEATURE = 'Key'

file_name = 'C:/Users/Remzi/PycharmProjects/aws/GE/source/ge.xlsx'
json_path_to_write = 'C:/Users/Remzi/PycharmProjects/aws/GE/target/'
table_name = ['transfer_table']

# Methods
def get_col_name(row):
    col_name = row[COL_NAME].replace(' ','_')
    return col_name.lower().strip()

def get_type_line(row):
    line = ",\n" + exp_type + dict_exp['type'] + column_value + get_col_name(row) + column_type + \
        dict_col_types[row[COL_DATA_TYPE].lower().strip()] + meta_value
    return line

def get_not_null_line(row):
    line = ",\n" +exp_type + dict_exp['not_null'] + column_value + get_col_name(row) + meta_value
    return line

def get_unique_line(row):
    line = ",\n" +exp_type + dict_exp['unique'] + column_value + get_col_name(row) + meta_value
    return line

def get_column_list_line(df):
    line_begin = """    {"expectation_type": "expect_table_columns_to_match_ordered_list", "kwargs": {"column_list": [\""""
    line_end = """"]}, "meta": {"ExpectationsDatasetProfiler": {"confidence": "very high"}}}"""
    column_list = '", "'.join(  [name.lower().strip().replace(' ','_') for name in df[COL_NAME]]  )
    return line_begin + column_list + line_end

def get_last_line(table):
    first_part = """\n], "data_asset_type": "SparkDFDataset", "expectation_suite_name": \""""
    last_part = """_expectation"}"""
    return first_part + table + last_part

# =====================================================================================================

for sheet in table_name:
    df = pd.read_excel(file_name, sheet_name=sheet, skiprows=[0, 1], usecols=[0, 1, 2, 3])
    df = df.filter(items=[COL_NAME, COL_DATA_TYPE, COL_FEATURE])
    df = df[~df[COL_NAME].isna()]
    print("======> " + sheet + " ===> " + str(len(df)))

    expectation_body = ""

    for index, row in df.iterrows():
        expectation_body += get_type_line(row)

        if str(row[COL_FEATURE]).lower().strip() in ['pk']:
            expectation_body += get_not_null_line(row)
            expectation_body += get_unique_line(row)

        elif str(row[COL_FEATURE]).lower().strip() in ['fk']:
            expectation_body += get_not_null_line(row)

        elif str(row[COL_FEATURE]).lower().strip() in ['unique']:
            expectation_body += get_unique_line(row)

        elif str(row[COL_FEATURE]).lower().strip() in ['notnull']:
            expectation_body += get_not_null_line(row)

    expectations_json = first_line + get_column_list_line(df) + expectation_body + get_last_line(sheet.lower())

    print(expectations_json)

    with open(f"{json_path_to_write}{sheet.lower()}_expectation.json", 'w') as f:
        f.write(expectations_json)

