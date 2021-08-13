import pandas as pd
import numpy as np


# Function to read data
def read_data(path):

    """
    reads file from  filepath and return a dataframe

    :param path: the path of file to be read
    :return: the dataframe
    """

    df = pd.read_json(path, lines=True)
    print("The shape of dataframe is:", df.shape)
    print("\nThe information about the columns:\n")
    df.info()
    df.head()

    return df

df_read = read_data("supplier_car.json")

# Function to preprocess data
def pre(df, index, col, new_col, old_col):

    """
    pre-process the data
    :param df: dataframe
    :param index: columns to be made index
    :param col: column to be used as column values
    :param new_col: new column created after split
    :param old_col: old column to be passed for splitting
    :return: a pre-processed dataframe
    """

    df = df.set_index(index, append=True)[col].unstack().reset_index().rename_axis(columns = None)
    df[new_col] = df[old_col].str.split('l/100',expand=True)
    return df
index = ['MakeText', "TypeName", "ModelText", "Attribute Names"]
col = "Attribute Values"
new_col = ['mileage','mileage_unit']
old_col = "ConsumptionTotalText"
df_pre = pre(df_read, index, col, new_col, old_col)


# Function to normalize the data
def norm(df, replace_cols, replace_value_dict, col):
    """
    Data Normalization

    :param df: pre-processed dataframe
    :param replace_cols: columns which will get their value replaced
    :param replace_value_dict: new value to replace old value
    :param col: column which need to change the letter case
    :return: Normalized dataframe
    """
    df[replace_cols] = df[replace_cols].replace(replace_value_dict)
    col_name = []
    for x in df[col]:
        if len(x) <= 3:
            x = x.upper()
            col_name.append(x)
        else:
            x = x.title()
            col_name.append(x)
    df[col] = col_name
    return df


replace_cols = ["ModelText", "BodyColorText", "BodyTypeText", "ConditionTypeText", "FirstRegMonth",
       "FirstRegYear", "mileage", "City", "mileage_unit"]

replace_value_dict = {
                                  "km":"kilometer",
                                   None:"null",
                                    "0" : "4",
                                }


df_norm = norm(df_pre, replace_cols, replace_value_dict, "MakeText")
df_norm["BodyColorText"] = df_norm["BodyColorText"].fillna('No Value').apply(lambda x :x.title()).replace('No Value',np.nan)

# Function to integrate data
def integration(df, drop_col, rename_dict, seq_list, seq_front=True):

    """
    Integration of the dataframe
    :param df: normalized dataframe
    :param drop_col: column to be dropped
    :param rename_dict: columns to be renamed
    :param seq_list: sequence of column in final dataframe
    :param seq_front: columns that are to be kept in beginning
    :return: integrated dataframe
    """

    df.drop(drop_col, axis = 1, inplace = True)
    df.rename(columns = rename_dict, inplace = True)
    cols = seq_list[:] # copy so we don't mutate seq
    for x in df.columns:
        if x not in cols:
            if seq_front: #we want "seq" to be in the front
                #so append current column to the end of the list
                cols.append(x)
    return df[cols]

drop_col = ["level_0",'Ccm', 'Co2EmissionText','ConsumptionRatingText','Doors','DriveTypeText',
             'FuelTypeText', 'Hp', 'InteriorColorText', 'Km','Properties', 'Seats', 'TransmissionTypeText',
           'ConsumptionTotalText']

rename_dict = {
'BodyTypeText'              : 'carType',
'BodyColorText'             : "color",
'ConditionTypeText'         : 'condition',
'City'                      : "city",
"MakeText"                  : "make",
'FirstRegYear'              : 'manufacture_year',
'ModelText'                 : 'model_variant',
'TypeName'                  : 'model',
'FirstRegMonth'             : 'manufacture_month'

    }

seq_list = ['carType', 'color','condition', 'city', 'make', 'manufacture_year', 'mileage', 'mileage_unit',
            'model', 'model_variant', 'manufacture_month']

df_final = integration(df_norm, drop_col, rename_dict, seq_list)

df_final['mileage'] = pd.to_numeric(df_final['mileage'], errors='coerce')
df_final['manufacture_month'] = pd.to_numeric(df_final['manufacture_month'], errors='coerce')
df_final['manufacture_year'] = pd.to_numeric(df_final['manufacture_year'], errors='coerce')

with pd.ExcelWriter('task_output.xlsx') as writer:
    df_pre.to_excel(writer, sheet_name='pre-process')
    df_norm.to_excel(writer, sheet_name='normalization')
    df_final.to_excel(writer, sheet_name='integration')