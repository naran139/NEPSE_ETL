from app.database.connection import engine
import pandas as pd
from datetime import datetime
import pandas as pd
from app.etl.generate_hash import generate_hash
from app.etl.generate_uuid import generate_uuid


def rename_column(new_df):
    new_df.columns = new_df.columns.str.strip()
    columns =[
        'Symbol','Open Price (Rs)', 'High Price (Rs)', 'Low Price (Rs)', 'Total Traded Quantity', 'Total Traded Value',
        'Total Trades','LTP','Previous Day Close Price (Rs)', 'Average Traded Price (Rs)','52 Week High (Rs)',
        '52 Week Low (Rs)','date', 'Close Price* (Rs)','MarketCapitalization (Rs) (Amt in millions)'
    ]
    renamed_selected_df = new_df[columns].copy()
    renamed_selected_df.rename(columns={'Symbol':'symbol',
                                        'Open Price (Rs)':'open_price',
                                        'High Price (Rs)':'high_price',
                                        'Low Price (Rs)':'low_price',
                                        'Total Traded Quantity':'total_traded_quantity',
                                        'Total Traded Value':'total_traded_value',
                                        'Total Trades':'total_trades',
                                        'LTP':'ltp',
                                        'Previous Day Close Price (Rs)':'previous_day_close_price',
                                        'Average Traded Price (Rs)':'average_traded_price',
                                        '52 Week High (Rs)':'year_high',
                                        '52 Week Low (Rs)':'year_low',
                                        'date':'date',
                                        'MarketCapitalization (Rs) (Amt in millions)':'market_capitalization',
                                        'Close Price* (Rs)':'close_price'},inplace = True)
    
    return renamed_selected_df

def change_data_type(renamed_selected_df):
    obj_types = ['symbol']
    date_types = ['date']
    int_types = ['total_traded_quantity','total_trades']
    float_types = ['open_price','high_price','low_price','total_traded_value','ltp','year_high','year_low',
                   'previous_day_close_price','average_traded_price','market_capitalization','close_price']
    
    for column in renamed_selected_df.columns:
        if column in obj_types:
            renamed_selected_df[column] = renamed_selected_df[column].astype('object')
        elif column in date_types:
            renamed_selected_df[column] = pd.to_datetime(renamed_selected_df[column])
        elif column in int_types:
            renamed_selected_df[column] = renamed_selected_df[column].replace(",","",regex = True).astype('int64')
        elif column in float_types:
            renamed_selected_df[column] = renamed_selected_df[column].astype(str) 
            renamed_selected_df[column] = renamed_selected_df[column].str.replace(r"\(.*", "", regex=True)
            renamed_selected_df[column] = renamed_selected_df[column].replace("-", 0,regex = True)
            renamed_selected_df[column] = renamed_selected_df[column].replace(",", "",regex = True).astype('float64')
            
    
    return renamed_selected_df


def create_dimension_table(df,columns,id_column=None,hash_function= None):
    dim_table = df[columns].drop_duplicates()
    dim_table.reset_index(inplace=True,drop=True)

    if id_column and hash_function:
        dim_table[id_column] = dim_table.apply(hash_function,axis = 1)
        # print(f"Dimension Table Created:\n{dim_table.head()}")

    return dim_table

def create_fact_table(df,dim_tables,merge_keys,drop_columns):
    for dim_table , merge_key in zip(dim_tables,merge_keys):
        df = df.merge(dim_table, left_on = merge_key, right_on = merge_key, how='inner')

    df.drop(columns = drop_columns,inplace = True)
    df['row_id'] = df.apply(lambda _: generate_uuid(),axis = 1)

    return df

def transform():
    try:
        query = """SELECT *
                    FROM staging_area
                    WHERE date = (
                        select max(date) as latest_date
                        FROM staging_area
                    )"""
        # Fetch data
        # engine = create_engine('postgresql://postgres:2001@localhost:5432/NEPSE')
        new_df = pd.read_sql(query,con = engine)
        rename_new_df = rename_column(new_df)

        
        data_type_changed_df = change_data_type(rename_new_df)

       
        dim_symbol = create_dimension_table(data_type_changed_df,['symbol','year_high','year_low'],'symbol_id',generate_hash)
        dim_date =create_dimension_table(data_type_changed_df,['date'],'date_id',generate_hash)

       
        dim_tables = [dim_symbol,dim_date]
        merge_keys = ['symbol','date']
        drop_columns = ['date','symbol','year_high_x','year_low_x','year_high_y','year_low_y']

        fact_stock_trade = create_fact_table(data_type_changed_df,dim_tables,merge_keys,drop_columns)

        try:
            dim_date.to_sql('temp_dim_date',con = engine, if_exists="replace", index= False )
            dim_symbol.to_sql('temp_dim_symbol',con= engine,if_exists="replace",index=False)
            fact_stock_trade.to_sql('temp_fact_table',con = engine , if_exists="replace", index = False)
        except Exception as e:
            print(f"An error Occured{e}")


        return dim_date,dim_symbol,fact_stock_trade

        
        
    except Exception as e:
        print(f"An error Occured{e}")


