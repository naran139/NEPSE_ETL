from app.etl.transform import transform
from sqlalchemy import inspect
from app.database.connection import engine
from sqlalchemy.sql import text
import pandas as pd
def load(dim_date,dim_symbol,fact_stock_trade):

    # Tables 
    f_stock = "fact_stock_trade"
    d_symbol = "dim_symbol"
    d_date = "dim_date"

    #Transfrom Table

    inspector = inspect(engine)

    if d_symbol not in inspector.get_table_names():
        try:
            dim_symbol = pd.read_sql_table('temp_dim_symbol',con = engine)
            dim_symbol.to_sql(name = d_symbol, con = engine, if_exists = "replace", index = False)
        except Exception as e:
            print(f"creating Dim Symbol failed {e}")
    else:
        try:
            dim_symbol.to_sql(name = 'temp_dim_symbol', con = engine, if_exits = "replace", index = False)
            sql = """MERGE dim_symbol as D
                    USING temp_dim_symbol as S
                    on D.symbol_id = S.symbol_id
                    WHEN MATCHED THEN
                    UPDATE SET
                    D.symbol = S.symbol,
                    D.symbol_id = S.symbol_id,
                    D.year_high = S.year_high,
                    D.year_low = S.year_low
                    WHEN NOT MATCHED BY TARGET THEN
                    INSERT(symbol,symbol_id,year_high,year_low)
                    Values(S.symbol,S.symbol_id,year_high,year_low)"""
            with engine.begin() as con:
                con.execute(text(sql))
                con.commit()
        except Exception as e:
            print(f" Load Dim Symbol Failed{e}")

    if d_date not in inspector.get_table_names():
        try:
            dim_date = pd.read_sql_table('temp_dim_date',con = engine)
            dim_date.to_sql(name = d_date, con = engine, if_exists = "replace", index = False)
        except Exception as e:
            print(f"creating Dim Date Failed{e}")
    else:
        try:
            dim_date.to_sql(name = 'temp_dim_date', con = engine, if_exists = "replace", index = False)
            sql = """
                    MERGE dim_date as D
                    Using temp_dim_date as S
                    On S.date_id = D.date_id
                    WHEN MATCHED THEN 
                    UPDATE SET 
                    D.date_id = S.date_id,
                    D.date = S.date
                    WHEN NOT MATCHED BY TARGET THEN
                    INSERT(date_id,date)
                    VALUES(S.date_id,S.date) """
            with engine.begin() as con:
                con.execute(text(sql))
                con.commit()
        except Exception as e:
            print(f"Load Dim Date Failed{e}")
    
    if f_stock not in inspector.get_table_names():
        try:
            fact_stock_trade = pd.read_sql_table('temp_fact_table',con = engine)
            fact_stock_trade.to_sql(name = f_stock, con = engine, if_exists = "replace", index = False)
        except Exception as e:
            print(f"Creating Fact Table failed {e}")
    else:
        try:
            dim_symbol.to_sql(name = 'temp_fact_table', con = engine, if_exits = "replace", index = False)
            sql = """MERGE fact_stock_trade as D
                    USING temp_fact_table as S
                    on D.row_id = S.row_id
                    WHEN MATCHED THEN
                    UPDATE SET
                    D.open_price = S.open_price,
                    D.high_price = S.high_price,
                    D.close_price = S.close_price,
                    D.low_price = S.low_price,
                    D.ltp = S.ltp,
                    D.total_traded_quantity = S.total_traded_quantity,
                    D.total_traded_value = S.total_traded_value,
                    D.total_trades = S.total_trades,
                    D.previous_day_close_price = S.previous_day_close_price,
                    D.average_traded_price = S.average_traded_price,
                    D.year_low = S.year_low,
                    D.year_high = S.year_high,
                    D.market_capitalization = S.market_capitalization
                    WHEN NOT MATCHED BY TARGET THEN
                    INSERT(row_id, open_price,high_price,close_price,low_price,ltp,total_traded_quantity,
                            total_traded_value,total_trades,previous_day_close_price,average_traded_price,
                            year_low,year_high,market_capitalization)
                    Values(S.row_id, S.open_price,S.high_price,S.close_price,S.low_price,S.ltp,S.total_traded_quantity,
                            S.total_traded_value,S.total_trades,S.previous_day_close_price,S.average_traded_price,
                            S.year_low,S.year_high,S.market_capitalization)"""
            with engine.begin() as con:
                con.execute(text(sql))
                con.commit()
        except Exception as e:
            print(f" Load Fact Table Failed{e}")


