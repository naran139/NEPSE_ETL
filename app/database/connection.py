from sqlalchemy import create_engine

con_str = 'postgresql://postgres:2001@host.docker.internal:5432/NEPSE'
engine = create_engine(con_str)