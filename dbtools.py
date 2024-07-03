# %%
import duckdb
import time
from datetime import datetime
from icecream import ic
from dbconfig import dburl
# mysingleton.py
def singleton(cls):
	instances = {}
	def getinstance(*args, **kwargs):
		if cls not in instances:
			instances[cls] = cls(*args, **kwargs)
		return instances[cls]
	return getinstance

@singleton
class DuckTool(object):
	# 初始化duck，如未安装插件，则安装
	def __init__(self):
		print('duckdb version:',duckdb.__version__)
		ic(dburl)
		self.dd=duckdb.connect('advance_production.duckdb')
		sql="""select * from duckdb_extensions() where extension_name like 'postgres%'"""
		if not self.dd.execute(sql).fetchone():
			print("install postgres extension")
			self.dd.execute("install postgres;SET pg_experimental_filter_pushdown=true;")
		self.gp=None
	
	def attach_db(self,dburl,gp='gp'):
		sql=f"""load postgres;ATTACH '{dburl}' AS {gp} (TYPE postgres);"""
		if not self.dd.execute(f"select * from duckdb_databases where database_name='{gp}'").fetchone():
			print(f"attach {gp}")
			self.dd.execute(sql)
		return gp

	def from_db(self,sql,file):
		t=time.time()
		sql=f"""copy ({sql}) to 'parquet/{file}.parquet'"""
		ic(sql)
		self.dd.execute(sql)
		ic('cost:',round(time.time()-t,2))
		self.dd.execute(f"drop view if exists {file};create view {file} as select * from read_parquet('parquet/{file}.parquet')")
		self.dd.sql(f"""select  count(*) from {file}""") #debug用，正式时注释掉
	
	def ducksql(self,sql):
		ic('now:',datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
		#t=time.time()
		self.dd.execute(sql)
		#ic('cost:',round(time.time()-t,2))
 
myduck = DuckTool()
 
#%%
	
from dbtools import *
if __name__ == '__main__':
	print(myduck.dd)
	gp=myduck.attach_db(dburl,'gp')
	print(gp)

# %%
