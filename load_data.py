#%%
#from dbtools import *

#%%
def adm_plan_config(myduck,batch_id):	
	sql=f"""	
select batch_id,start_date as dt,row_number() over (order by start_date desc) dt_rn
from {gp}.tenant_snowbeer_adm.adm_plan_config 
where batch_id={batch_id}
"""
	myduck.from_db(sql,f"adm_plan_config")
	
def adm_product_available(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
select batch_id,stockorg_code, sku_code, stock_date dt,available_qty begin_stock_qty 
from tenant_snowbeer_adm.adm_product_available  a
where a.batch_id={batch_id}
$$)"""
	myduck.from_db(sql,f"adm_product_available")    

def adm_production_line_product_producibility(myduck,batch_id):	
	sql=f"""
select * from postgres_query('{gp}',$$
select batch_id,factory_code, line_code, sku_code, to_date(concat(start_year,'-', start_month),'YYYY-MM') dt_start,
to_date(concat(end_year,'-', end_month),'YYYY-MM') dt_end,'warehouse.'||factory_code stockorg_code --select count(*)--select *
from tenant_snowbeer_adm.adm_production_line_product_producibility a
where a.producibility=1 and a.batch_id={batch_id}
;$$)"""
	myduck.from_db(sql,f"adm_production_line_product_producibility")

def adm_safety_stock(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
select batch_id,stockorg_code,sku_code,
make_date(start_year, start_month,1) dt_start,
make_date(end_year, end_month,1) dt_end,safety_stock_days
from tenant_snowbeer_adm.adm_safety_stock 
where  batch_id={batch_id} 
$$)"""
	myduck.from_db(sql,f"adm_safety_stock")

def adm_sku_transfer_route(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
SELECT batch_id,receive_sku_code,receive_stockorg_code,send_sku_code,send_stockorg_code,priority,
	make_date(year, month,1) dt 
from tenant_snowbeer_adm.adm_sku_transfer_route a 
where a.batch_id={batch_id}
$$)"""
	myduck.from_db(sql,f"adm_sku_transfer_route")

def adm_warehouse_sku_month_demand(myduck,batch_id):	
	sql=f"""
select * from postgres_query('{gp}',$$
with a as(
select a.batch_id,a.stockorg_code, sku_code, to_date(concat(year,'-', month),'YYYY-MM') dt,
 demand_qty -- select *
from tenant_snowbeer_adm.adm_warehouse_sku_month_demand a
where a.demand_type=10 and a.batch_id={batch_id}
)select a.*,coalesce (b.demand_qty,a.demand_qty) demand_qty_next ,coalesce (b.dt,a.dt) dt2
from a
left join a as b on a.dt+'1 month'::interval =b.dt 
and a.stockorg_code=b.stockorg_code and a.sku_code=b.sku_code
$$)
"""
	myduck.from_db(sql,f"adm_warehouse_sku_month_demand")

def adm_production_line_info(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
	select * from tenant_snowbeer_adm.adm_production_line_info where batch_id={batch_id}$$)
	"""
	myduck.from_db(sql,f"adm_production_line_info")

def adm_production_line_capacity(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
select factory_code,line_code
,make_date(start_year,start_month,1) start_dt, make_date(end_year,end_month,1) end_dt
,max_hour
from tenant_snowbeer_adm.adm_production_line_capacity
where max_hour>0 and batch_id={batch_id}$$)
"""
	myduck.from_db(sql,f"adm_production_line_capacity")

def adm_production_rate(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
	SELECT batch_id,a.factory_code,a.line_code,production_rate,
	make_date(start_year,start_month,1) start_dt,
	make_date(start_year,start_month,1) end_dt
FROM tenant_snowbeer_adm.adm_production_rate a
where production_rate>0 and batch_id={batch_id}
$$)"""
	myduck.from_db(sql,f"adm_production_rate")

def adm_production_line_product_priority(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
	select batch_id , factory_code, line_code, sku_code, make_date(year, month,1) dt, priority
from tenant_snowbeer_adm.adm_production_line_product_priority  
where batch_id ={batch_id}
$$)"""
	myduck.from_db(sql,f"adm_production_line_product_priority")

def dim_stockorg(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
	select stockorg_code, stockorg_name,parent_org_code
from tenant_snowbeer_edw.dim_stockorg
where day_date=(select day_date from tenant_snowbeer_adm.adm_plan_config limit 1)
$$)"""
	myduck.from_db(sql,f"dim_stockorg")

def adm_factory_linetype_capacity_gap(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
select a.factory_code,a.linetype,a.dt,a.qty_surplus,a.qty_shortage
from tenant_snowbeer_adm.adm_factory_linetype_capacity_gap a
where a.batch_id={batch_id}
$$)"""
	myduck.from_db(sql,f"adm_factory_linetype_capacity_gap")
#%%
def adm_factory_capacity(myduck,batch_id):	
	sql=f"""select * from postgres_query('{gp}',$$
select a.factory_code,a.linetype, 
make_date(start_year,start_month,1) dt_start,
make_date(end_year, end_month,1) dt_end,
equilibrium_capacity
from tenant_snowbeer_adm.adm_factory_capacity a
where a.batch_id={batch_id}
$$)"""
	sql=f"""
select a.factory_code,
make_date(start_year,start_month,1) dt_start,
make_date(end_year, end_month,1) dt_end,
equilibrium_capacity
from {gp}.tenant_snowbeer_adm.adm_factory_capacity a
where a.batch_id={batch_id}"""
	myduck.from_db(sql,f"adm_factory_capacity")
#%%
def advance_linetype_capacity(myduck,batch_id):	
	sql=f"""
select batch_id,factory_code,linetype,dt,linetype_rn,capacity::numeric capacity-- select *
from {gp}.tenant_snowbeer_adm.advance_linetype_capacity 
--where batch_id={batch_id}
"""
	myduck.from_db(sql,f"advance_linetype_capacity")
#%%
def load_data(myduck,batch_id,data_source='parquet'):
	if data_source=='parquet':
		exit()
	else:
		advance_linetype_capacity(myduck,batch_id)


#%%
if __name__ == '__main__':
	from dbtools import *
	batch_id=203
	gp=myduck.attach_db(dburl,'gp')
	load_data(myduck,batch_id,gp)
	


# %%
