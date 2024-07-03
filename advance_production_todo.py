#%% 
from dbtools import *
dd=myduck.dd
ducksql=myduck.ducksql
gp=myduck.gp
# 
# %%
#-- 没有生产可能性的需求(需找他人转储)
sql="""drop table if exists tmp_demand_other;
create temp table tmp_demand_other as
select a.* 
from adm_warehouse_sku_month_demand a
where not exists(select 1 from adm_production_line_product_producibility b 
where a.stockorg_code=b.stockorg_code and a.sku_code=b.sku_code
and a.dt>=b.dt_start and a.dt<b.dt_end
)
and demand_qty>0
;"""
ducksql(sql)
#%%
# -- 有生产可能性的转储路径
sql="""drop table if exists tmp_transfer_route;
create temp table tmp_transfer_route as
select *
from adm_sku_transfer_route a
where exists(select 1 from adm_production_line_product_producibility b
	where a.send_stockorg_code=b.stockorg_code and a.send_sku_code=b.sku_code
	and a.dt>=b.dt_start and a.dt<b.dt_end )
;"""
ducksql(sql)
# %%
#-- 没有生产可能性的要转入，且转出方要有生产可能性
sql=f"""
drop table if exists advance_sku_transfer;
create table advance_sku_transfer as
with t as(select a.stockorg_code, a.sku_code, a.dt, a.demand_qty,
b.send_sku_code,b.send_stockorg_code,b.priority 
,row_number() over (partition by a.stockorg_code,a.sku_code,a.dt order by priority) rn
from tmp_demand_other a
inner join tmp_transfer_route b
	on a.stockorg_code=b.receive_stockorg_code and a.sku_code=b.receive_sku_code and a.dt=b.dt
-- 转出方要有生产可能性
)select * from t where rn=1
;"""
ducksql(sql)
# %%
# 安全库存（下月的日均*安全库存天数，末月无下月取当月）
sql="""drop table if exists tmp_safety;
create temp table tmp_safety as
with c as(
select stockorg_code,sku_code,cfg.dt dt,safety_stock_days,dt_rn rn
from adm_plan_config cfg 
inner join adm_safety_stock safe on cfg.dt>=safe.dt_start and cfg.dt<safe.dt_end
),rst as(select c.stockorg_code, c.sku_code,c.dt,d.demand_qty
/extract(DAY FROM (date_trunc('MONTH', c.dt) + INTERVAL '2 MONTH' - interval '1 day'))*c.safety_stock_days qty
,'safety'::text qty_type,c.rn 
from c 
inner join adm_warehouse_sku_month_demand d on c.dt=d.dt-'1 month'::interval and c.stockorg_code=d.stockorg_code and c.sku_code=d.sku_code
)
select stockorg_code, sku_code, strftime(dt,'%Y-%m-%d') dt, qty, qty_type from rst 
union all
select stockorg_code, sku_code, strftime(dt+'1 month'::interval,'%Y-%m-%d') dt, qty, qty_type from rst where rn=2
;"""
ducksql(sql)
#%%
dd.sql("from tmp_safety")
# %%
# 循环计算库存
do_month = dd.sql("""select min(dt) from adm_plan_config""").fetchone()[0].strftime("%Y-%m-%d")
ic(do_month) #2024-04-01 00:00:00 #注意字符串和日期的汇总
sql=f"""-- 第一个月的期初库存
drop table if exists tmp_stock;
create temp table tmp_stock as
select stockorg_code, sku_code, '{do_month}'::date as dt
,begin_stock_qty 
from adm_product_available  a
;"""
ducksql(sql)
#%%
# 创建 todo临时表
sql=f"""drop table if exists adm_warehouse_sku_todo;
create table adm_warehouse_sku_todo(
	stockorg_code text,
	sku_code text,
	dt date,
	begin_stock_qty numeric,
	demand_qty numeric,
	production_qty numeric,
	trans_in_qty numeric,
	trans_out_qty numeric,
	safety_stock_qty numeric,
	todo_qty numeric,
	end_stock_qty numeric
)"""
ducksql(sql)

# %%
for r in dd.execute('select dt from adm_plan_config order by 1').fetchall():
	do_month = r[0].strftime("%Y-%m-%d")
	ic('do_month:',do_month)
	sql=f"""-- 重算调拨的量：max(月需求量+安全库存-期初库存,0)
drop table if exists tmp_transfer_new;
create temp table tmp_transfer_new as
select a.* ,GREATEST(a.demand_qty+coalesce(c.qty,0)-coalesce(b.begin_stock_qty,0),0) qty 
from advance_sku_transfer a 
left join tmp_stock b on a.stockorg_code=b.stockorg_code and a.sku_code=b.sku_code and a.dt=b.dt
left join tmp_safety c on a.stockorg_code=c.stockorg_code and a.sku_code=c.sku_code and a.dt=c.dt
where a.dt='{do_month}'
;"""
	ducksql(sql)

	sql=f"""
-- 汇合所有数据
drop table if exists tmp_all;
create temp table tmp_all as
select stockorg_code,sku_code,dt::date dt,begin_stock_qty qty,'stock' qty_type from tmp_stock --期初
union all
select stockorg_code,sku_code,dt,demand_qty qty,'demand' qty_type 
from adm_warehouse_sku_month_demand where dt='{do_month}' --月需求
union all --生产量
select stockorg_code,sku_code,dt,demand_qty qty,'production' qty_type 
from adm_warehouse_sku_month_demand a
where exists(select 1 from adm_production_line_product_producibility b 
	where a.stockorg_code=b.stockorg_code and a.sku_code=b.sku_code and a.dt>=b.dt_start and a.dt<b.dt_end)
	and dt='{do_month}'
union all
select stockorg_code,sku_code,dt, qty,'trans_in' qty_type from tmp_transfer_new where dt='{do_month}'
union all
select send_stockorg_code,send_sku_code,dt, qty,'trans_out' qty_type from tmp_transfer_new where dt='{do_month}'
union all
select stockorg_code,sku_code,dt, qty,'safety' qty_type from tmp_safety where dt='{do_month}'
;"""
	ducksql(sql)

	sql=f"""
drop table if exists tmp_todo;
create temp table tmp_todo as
select stockorg_code,sku_code,dt
,coalesce(sum(qty) filter (where qty_type='stock'),0) begin_stock_qty
,coalesce(sum(qty) filter (where qty_type='demand'),0) demand_qty
,coalesce(sum(qty) filter (where qty_type='production'),0) production_qty
,coalesce(sum(qty) filter (where qty_type='trans_in'),0) trans_in_qty
,coalesce(sum(qty) filter (where qty_type='trans_out'),0) trans_out_qty
,coalesce(sum(qty) filter (where qty_type='safety'),0) safety_stock_qty 
from tmp_all 
group by stockorg_code,sku_code,dt
;"""
	ducksql(sql)
	# 插入结果表
	#--    待生产量:无生产可能性，直接置为0. 有生产可能性，max(月需求量+转出量+安全库存-期初库存-转入量,0)
#--    期末库存:max(期初库存+转入量+待生产量-月需求量-转出量，0)
	sql=f"""insert into adm_warehouse_sku_todo 
with a as(
select a.*
,case when trans_in_qty>0 then 0 else 
GREATEST(demand_qty+trans_out_qty+safety_stock_qty-begin_stock_qty-trans_in_qty,0) end as todo_qty
from tmp_todo a
)select a.*,GREATEST(begin_stock_qty+trans_in_qty+todo_qty-demand_qty-trans_out_qty,0) end_stock_qty
from a
;
"""
	ducksql(sql)
	# 下轮循环前，本月期末做下个月的期初库存
	sql=f"""truncate table tmp_stock;
insert into tmp_stock
select stockorg_code, sku_code, dt+'1 month'::interval dt 
,end_stock_qty begin_stock_qty 
from adm_warehouse_sku_todo
where dt='{do_month}'
; """
	ducksql(sql)
ic('loop end')
#%%
#验证结果
# dd.sql("""select dt,sum(COLUMNS('.*qty')) from adm_warehouse_sku_todo group by dt""")
#%%
sql="""drop table if exists tmp_linetype;
create temp table tmp_linetype as
select factory_code,line_code--,case when is_pure_draft='是' then '纯生-' else '普通-' end ||package_type linetype
,case when a.line_code in('WHC_BZD','QJC_BZ听') then a.line_code 
else ((case when is_pure_draft='是' then '纯生-' else '普通-' end)||package_type ) end linetype,
case when a.line_code in('WHC_BZD','QJC_BZ听') then 1 when package_type in ('瓶','听') then 2 else 0 end linetype_rn--select *
from adm_production_line_info a
;"""
ducksql(sql)
#%%
#-- 汇总到产线类型,1. sku用生产可能性换算到产线类型；2. 产线有产线类型属性
sql="""drop table if exists tmp_linetype_sku;
create temp table tmp_linetype_sku as
with a as(--可能性
select factory_code,line_code,stockorg_code,sku_code,b.dt
from adm_production_line_product_producibility a
inner join adm_plan_config b on b.dt>=a.dt_start and b.dt<a.dt_end
),p as(--优先级
    select  factory_code, line_code, sku_code, dt, priority
	from adm_production_line_product_priority 
),a2 as(
select  a.factory_code, a.line_code, a.sku_code,a.dt,p.priority
,row_number() over (partition by a.factory_code, a.sku_code,a.dt order by p.priority) rn
from a --取优先级最小的一条
inner join p 
on a.factory_code=p.factory_code and a.line_code=p.line_code and a.sku_code=p.sku_code and a.dt=p.dt
)select a.*,b.linetype
from a2 a 
inner join tmp_linetype b on a.factory_code=b.factory_code and a.line_code=b.line_code
where a.rn=1
;"""
ducksql(sql)
# %%
# -- 确定 sku 的唯一产线类型，带日期，全纯生才纯生，有普通则普通，取巧：select least('普通','纯生')
sql="""drop table if exists advance_sku_linetype ;
create table advance_sku_linetype as
select factory_code,'warehouse.'||factory_code stockorg_code,sku_code,dt
    ,array_agg(distinct linetype) linetypes
    ,min(linetype) linetype
from tmp_linetype_sku
group by factory_code,sku_code,dt
"""
ducksql(sql)
# %%
# 汇总到产线类型的 todo
sql="""drop table if exists adm_warehouse_linetype_todo ;
create table adm_warehouse_linetype_todo as
select a.stockorg_code,b.linetype,a.dt
,sum(a.begin_stock_qty) begin_stock_qty
,sum(a.demand_qty) demand_qty
,sum(a.production_qty) production_qty
,sum(a.trans_in_qty) trans_in_qty
,sum(a.trans_out_qty) trans_out_qty
,sum(a.safety_stock_qty) safety_stock_qty
,sum(a.todo_qty) todo_qty
,sum(a.end_stock_qty) end_stock_qty
from adm_warehouse_sku_todo a
inner join advance_sku_linetype b on a.sku_code=b.sku_code and a.stockorg_code=b.stockorg_code and a.dt=b.dt
group by 1,2,3"""
ducksql(sql)

# %%
#-- 计算产线产能=avg(可生产小时数*生产速率)
sql="""drop table if exists tmp_hour;
create temp table tmp_hour as
select factory_code,line_code
,b.dt,max_hour
from adm_production_line_capacity a
inner join adm_plan_config b on b.dt>=a.start_dt and b.dt<a.end_dt
"""
ducksql(sql)
#%%
"""
drop table if exists tmp_production_rate;
create table tmp_production_rate as 
SELECT a.factory_code,a.line_code,production_rate,b.dt
FROM adm_production_rate a
inner join adm_plan_config b on b.dt>=a.start_dt and b.dt<a.end_dt+interval '1 month'
;"""
ducksql(sql)
#%%
# 关联产线类型
sql="""drop table if exists tmp_rate;
create temp table tmp_rate as
select a.factory_code,a.line_code,b.linetype
,dt,b.linetype_rn
,avg(production_rate) avg_rate 
from tmp_production_rate a
inner join tmp_linetype b on a.factory_code=b.factory_code and a.line_code=b.line_code
group by 1,2,3,4,5
;"""
ducksql(sql)
#%%
#-- 产线类型的产能
sql="""drop table if exists advance_linetype_capacity;
create table advance_linetype_capacity as
select a.factory_code
,b.linetype,b.linetype_rn
,a.dt,
sum(a.max_hour*b.avg_rate) capacity 
from tmp_hour a
inner join tmp_rate b on a.dt=b.dt and a.factory_code=b.factory_code and a.line_code=b.line_code
group by 1,2,3,4
;"""
ducksql(sql)
#%%
# sku待生产明细汇总到产线类型
sql="""drop table if exists adm_factory_linetype_todo;
create table adm_factory_linetype_todo as
select c.parent_org_code factory_code,b.linetype,a.dt
,sum(a.begin_stock_qty) begin_stock_qty
,sum(a.demand_qty) demand_qty
,sum(a.production_qty) production_qty
,sum(a.trans_in_qty) trans_in_qty
,sum(a.trans_out_qty) trans_out_qty
,sum(a.safety_stock_qty) safety_stock_qty
,sum(a.todo_qty) todo_qty
,sum(a.end_stock_qty) end_stock_qty
from adm_warehouse_sku_todo a
inner join advance_sku_linetype b on a.sku_code=b.sku_code and a.stockorg_code=b.stockorg_code and a.dt=b.dt
inner join dim_stockorg c on a.stockorg_code=c.stockorg_code 
group by 1,2,3
;"""
ducksql(sql)
#%%
# sku待生产明细汇总到工厂
sql="""drop table if exists adm_factory_sku_todo;
create table adm_factory_sku_todo as --关联产线类型
select split_part(coalesce(t.send_stockorg_code,a.stockorg_code),'.',2) factory_code,a.dt
,coalesce (t.send_sku_code,a.sku_code) sku_code,coalesce(d.linetype,c.linetype ) linetype
,sum(a.begin_stock_qty) begin_stock_qty
,sum(a.demand_qty) demand_qty
,sum(a.production_qty) production_qty
,sum(a.trans_in_qty) trans_in_qty
,sum(a.trans_out_qty) trans_out_qty
,sum(a.safety_stock_qty) safety_stock_qty
,sum(a.todo_qty) todo_qty,sum(a.end_stock_qty) end_stock_qty
from adm_warehouse_sku_todo a
left join advance_sku_linetype c on a.sku_code=c.sku_code and a.stockorg_code=c.stockorg_code and a.dt=c.dt-- 自产的
left join advance_sku_transfer t on a.sku_code=t.sku_code and a.stockorg_code=t.stockorg_code and a.dt=t.dt
left join advance_sku_linetype d on d.sku_code=t.send_sku_code and t.send_stockorg_code=d.stockorg_code and t.dt=d.dt--转储的
group by 1,2,3,4
"""
ducksql(sql)
# %%
sql="""select dt,sum(todo_qty) 
from adm_warehouse_sku_todo --where batch_id =203
group by dt"""
dd.sql(sql)
# %%
sql="""select dt,sum(todo_qty) 
from adm_factory_linetype_todo --where batch_id =203
group by dt"""
dd.sql(sql)
# %%
sql="""select dt,sum(todo_qty) 
from adm_factory_sku_todo --where batch_id =203
group by dt"""
dd.sql(sql)


# %%
dd.sql('select * from advance_sku_linetype')
# %%
dd.sql('select count(*) from advance_sku_linetype')

# %%
dd.sql("""select dt,sum(COLUMNS('.*qty')) from adm_warehouse_sku_todo group by dt""")
# %%
