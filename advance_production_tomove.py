"""
各产线类型能够挪移的量计算，注意普通可以到纯生线生产，反之不行
输入：各产线类型的 gap（含富余和缺口）adm_factory_linetype_capacity_gap
输出：各月的挪移量 advance_tomove_month
分配逻辑：
1. 从末月往前月循环
2. 每月内部 7 轮循环，依次为纯生-听、普通-听、普通用纯生-听、纯生-瓶、普通瓶、普通用纯生-瓶、特殊产线
"""
#%%
from dbtools import myduck
ducksql=myduck.ducksql
#%%
# 记录往前移的中间表
sql="""
drop table if exists tmp_shortage_add;
create table tmp_shortage_add(
factory_code text,linetype text,dt date,qty_surplus numeric,qty_shortage numeric)
;
"""
ducksql(sql)
#%%
#-- 外层循环的中间表
sql="""drop table if exists tmp_linetype_tomove;	
create table tmp_linetype_tomove(
factory_code text,linetype text,linetype2 text,dt date,qty_tomove numeric,qty_shortage numeric,qty_factory numeric)
;
"""
ducksql(sql)
#%%
#-- 内层循环的中间表
sql="""drop table if exists tmp_tomove;
create table tmp_tomove(
	factory_code text,linetype text,linetype2 text,dt date,qty_tomove numeric)
;
"""
ducksql(sql)
#%%
# 目标表
"""--truncate table tenant_snowbeer_adm.advance_production_month;
DELETE from tenant_snowbeer_adm.advance_production_month WHERE batch_id=v_batch_id;
delete from tenant_snowbeer_adm.advance_tomove_month where batch_id=v_batch_id;
"""
# 从末月往前推
sql="select dt from adm_plan_config order by dt desc"
for do_month in myduck.dd.execute(sql).fetchall():
	print('do_month:',do_month)
#%%
do_month='2024-06-01'
#-- 某月的缺口（当月+后月迁移的）
sql=f"""drop table if exists tmp_shortage_mon;
create temp table tmp_shortage_mon as
with a as(
select a.factory_code,a.linetype,a.dt,a.qty_surplus,a.qty_shortage
from adm_factory_linetype_capacity_gap a
where a.dt='{do_month}'::date
	and qty_shortage>0
union all
select * from tmp_shortage_add
)select a.factory_code,a.linetype,a.dt,sum(a.qty_surplus) qty_surplus,sum(a.qty_shortage) qty_shortage
from a group by 1,2,3
;"""
ducksql(sql)
#%%
if record_count==0:
	print('没有要提前的缺口量:%')
	do_month=do_month-'1 month'::interval;
	continue
#%%
sql="""-- 工厂总缺口
drop table if exists adm_factory_capacity_gap;
CREATE temp TABLE adm_factory_capacity_gap as
select factory_code,  dt,sum(qty_shortage) qty_gap --select *
from tmp_shortage_add 
group by 1,2
;
"""
ducksql(sql)
#%%
sql="""
drop table if exists tmp_shortage;
CREATE temp TABLE tmp_shortage AS
SELECT a.factory_code,a.linetype,a.dt::date dt,a.qty_shortage,b.qty_gap --select *
FROM tmp_shortage_mon a
left join adm_factory_capacity_gap b on a.dt=b.dt and a.factory_code=b.factory_code  
"""
ducksql(sql)

#%%
for r in [('纯生-听','纯生-听'),('普通-听','普通-听'),('普通-听','纯生-听'),('纯生-瓶','纯生-瓶'),('普通-瓶','普通-瓶'),('普通-瓶','纯生-瓶')]:
	print(r[0],r[1])
	sql=f"""insert into tmp_tomove
select a.factory_code,a.linetype,b.linetype linetype2,a.dt,
least(least(b.qty_surplus,a.qty_gap),a.qty_shortage) qty_tomove 
from (select * from tmp_shortage where linetype='{r[0]}')a
inner join (select * from adm_factory_linetype_capacity_gap where linetype='{r[1]}')b
on a.dt=b.dt and a.factory_code=b.factory_code 
    and b.qty_surplus>0
	--and a.qty_gap>0
	"""
	print(sql)
	ducksql(sql)
#%%
	
myduck.dd.sql("from tmp_tomove --where factory_code='GSC' ")
			  #%%
myduck.dd.sql("from tmp_shortage where factory_code='GSC' and linetype='普通-瓶' limit 9")
#%%
myduck.dd.sql("from adm_factory_linetype_capacity_gap where factory_code='GSC' and linetype='普通-瓶' limit 9")
#%%
sql2="""select count(*)
from tmp_shortage a
inner join adm_factory_linetype_capacity_gap b
on a.dt=b.dt and a.factory_code=b.factory_code 
    and b.qty_surplus>0
	and a.linetype='普通-瓶'
	  and b.linetype='普通-瓶'
	--and a.qty_gap>0
	"""
myduck.dd.sql(sql2)
#%%
sql2="""with a as(select *
from tmp_shortage a where linetype='普通-瓶' and factory_code='GSC')
,b as(select * from adm_factory_linetype_capacity_gap where linetype='普通-瓶' and factory_code='GSC')
select * from a --union all select * from b --
inner join  b on a.dt=b.dt and a.factory_code=b.factory_code 
	"""
myduck.dd.sql(sql2)
#%%
# -- 瓶听结束后，排 4 个特殊产线的
sql="""
select a.factory_code,a.linetype,b.linetype linetype2,a.dt,
least(least(b.qty_surplus,a.qty_gap),a.qty_shortage) qty_tomove --select *
from tmp_shortage a
inner join adm_factory_linetype_capacity_gap b
on a.dt=b.dt and a.factory_code=b.factory_code and a.linetype=b.linetype and b.qty_surplus>0
	and a.linetype in('WHC_BZD','QJC_BZ听') AND b.batch_id=v_batch_id
	and a.qty_gap>0
;"""
ducksql(sql)
#%%
record_count=myduck.sql("select count(*) from tmp_tomove")[0][0]
print('记录数: %',record_count)
IF record_count>0 then
	insert into tmp_linetype_tomove
	select * from tmp_tomove
	;-- select * from tmp_linetype_tomove where factory_code='GSC'
-- 更新缺口量 select * from tmp_shortage
	update tmp_shortage a
	set qty_shortage=a.qty_shortage-b.qty_tomove--,qty_gap=a.qty_gap-b.qty_tomove
	from tmp_tomove b
	where a.dt=b.dt and a.factory_code=b.factory_code and a.linetype=b.linetype
	;
--	update tmp_shortage a
--	set --qty_shortage=a.qty_shortage-b.qty_tomove,
--		qty_gap=a.qty_gap-b.qty_tomove
--	from tmp_tomove b
--	where a.dt=b.dt and a.factory_code=b.factory_code-- and a.linetype=b.linetype
--	;
end if;

    end loop;

#-- 4类6个分支找完后，如果当月还有缺口，则转移到上个月
sql="""truncate table tmp_shortage_add;
insert into tmp_shortage_add
select factory_code, linetype, dt-'1 month'::interval, null, qty_shortage
from tmp_shortage where qty_shortage>0
;--select * from tmp_shortage_add
	if do_month=start_date then
	update tenant_snowbeer_adm.advance_tomove_month a
	set qty_left=coalesce(b.qty_shortage,0)
	from tmp_shortage_add b
	where a.factory_code=b.factory_code and a.linetype=b.linetype
	;
	else
	insert into tenant_snowbeer_adm.advance_tomove_month(factory_code,linetype,dt,qty_tomove,batch_id)
	select factory_code,linetype ,dt ,qty_shortage,v_batch_id --select *
	from tmp_shortage_add
	;
	end if;
end if;
do_month=do_month-'1 month'::interval;

end loop;
insert into tenant_snowbeer_adm.advance_production_month
select *,v_batch_id --select *
from tmp_linetype_tomove;

--------------------------------------------------
-- 最后月当月的其他线可产不能算提前生产,不能从下月分配sku
drop table if exists tmp_max;
create temp table tmp_max as
select factory_code,linetype,max(dt) dt --select *
from tenant_snowbeer_adm.adm_factory_linetype_capacity_gap
where  batch_id=v_batch_id and qty_shortage>0 --and factory_code='HBA'
group by 1,2
;