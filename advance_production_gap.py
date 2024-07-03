"""
根据平衡产能重算各产线组（linetype）的产能
输入：产线组产能tmp_linetype_capacity、产线组 todotmp_linetype_todo、工厂产能tmp_factory_capacity
输出：新的产线组产能及产能与 todo 的 gap
"""
#%%
import decimal
def prepare_data():
    # 获取所有产线组产能、工厂剩余产能、todo
    # --循环前准备
	# -- 产线组产能
    sql="""drop table if exists tmp_linetype_capacity;
create table tmp_linetype_capacity as
select a.factory_code,a.linetype,a.dt,b.linetype_rn as linetype_rn,capacity capacity
from advance_linetype_capacity a
left join advance_linetype_rn b on a.linetype=b.linetype and a.factory_code=b.factory_code
;"""
    myduck.ducksql(sql)
#-- 产线组产能 & todo
    sql="""
drop table if exists tmp_linetype_capacity_todo;
create table tmp_linetype_capacity_todo as
select a.factory_code,a.linetype,a.dt,a.linetype_rn,
a.capacity line_capacity,
b.todo_qty todo_qty
from tmp_linetype_capacity a
left join adm_factory_linetype_todo b on a.factory_code=b.factory_code and a.linetype=b.linetype and a.dt=b.dt
;"""
    myduck.ducksql(sql)
# 工厂产能，拉链展开到月    
    sql="""
drop table if exists tmp_factory_capacity;
create table tmp_factory_capacity as
select a.factory_code,b.dt,a.equilibrium_capacity as factory_capacity
from adm_factory_capacity a 
inner join adm_plan_config b on b.dt>=a.dt_start and b.dt<a.dt_end
;"""
    myduck.ducksql(sql)
       
    # 产线顺序
    sql="""
"""
#%%    
def rst_table(): 
    sql="""drop table if exists tmp_assigned_capacity;
create table tmp_assigned_capacity(
factory_code text,linetype text,dt date,capacity numeric, assigned_capacity numeric)
"""
    myduck.ducksql(sql)

#%%  
def rebanlance_capacity_month(factory_code,do_month):
    # 计算每个产线组的产能
    sql=f"select factory_capacity from tmp_factory_capacity where factory_code='{factory_code}' and dt='{do_month}'"
    ic(sql)
    # 当月初始工厂产能
    factory_capacity_left=decimal.Decimal((myduck.dd.execute(sql).fetchone())[0])
        
    sql=f"from tmp_linetype_capacity_todo where factory_code='{factory_code}' and dt='{do_month}' order by linetype_rn desc;"
    # 循环每条产线
    for r in myduck.dd.execute(sql).fetchall():
        linetype_capacity=r[4]
        linetype_todo=r[5]
        ic(linetype_capacity,linetype_todo,factory_capacity_left)   
        if not linetype_todo:
            continue
        assigned,factory_capacity_left,pure_capacity_left,linetype_todo_left=rebanlance_capacity_linetype(r[1],linetype_capacity, factory_capacity_left,linetype_todo)
        #ic(assigned,factory_capacity_left,pure_capacity_left,linetype_todo_left)

        # 插入到产能分配表
        sql=f"""insert into tmp_assigned_capacity
select '{r[0]}', '{r[1]}', '{do_month}',{linetype_capacity},{assigned}
"""
        myduck.ducksql(sql)
        # 剩余的 todo 挪到上个月
        if linetype_todo_left>0:
            sql=f"""update tmp_linetype_capacity_todo set todo_qty=todo_qty+{linetype_todo_left} 
	where factory_code='{r[0]}' and linetype='{r[1]}' and dt='{do_month}'::date-'1 month'::interval"""
            myduck.ducksql(sql)
#%%           
def rebanlance_capacity_linetype(linetype,linetype_capacity, factory_capacity_left,linetype_todo,pure_capacity_left=0):
    # 计算每个产线的产能，工厂剩余产能、产线产能、todo 取小
    assigned=min(linetype_capacity, factory_capacity_left,linetype_todo)
    # 如果是纯生线，还需计算剩余的产线产能
    if linetype in('纯生-听','纯生-瓶'):
        pure_capacity_left=linetype_capacity-assigned #剩余的纯生产能 瓶 bottle 听 can
    # 扣减工厂产能和剩余的 todo   
    factory_capacity_left=factory_capacity_left-assigned
    linetype_todo_left=linetype_todo-assigned

    # 如果是普通瓶听线，还可以将剩余的对应纯生线再分配一次
    if linetype in('普通-听','普通-瓶') and linetype_todo_left>0 and pure_capacity_left>0:
        assigned=assigned+min(pure_capacity_left, factory_capacity_left,linetype_todo)
        pure_capacity_left=0
        linetype_todo_left=linetype_todo-assigned
    return assigned,factory_capacity_left,pure_capacity_left,linetype_todo_left
#%%
def rebanlance_capacity_factory(factory_code):
    sql=f"select dt from adm_plan_config order by dt desc"
    for r in myduck.dd.execute(sql).fetchall():
        do_month=r[0]
        ic(do_month)
        rebanlance_capacity_month(factory_code,do_month)
#%%
def rebanlance_capacity():
    # 计算每个工厂的产能
    for r in myduck.dd.execute("select distinct factory_code from adm_factory_capacity order by 1").fetchall():
        rebanlance_capacity_factory(r[0])   


#%%
if __name__ == '__main__':
    from dbtools import *
    from icecream import ic
    from datetime import datetime
    #ic(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    t=time.time()
    ducksql=myduck.ducksql
    print(myduck)
    #prepare_data()

    rst_table()

    rebanlance_capacity()
    # 单工厂测试
    # rebanlance_capacity_factory('XDF')   
    # 查看结果
    # myduck.dd.sql("select * from tmp_assigned_capacity where factory_code='XDF' order by dt desc,linetype")
    ic.disable()
    print("total cost:",round(time.time()-t,2))


