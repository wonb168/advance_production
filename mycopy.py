# %%
import psycopg
dsn_src="postgresql://gpadmin:oFkRt1CkVk5fJ6R8@10.1.11.62:2345/mdmaster_fastfish_dev"
dsn_tgt="postgresql://gpadmin:oFkRt1CkVk5fJ6R8@127.0.0.1:5432/mdmaster_fastfish_dev"  
#%%
%%time
# 单表copy
t="tenant_lansheng5_biz.rst_ra_skc_org_detail"
sql_copy=f"COPY (select * from {t} where day_date='2024-03-21') TO STDOUT (FORMAT BINARY)"
print(sql_copy)
with psycopg.connect(dsn_src) as conn1, psycopg.connect(dsn_tgt) as conn2:
    with conn1.cursor().copy(sql_copy) as copy1:
        with conn2.cursor().copy(f"COPY {t} FROM STDIN (FORMAT BINARY)") as copy2:
            for data in copy1:
                copy2.write(data)
# %%
