#%%
import sys
sys.version
# %%
# !python3.11 -m pip install jupysql duckdb-engine

# %%
%load_ext sql
# %%
# 访问duckdb
#%sql duckdb://
# %%
#%sql select version();
# %%
# 访问 pg
# dialect+driver://username:password@host:port/database UOwn_scp2023@$!
%sql postgresql://ur_0_uown_crb_edw_scp:UOwn_scp2023%40$!@10.207.64.41:2345/crb_edw_scp
# %%
%sql select version();
# %%
# 提前备货查询步骤
# 1. 产线组的生产量
%%sql
select * 
from tenant_snowbeer_adm.