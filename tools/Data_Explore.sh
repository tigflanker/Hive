#!/bin/bash
#Author:tigflanker
#Date:27Jun2020
# 功能：用于统计给定表指定数值型变量的描述统计（饱和度、0值率、最大最小值、均值标准差、Q1Q2Q3分位数）
# 可以使用order=‘echo’，直接打印出SQL语句，便于调试

order="echo"  # 打印命令或直接执行，可选："echo" or "hive -e "
para=""       # 是否并行执行语句，可选："" or "wait"。wait为串行，""为并行
dbs="dev"     # 临时库名
vars=sex,age,shangcheng_amt,shangcheng_cnt  # 需要统计的数值型变量
table="database.example_table where dt = '2020-05-01'"  #  取数表和条件
prefix=tig_202005  # 临时表前缀

# 最终输出表：${prefix}_stats_final_combin_result

vars_map=`echo $vars | sed "s/\(\w\+\)/'\1',\1/g"`
vars_cnt=`echo $vars | sed 's/\(\w\+\)/count(\1) as \1/g'`
vars_cnt0=`echo $vars | sed 's/\(\w\+\)/sum(int(\1 = 0)) as \1/g'`
vars_mm=`echo $vars | sed 's/\(\w\+\)/array(round(min(float(\1)), 4), round(max(float(\1)), 4)) as \1/g'`
vars_as=`echo $vars | sed 's/\(\w\+\)/array(round(avg(\1), 4), round(std(\1), 4)) as \1/g'`
vars_pct=`echo $vars | sed 's/\(\w\+\)/percentile_approx(\1,array(0.25,0.5,0.75)) as \1/g'`

stats_cnt="
use ${dbs};
drop table if exists ${prefix}_stats_cnt_temp_table;
create table if not exists ${prefix}_stats_cnt_temp_table as 

select vars, counts
  from (select count(1) as dataset_cnt, ${vars_cnt} from ${table}) x 
lateral view outer 
explode(map('dataset_cnt', dataset_cnt, ${vars_map})) u as vars, counts
;
"

stats_cnt0="
use ${dbs};
drop table if exists ${prefix}_stats_cnt0_temp_table;
create table if not exists ${prefix}_stats_cnt0_temp_table as 

select vars, cnt0 
  from (select ${vars_cnt0} from ${table}) x 
lateral view outer 
explode(map(${vars_map})) u as vars, cnt0
;
"

stats_mm="
use ${dbs};
drop table if exists ${prefix}_stats_mm_temp_table;
create table if not exists ${prefix}_stats_mm_temp_table as 

select vars, min_max 
  from (select ${vars_mm} from ${table}) x 
lateral view outer 
explode(map(${vars_map})) u as vars, min_max
;
"

stats_as="
use ${dbs};
drop table if exists ${prefix}_stats_as_temp_table;
create table if not exists ${prefix}_stats_as_temp_table as 

select vars, avg_std 
  from (select ${vars_as} from ${table}) x 
lateral view outer 
explode(map(${vars_map})) u as vars, avg_std
;
"

stats_pct="
use ${dbs};
drop table if exists ${prefix}_stats_pct_temp_table;
create table if not exists ${prefix}_stats_pct_temp_table as 
 
select vars, pct_q 
  from (select ${vars_pct} from ${table}) x 
lateral view outer 
explode(map(${vars_map})) u as vars, pct_q
;
"

combin="
use ${dbs};
drop table if exists ${prefix}_stats_final_combin_result;
create table if not exists ${prefix}_stats_final_combin_result as 

select cnt.vars,
       counts as stats_cnt,
       counts / max(if(cnt.vars = 'dataset_cnt', counts, 0)) over() as stats_cnt_rate,
       cnt0 as stats_cnt0,
       cnt0 / max(if(cnt.vars = 'dataset_cnt', counts, 0)) over() as stats_cnt0_rate,
       min_max[0] as stats_min,
       min_max[1] as stats_max,
       avg_std[0] as stats_avg,
       avg_std[1] as stats_std,
       pct_q[0] as stats_q1,
       pct_q[1] as stats_q2,
       pct_q[2] as stats_q3

  from ${prefix}_stats_cnt_temp_table cnt
  left join ${prefix}_stats_cnt0_temp_table cnt0
    on cnt.vars = cnt0.vars
  left join ${prefix}_stats_mm_temp_table mm
    on cnt.vars = mm.vars
  left join ${prefix}_stats_as_temp_table avg
    on cnt.vars = avg.vars
  left join ${prefix}_stats_pct_temp_table pct
    on cnt.vars = pct.vars
;
"

$order "$stats_cnt" & $para
$order "$stats_cnt0" & $para  
$order "$stats_mm" & $para  
$order "$stats_as" & $para  
$order "$stats_pct" & $para  
$order "$combin" 




