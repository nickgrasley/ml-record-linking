declare @batch_num varchar(3)
declare @next_batch_num varchar(3)
declare @year0 varchar(4)
declare @year1 varchar(4)
set @batch_num = '0'
set @next_batch_num = '1'
set @year0 = '1900'
set @year1 = '1920'

exec(
'
select * 
into [Price].[dbo].[training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num +
'] from Price.dbo.compact' + @year0 + '_census as ca
where index_' + @year0 + ' >= ' + @batch_num + '0000000 and index_' + @year0 + ' < ' + @next_batch_num + '0000000;
alter table Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + '
alter column index_' + @year0 + ' int not null;
/* Duplicate index deletion (if needed)
WITH CTE AS(
   SELECT *,
       RN = ROW_NUMBER()OVER(PARTITION BY index_' + @year0 + ' ORDER BY index_' + @year0 + ')
   FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + '
)
DELETE FROM CTE WHERE RN > 1
*/
alter table Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + '
add constraint PK_training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' primary key (index_' + @year0 + ');
select index_' + @year0 + ', index_' + @year1 + '
into Price.dbo.training_indices_' + @year0 + '_' + @year1 + '_' + @batch_num + '
from Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + '
left join Price.dbo.compact' + @year1 + '_census on Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + '.index_' + @year0 + ' = Price.dbo.compact' + @year1 + '_census.index_' + @year1 + '
where 1 = 2;
alter table Price.dbo.training_indices_' + @year0 + '_' + @year1 + '_' + @batch_num + '
alter column index_' + @year1 + ' int not null;
alter table Price.dbo.training_indices_' + @year0 + '_' + @year1 + '_' + @batch_num + '
add constraint PK_training_indices_' + @year0 + '_' + @year1 + '_' + @batch_num + ' primary key (index_' + @year0 + ', index_' + @year1 + ');
insert into Price.dbo.training_indices_' + @year0 + '_' + @year1 + '_' + @batch_num + '
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '   and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '    and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '    and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.last_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
/* Testing bins */
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_init_' + @year0 + '  = xgb2.last_init_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.fbp_' + @year0 + '        = xgb2.fbp_' + @year1 + '        and
											xgb1.mbp_' + @year0 + '        = xgb2.mbp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.training_temp_' + @year0 + '_' + @year1 + '_' + @batch_num + ' as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_init_' + @year0 + '  = xgb2.last_init_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.fbp_' + @year0 + '        = xgb2.fbp_' + @year1 + '        and
											xgb1.mbp_' + @year0 + '        = xgb2.mbp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL'
)