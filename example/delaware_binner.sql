declare @year0 varchar(4)
declare @year1 varchar(4)
set @year0 = '1910'
set @year1 = '1920'

exec(
'
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '   and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '    and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '    and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.last_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and 
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.state_' + @year0 + '      = xgb2.state_' + @year1 + '      and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_init_' + @year0 + ' = xgb2.first_init_' + @year1 + ' and
											xgb1.last_sdxn_' + @year0 + '  = xgb2.last_sdxn_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
/* Testing bins */
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort2_' + @year0 + '    = xgb2.cohort2_' + @year1 + '    and  
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_init_' + @year0 + '  = xgb2.last_init_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.fbp_' + @year0 + '        = xgb2.fbp_' + @year1 + '        and
											xgb1.mbp_' + @year0 + '        = xgb2.mbp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48
UNION
SELECT index_' + @year0 + ', index_' + @year1 + '
FROM Price.dbo.compact' + @year0 + '_census as xgb1
LEFT JOIN Price.dbo.compact' + @year1 + '_census as xgb2 on     xgb1.cohort1_' + @year0 + '    = xgb2.cohort1_' + @year1 + '    and  
											xgb1.county_' + @year0 + '     = xgb2.county_' + @year1 + '     and 
											xgb1.female_' + @year0 + '     = xgb2.female_' + @year1 + '     and 
											xgb1.first_sdxn_' + @year0 + ' = xgb2.first_sdxn_' + @year1 + ' and
											xgb1.last_init_' + @year0 + '  = xgb2.last_init_' + @year1 + '  and
											xgb1.bp_' + @year0 + '         = xgb2.bp_' + @year1 + '         and
											xgb1.race_' + @year0 + '       = xgb2.race_' + @year1 + '       and
											xgb1.fbp_' + @year0 + '        = xgb2.fbp_' + @year1 + '        and
											xgb1.mbp_' + @year0 + '        = xgb2.mbp_' + @year1 + '
WHERE xgb2.index_' + @year1 + ' IS NOT NULL and xgb1.state = 48 and xgb2.state = 48'
)