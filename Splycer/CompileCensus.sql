use Price
go;

declare @year varchar(4)
declare @chunk_start varchar(3)
declare @chunk_end varchar(3)
set @year = '1930'
set @chunk_start = '0'
set @chunk_end = '1'

/* Run this first exec() only once. Comment it out for later chunks */
exec(
'
create table(
index_'       + @year + ' int not null primary key,
marstat_'     + @year + ' smallint,
birth_year_'  + @year + ' smallint,
household_'   + @year + ' int,
immigration_' + @year + ' smallint,
race_'        + @year + ' smallint,
rel_'         + @year + ' smallint,
female_'      + @year + ' smallint,
bp_'          + @year + ' smalllint,
mbp_'         + @year + ' smallint,
fbp_'         + @year + ' smallint,
state_'       + @year + ' smallint,
county_'      + @year + ' smallint,
cohort1_'     + @year + ' smallint,
cohort2_'     + @year + ' smallint,
last_sdxn_'   + @year + ' smallint,
first_sdxn_'  + @year + ' smallint,
last_init_'   + @year + ' smallint,
first_init_'  + @year + ' smallint,
bp_comm_'     + @year + ' int,
first_comm_'  + @year + ' float,
last_comm_'   + @year + ' float,
first_'       + @year + ' nvarchar(50),
last_'        + @year + ' nvarchar(90),
bp_lat_'      + @year + ' float,
bp_lon_'      + @year + ' float,
res_lat_'     + @year + ' float,
res_lon_'     + @year + ' float,
full_name_'   + @year + ' nvarchar(50)
)
')

exec(
'
insert into compiled_' + @year + '
select census.index_' + @year + ', census.marstat_' + @year + ', census.birth_year_' + @year + ', census.household_' + @year + 
      ', census.immigration_' + @year + ', census.race_' + @year + ', census.rel_' + @year + ', census.female_' + @year + 
	  ', census.bp_' + @year + ', census.mbp_' + @year + ', census.fbp_' + @year + ', census.state_' + @year + 
	  ', census.county_' + @year + ', census.cohort1_' + @year + ', census.cohort2_' + @year + ', census.last_sdxn_' + @year + 
	  ', census.first_sdxn_' + @year + ', census.last_init_' + @year + ', census.first_init_' + @year +
      'bp_comm.bp_comm_1910 as bp_comm_' + @year + ', f_comm.first_comm_' + @year + ', l_comm.last_comm_' + @year + 
	  ', snames.first_' + @year + ', snames.last_' + @year + ', g_bp.bp_lat as bp_lat_' + @year + ', g_bp.bp_lon as bp_lon_' + @year + 
	  'gr.res_lat as res_lat_' + @year + 'gr.res_lon as res_lon_' + @year + ', nicknames.full_name as full_name_' + @year +' 

from compact' + @year + '_census as census
left join compact' + @year + '_stringnames as snames on census.index_' + @year + ' = snames.index_' + @year + ' 
left join compact' + @year + '_place as place on census.index_' + @year + ' = place.index_' + @year + '
left join compact' + @year + '_first_comm as f_comm on snames.first_' + @year + ' = f_comm.first_' + @year '
left join compact' + @year + '_last_comm  as l_comm on snames.last_'  + @year + ' = l_comm.last_'  + @year '
left join crosswalk_geo_bplace as g_bp on census.bp_' + @year + ' = g_bp.int_place
left join crosswalk_geo_res as gr on place.township = gr.city and place.county = gr.county and place.state = gr.state
left join crosswalk_nickname as nickname on snames.first_' + @year + ' = nickname.nickname
left join compact1910_bplace_comm as bp_comm on census.bp_' + @year + ' = bp_comm.int_place
where census.index_' + @year + ' >= ' + @chunk_start + '0000000 and census.index_' + @year + ' < ' + @chunk_end + '0000000
')