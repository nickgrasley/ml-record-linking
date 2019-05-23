cd R:/JoePriceResearch/record_linking
use data/census_compact/1910/census1910 if state == 31

merge m:1 index1910 using data/census_compact/1910/stringnames1910, keep(3) nogen keepus(first1910 last1910)
ren first1910 pr_name_gn
merge m:1 pr_name_gn using projects/fixing_lastnames/firstnametovec, keep(3) nogen
ren v* first_vec*
ren last1910 pr_name_surn
merge m:1 pr_name_surn using projects/fixing_lastnames/lastnametovec, keep(3) nogen
ren v* last_vec*

ren (index1910 county state) (index county_int state_int)
merge m:1 index using data/census_compact/1910/place1910_v2, keep(3) nogen
ren (index township) (index1910 city)

merge m:1 city county state using data/crosswalks/census_towns_coor_v6_small, keep(1 3) keepus(lat lon) nogen
ren (lat lon) (event_lat event_lon)

ren bp int_place
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (lat lon int_place) (bplace_lat bplace_lon bp)

ren mbp int_place
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (lat lon int_place) (mbp_lat mbp_lon mbp)

ren fbp int_place
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (lat lon int_place) (fbp_lat fbp_lon fbp)
