clear all
cd R:/JoePriceResearch/record_linking
import delim using projects/deep_learning/ml-record-linking/approx_nearest_neighbor/weight_training_arks_1920.csv
gen num = _n
merge m:1 ark1920 using data/census_compact/1920/ind_ark1920, keep(1 3) nogen 
merge m:1 index1920 using data/census_compact/1920/census1920, keep(1 3) nogen

merge m:1 index1920 using data/census_compact/1920/stringnames1920, keep(1 3) nogen keepus(first1920 last1920)
ren first1920 pr_name_gn
merge m:1 pr_name_gn using projects/fixing_lastnames/firstnametovec, keep(1 3) nogen
ren v* first_vec*
ren last1920 pr_name_surn
merge m:1 pr_name_surn using projects/fixing_lastnames/lastnametovec, keep(1 3) nogen
ren v* last_vec*
drop pr_name_gn pr_name_surn

ren (index1920 county state) (index county_int state_int)
merge m:1 index using data/census_compact/1920/places1920_v2, keep(1 3) nogen
ren (index township) (index1920 city)

merge m:1 city county state using data/crosswalks/census_towns_coor_v6_small, keep(1 3) keepus(lat lon) nogen
ren (lat lon) (event_lat event_lon)
drop city county state

ren bp int_place
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (lat lon) (bplace_lat bplace_lon)

ren (int_place mbp) (bp int_place)
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (lat lon) (mbp_lat mbp_lon)

ren (int_place fbp) (mbp int_place)
merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
ren (int_place lat lon) (fbp fbp_lat fbp_lon)

save projects/deep_learning/ml-record-linking/approx_nearest_neighbor/training_weights_1920, replace
