cd R:/JoePriceResearch/record_linking

/* Create random subsample
use projects/deep_learning/ml-record-linking/data/training_pairs, clear
pause on
//Use Webb County, Texas as a prediction sample
//FIXME add compress periodically to code
//Select random subsample
duplicates drop ark1900, force
gen rand_sample = runiform()
sort rand_sample
keep in 1/5000
drop ark1920 index* ismatch rand_sample
merge 1:m ark1900 using projects/deep_learning/ml-record-linking/data/training_pairs, keep(3) nogen
*/
args candidate_pairs_file out_file bplace_geo

use `candidate_pairs_file', clear
cap ren islinked ismatch

//Merge in censuses
ren index1900 index
merge m:1 index using data/census_compact/1900/census1900, keep(3) nogen
ren (*) (*1900)
*ren (ark19001900 ark19201900 index19201900) (ark1900 ark1920 index1920)
ren index19201900 index1920
cap ren ismatch1900 ismatch

ren index1920 index
merge m:1 index using data/census_compact/1920/census1920, keep(3) nogen
ren (index marstat birth_year household immigration race rel female bp mbp fbp state midinit cohort1 cohort2 county last_sdxn first_sdxn first_init last_init) ///
	(index1920 marstat1920 birth_year1920 household1920 immigration1920 race1920 rel1920 female1920 bp1920 mbp1920 fbp1920 state1920 midinit1920 cohort11920 cohort21920 county1920 last_sdxn1920 first_sdxn1920 first_init1920 last_init1920)

//Merge in names
merge m:1 index1900 using data/census_compact/1900/stringnames1900, keep(3) nogen keepus(first1900 last1900)
merge m:1 index1920 using data/census_compact/1920/stringnames1920, keep(3) nogen keepus(first1920 last1920)
drop household* cohort* midinit*

//Merge in name commonality
ren first1900 pr_name_gn
merge m:1 pr_name_gn using data/census_1900/data/obj/names_crosswalk, keep(1 3) nogen //1,502 names w/o commonality
ren (pr_name_gn tot) (first1900 first_name_comm1900)
compress

ren first1920 pr_name_gn
merge m:1 pr_name_gn using data/census_1900/data/obj/names_crosswalk, keep(1 3) nogen //3,829 names w/o commonality
ren (pr_name_gn tot) (first1920 first_name_comm1920)
compress

ren last1900 pr_name_surn
merge m:1 pr_name_surn using data/census_1900/data/obj/surn_comm, keep(1 3) nogen //1,944 "   "
ren (pr_name_surn comm) (last1900 last_name_comm1900)
compress

ren last1920 pr_name_surn
merge m:1 pr_name_surn using data/census_1920/data/obj/surn_comm, keep(1 3) nogen //7,236 "   "
ren (pr_name_surn comm) (last1920 last_name_comm1920)
compress

//Merge in geo distances
//FIXME add county lat lon R:/JoePriceResearch/record_linking/data/crosswalks/county_lat_lon_all
ren index1900 index
merge m:1 index using data/census_compact/1900/place1900_v2, keep(3) nogen
ren (index township) (index1900 city)

merge m:1 city county state using data/crosswalks/census_towns_coor_v6_small.dta, keep(1 3) keepus(lat lon) nogen //3,830
/*
replace lat = 38.904722 if regexm(event_place, "Washington, District of Columbia, United States")
replace lon = -77.016389 if regexm(event_place, "Washington, District of Columbia, United States")
replace lat = 37.533333 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
replace lon = -77.466667 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
*/
ren (lat lon) (event_lat1900 event_lon1900)
ren (city county state index1920) (city_string1900 county_string1900 state_string1900 index)

merge m:1 index using data/census_compact/1920/places1920_v2, keep(3) nogen
ren (index township) (index1920 city)
merge m:1 city county state using data/crosswalks/census_towns_coor_v6_small, keep(1 3) keepus(lat lon) nogen //6,154
/*
replace lat = 38.904722 if regexm(event_place, "Washington, District of Columbia, United States")
replace lon = -77.016389 if regexm(event_place, "Washington, District of Columbia, United States")
replace lat = 37.533333 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
replace lon = -77.466667 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
*/
ren (lat lon) (event_lat1920 event_lon1920)
ren (city county state) (city_string1920 county_string1920 state_string1920)
compress

/*
save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/dict_place_group, clear
drop township group state
duplicates drop county_state, force
ren county_state county1900
merge 1:m county1900 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
ren county county_string1900
save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/dict_place_group, clear
drop township group state
duplicates drop county_state, force
ren county_state county1920
merge 1:m county1920 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
ren county county_string1920
save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/state_dict, clear
drop if strlen(state) == 2
drop if state == "New Mexico Territory" | state == "Hawaii Territory" | state == "Consular Service" | state == "Indian Territory"
ren (int_state state) (state1900 state_string1900)
merge 1:m state1900 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/state_dict, clear
drop if strlen(state) == 2
drop if state == "New Mexico Territory" | state == "Hawaii Territory" | state == "Consular Service" | state == "Indian Territory"
ren (int_state state) (state1920 state_string1920)
merge 1:m state1920 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
save projects/deep_learning/ml-record-linking/data/temp, replace
*/

//Update lat lon if missing with county lat lon
ren (county_string1900 state_string1900) (county state)
merge m:1 county state using data/crosswalks/county_lat_lon_all, keep(1 3) nogen
replace event_lat1900 = latitude if event_lat1900 == .
replace event_lon1900 = longitude if event_lon1900 == .
drop latitude longitude
ren (county state county_string1920 state_string1920) (county_string1900 state_string1900 county state)
merge m:1 county state using data/crosswalks/county_lat_lon_all, keep(1 3) nogen
replace event_lat1920 = latitude if event_lat1920 == .
replace event_lon1920 = longitude if event_lon1920 == .
drop latitude longitude
ren (county state) (county_string1920 state_string1920)

ren bp1900 int_place
merge m:1 int_place using R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/merrill_bplace_comm1900, keep(1 3) keepus(comm) nogen
ren (int_place bp1920 comm) (bp1900 int_place bp_comm1900)
merge m:1 int_place using R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/merrill_bplace_comm1900, keep(1 3) keepus(comm) nogen
ren (int_place comm) (bp1920 bp_comm1920)
compress

//There's a branch cut  for longitude in the Pacific Ocean (i.e. -180, 180 are the same spot),
//but it's in the middle of nowhere. It's hopefully not a problem, but keep that in mind.
/*
if "`bplace_geo'" == "True" {
	ren bp1900 int_place
	merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
	ren (int_place bp1920 lat lon) (bp1900 int_place bplace_lat1900 bplace_lon1900)
	merge m:1 int_place using data/census_compact/dictionaries/bplace_lat_lon, keep(1 3) nogen
	ren (int_place lat lon) (bp1920 bplace_lat1920 bplace_lon1920)
}
*/
/*
//now merge w/ matched_1900_1920
//FIXME merge on city county state. The cities do not match
merge m:1 county1900 state1900 using data/crosswalks/cities/matched_1900_1920, keep(1 3) keepus(pop1900)
pause
drop _merge
merge m:1 county1920 state1920 using data/crosswalks/cities/matched_1900_1920, keep(1 3) keepus(pop1920)
pause
drop _merge
*/
/*
if "`bplace_geo'" == "True" {
	order index1900 ark1900 marstat1900 birth_year1900 immigration1900 race1900 rel1900 ///
		female1900 mbp1900 fbp1900 first_sdxn1900 last_sdxn1900 first_init1900 last_init1900 ///
		first1900 last1900 first_name_comm1900 last_name_comm1900 event_lat1900 event_lon1900 ///
		county1900 state1900 bp1900 bp_comm1900 bplace_lat1900 bplace_lon1900 index1920 ark1920 marstat1920 birth_year1920 ///
		immigration1920 race1920 rel1920 female1920 mbp1920 fbp1920 first_sdxn1920 last_sdxn1920 ///
		first_init1920 last_init1920 first1920 last1920 first_name_comm1920 last_name_comm1920 ///
		event_lat1920 event_lon1920 county1920 state1920 bp1920 bp_comm1920 bplace_lat1920 bplace_lon1920
}
else {
	order index1900 ark1900 marstat1900 birth_year1900 immigration1900 race1900 rel1900 ///
		female1900 mbp1900 fbp1900 first_sdxn1900 last_sdxn1900 first_init1900 last_init1900 ///
		first1900 last1900 first_name_comm1900 last_name_comm1900 event_lat1900 event_lon1900 ///
		county1900 state1900 bp1900 bp_comm1900 index1920 ark1920 marstat1920 birth_year1920 ///
		immigration1920 race1920 rel1920 female1920 mbp1920 fbp1920 first_sdxn1920 last_sdxn1920 ///
		first_init1920 last_init1920 first1920 last1920 first_name_comm1920 last_name_comm1920 ///
		event_lat1920 event_lon1920 county1920 state1920 bp1920 bp_comm1920
}
*/
drop state_string1920 state_string1900 county_string1920 county_string1900
save `out_file', replace
exit, STATA clear
