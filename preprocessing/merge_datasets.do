cd R:/JoePriceResearch/record_linking

/* Create random subsample
use projects/deep_learning/ml-record-linking/data/training_pairs, clear
pause on
//Use Webb County, Texas as a prediction sample
//FIXME add compress periodically to code
//Select random subsample
duplicates drop ark1910, force
gen rand_sample = runiform()
sort rand_sample
keep in 1/5000
drop ark1920 index* ismatch rand_sample
merge 1:m ark1910 using projects/deep_learning/ml-record-linking/data/training_pairs, keep(3) nogen
*/

use projects/deep_learning/data/prediction_sets/test2 in 1/10000000, clear
ren y ismatch

//Merge in censuses
ren index1910 index
merge m:1 index using data/census_compact/1910/census1910, keep(3) nogen
ren (*) (*1910)
ren (ismatch1910 ark19101910 ark19201910 index19201910) (ismatch ark1910 ark1920 index1920)

ren index1920 index
merge m:1 index using data/census_compact/1920/census1920, keep(3) nogen
ren (index marstat birth_year household immigration race rel female bp mbp fbp state midinit cohort1 cohort2 county last_sdxn first_sdxn first_init last_init) ///
	(index1920 marstat1920 birth_year1920 household1920 immigration1920 race1920 rel1920 female1920 bp1920 mbp1920 fbp1920 state1920 midinit1920 cohort11920 cohort21920 county1920 last_sdxn1920 first_sdxn1920 first_init1920 last_init1920)

//Merge in names
merge m:1 index1910 using data/census_compact/1910/stringnames1910, keep(3) nogen keepus(first1910 last1910)
merge m:1 index1920 using data/census_compact/1920/stringnames1920, keep(3) nogen keepus(first1920 last1920)
drop household* cohort* midinit*

//Merge in name commonality
ren first1910 pr_name_gn
merge m:1 pr_name_gn using data/census_1910/data/obj/names_crosswalk, keep(1 3) nogen //1,502 names w/o commonality
ren (pr_name_gn tot) (first1910 first_name_comm1910)
compress

ren first1920 pr_name_gn
merge m:1 pr_name_gn using data/census_1910/data/obj/names_crosswalk, keep(1 3) nogen //3,829 names w/o commonality
ren (pr_name_gn tot) (first1920 first_name_comm1920)
compress

ren last1910 pr_name_surn
merge m:1 pr_name_surn using data/census_1910/data/obj/surn_comm, keep(1 3) nogen //1,944 "   "
ren (pr_name_surn comm) (last1910 last_name_comm1910)
compress

ren last1920 pr_name_surn
merge m:1 pr_name_surn using data/census_1920/data/obj/surn_comm, keep(1 3) nogen //7,236 "   "
ren (pr_name_surn comm) (last1920 last_name_comm1920)
compress

//Merge in geo distances
//FIXME add county lat lon R:/JoePriceResearch/record_linking/data/crosswalks/county_lat_lon_all
ren index1910 index
merge m:1 index using data/census_compact/1910/place1910, keep(3) nogen
ren (index place) (index1910 event_place)
merge m:1 event_place using data/crosswalks/event_lat_lon_1910, keep(1 3) keepus(lat lon) nogen //3,830
replace lat = 38.904722 if regexm(event_place, "Washington, District of Columbia, United States")
replace lon = -77.016389 if regexm(event_place, "Washington, District of Columbia, United States")
replace lat = 37.533333 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
replace lon = -77.466667 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
ren (lat lon) (event_lat1910 event_lon1910)
ren (event_place index1920) (place1910 index)
compress

merge m:1 index using data/census_compact/1920/place1920, keep(3) nogen
ren (index place) (index1920 event_place)
merge m:1 event_place using data/crosswalks/event_lat_lon_1920, keep(1 3) keepus(lat lon) nogen //6,154
replace lat = 38.904722 if regexm(event_place, "Washington, District of Columbia, United States")
replace lon = -77.016389 if regexm(event_place, "Washington, District of Columbia, United States")
replace lat = 37.533333 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
replace lon = -77.466667 if regexm(event_place, "Richmond (Independent City), Virginia, United States")
ren (lat lon) (event_lat1920 event_lon1920)
ren event_place place1920
compress

save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/dict_place_group, clear
drop township group state
duplicates drop county_state, force
ren county_state county1910
merge 1:m county1910 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
ren county county_string1910
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
ren (int_state state) (state1910 state_string1910)
merge 1:m state1910 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
save projects/deep_learning/ml-record-linking/data/temp, replace

use data/census_compact/dictionaries/state_dict, clear
drop if strlen(state) == 2
drop if state == "New Mexico Territory" | state == "Hawaii Territory" | state == "Consular Service" | state == "Indian Territory"
ren (int_state state) (state1920 state_string1920)
merge 1:m state1920 using projects/deep_learning/ml-record-linking/data/temp, keep(2 3) nogen
save projects/deep_learning/ml-record-linking/data/temp, replace

//Update lat lon if missing with county lat lon
ren (county_string1910 state_string1910) (county state)
merge m:1 county state using data/crosswalks/county_lat_lon_all, keep(1 3) nogen
replace event_lat1910 = latitude if event_lat1910 == .
replace event_lon1910 = longitude if event_lon1910 == .
drop latitude longitude
ren (county state county_string1920 state_string1920) (county_string1910 state_string1910 county state)
merge m:1 county state using data/crosswalks/county_lat_lon_all, keep(1 3) nogen
replace event_lat1920 = latitude if event_lat1920 == .
replace event_lon1920 = longitude if event_lon1920 == .
drop latitude longitude
ren (county state) (county_string1920 state_string1920)

ren bp1910 int_place
merge m:1 int_place using R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/merrill_bplace_comm1910, keep(1 3) keepus(comm) nogen
ren (int_place bp1920 comm) (bp1910 int_place bp_comm1910)
merge m:1 int_place using R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/merrill_bplace_comm1910, keep(1 3) keepus(comm) nogen
ren (int_place comm) (bp1920 bp_comm1920)
compress
/*
//now merge w/ matched_1910_1920
//FIXME merge on city county state. The cities do not match
merge m:1 county1910 state1910 using data/crosswalks/cities/matched_1910_1920, keep(1 3) keepus(pop1910)
pause
drop _merge
merge m:1 county1920 state1920 using data/crosswalks/cities/matched_1910_1920, keep(1 3) keepus(pop1920)
pause
drop _merge
*/
