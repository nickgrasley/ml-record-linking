cd R:/JoePriceResearch/record_linking

use projects/deep_learning/ml-record-linking/data/training_pairs, clear
pause on
//Use Webb County, Texas as a prediction sample
//FIXME add compress periodically to code
//Select random subsample
duplicates drop ark1910, force
gen rand_sample = runiform()
sort rand_sample
keep in 1/500000
drop ark1920 index* ismatch rand_sample
merge 1:m ark1910 using projects/deep_learning/ml-record-linking/data/training_pairs, keep(3) nogen


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
drop household* cohort* midinit

//Merge in name commonality
ren first1910 pr_name_gn
merge m:1 pr_name_gn using data/census_1910/data/obj/names_crosswalk, keep(1 3) //1,502 names w/o commonality
pause
drop _merge
ren (pr_name_gn tot) (first1910 first_name_comm1910)

ren first1920 pr_name_gn
merge m:1 pr_name_gn using data/census_1910/data/obj/names_crosswalk, keep(1 3) //3,829 names w/o commonality
pause
drop _merge
ren (pr_name_gn tot) (first1920 first_name_comm1920)

ren last1910 pr_name_surn
merge m:1 pr_name_surn using data/census_1910/data/obj/surn_comm, keep(1 3) //1,944 "   "
drop _merge
ren (pr_name_surn comm) (last1910 last_name_comm1910)

ren last1920 pr_name_surn
merge m:1 pr_name_surn using data/census_1920/data/obj/surn_comm, keep(1 3) //7,236 "   "
drop _merge
ren (pr_name_surn comm) (last1920 last_name_comm1920)

//Merge in geo distances
ren index1910 index
merge m:1 index using data/census_compact/1910/place1910, keep(3) nogen
ren (index place) (index1910 event_place)
merge m:1 event_place using data/crosswalks/event_lat_lon_1910, keep(1 3) keepus(lat lon) //3,830
ren (lat lon) (event_lat1910 event_lon1910)
pause
drop _merge
ren (event_place index1920) (place1910 index)

merge m:1 index using data/census_compact/1920/place1920, keep(3) nogen
ren (index place) (index1920 event_place)
merge m:1 event_place using data/crosswalks/event_lat_lon_1920, keep(1 3) keepus(lat lon) //6,154
ren (lat lon) (event_lat1920 event_lon1920)
pause
drop _merge
ren event_place place1920

//Merge in residence and bplace commonality
//commonality is in record_linking/data/crosswalks/cities/matched_1910_1920
//merge with dictionaries in record_linking/data/census_compact/dictionaries/state_dict and dict_place_group
ren county1910 county_state
merge m:1 county_state using data/census_compact/dictionaries/dict_place_group, keep(1 3) keepus(county)
pause
drop _merge
ren county_state county1910

ren county1920 county_state
merge m:1 county-state using data/census_compact/dictionaries/dict_place_group, keep(1 3) keepus(county)
pause
drop _merge
ren county_state county1920

ren state1910 int_state
merge m:1 int_state using data/census_compact/dictionaries/state_dict, keep(1 3) keepus(state) //FIXME merge is not unique for state_dict
pause
drop _merge
ren (int_state state) (int_state1910 state1910)

ren state1920 int_state
merge m:1 int_state using data/census_compact/dictionaries/state_dict, keep(1 3) keepus(state) //FIXME merge is not unique for state_dict
pause
drop _merge
ren (int_state state) (int_state1920 state1920)

//now merge w/ matched_1910_1920
merge m:1 county1910 state1910 using data/crosswalks/cities/matched_1910_1920, keep(1 3) keepus(pop1910)
pause
drop _merge
merge m:1 county1920 state1920 using data/crosswalks/cities/matched_1910_1920, keep(1 3) keepus(pop1920)
pause
drop _merge

ren bp int_place
merge m:1 R:/JoePriceResearch/RA_work_folders/Nicholas_Grasley/merrill_bplace_comm1910, keep(1 3) keepus(comm)
pause
drop _merge

//data/crosswalks/cities/city_1910_1920_crosswalk
