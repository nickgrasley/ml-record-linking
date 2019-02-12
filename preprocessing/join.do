// Streamline cleaning between crosswalk and census

cd R:\JoePriceResearch\RA_work_folders\Neil_Duzett\New_folder
use matched_1910_1920.dta

rename county1910 county
joinby county using dict_place_group.dta

save county_joined.dta, replace

joinby state using state_dict.dta

save county_state_joined.dta
