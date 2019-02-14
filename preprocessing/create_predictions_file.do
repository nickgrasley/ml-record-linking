args predictions_file year1 year2 out_file no_arks ark_file
local pid_ark R:\JoePriceResearch\record_linking\data\census_tree\pid_ark

import delim using `predictions_file', clear
if no_arks {
	merge 1:1 index`year1' index`year2' using `ark_file', keep(3) nogen
}
//FIXME add erase function of `predictions_file'
save `out_file', replace

foreach year of local year1, year2 {
	use `pid_ark' if year == `year', clear
	gsort + ark - attached
	drop if ark[_n] == ark[_n - 1]
	drop attached year
	ren (*) (*`year')
	merge 1:m ark`year' using `out_file', keep(2 3) nogen
	save `out_file', replace
}

replace pid`year1' = pid`year2' if pid`year1' == ""
drop pid`year2'
ren pid`year1' pid
save `out_file', replace
