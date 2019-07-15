clear all
set more off

R:\JoePriceResearch\record_linking\data\census_compact\1910
use event_township1910.dta
rename township city

split city, p(",")


replace city=city2 if missing(city4) & regexm(city3,"Ward")
replace city=city2 if missing(city4) & regexm(city3,"Area")
replace city=city2 if missing(city4) & regexm(city3,"Councilmanic")
replace city=city2 if missing(city4) & regexm(city3,"Tract")
replace city=city2 if missing(city4) & regexm(city3,"Voting District")
**/
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Election Precinct")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Election District")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Justice Precinct")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Township")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Town")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"City") & !regexm(city2,"County")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Magisterial District")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Militia District")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Beat")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Civil District")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Police Jury")
replace city=city2 if missing(city3) & !missing(city2) & regexm(city1,"Supervisorial District")
drop city1-city11


split city, p(")")

replace city2 = regexr(city2,"-[0-9]","")
replace city2 = "" if city2=="."
replace city2 = "" if city2=="(part)"
replace city=city2 if !missing(city2) & missing(city3) & !regexm(city2, "Precinct")
drop city1-city3
**/

replace city = regexr(city,"\(.*\)","")
replace city = regexr(city,"\)","")
replace city = regexr(city,"excl\.","")
replace city = regexr(city,"\(part","")


replace city = "New York" if regexm(city, "New York City")
replace city = "New York" if regexm(city, "Brooklyn")
replace city = "New York" if regexm(city, "Manhattan")
replace city = "New York" if regexm(city, "Queens")
replace city = "New York" if regexm(city, "Bronx")
replace city = "New York" if regexm(city, "Staten Island")
replace city = "New York" if regexm(city, "Richmond") & county=="Richmond"
replace city = "New Orleans" if regexm(city, "New Orleans")
replace city = "Cleveland" if regexm(city, "Cleveland")
replace city = "Memphis" if regexm(city, "Memphis")
replace city = "Philadelphia" if regexm(city, "Philadelphia")
replace city = "Toledo" if regexm(city, "Toledo")
replace city = "Columbus" if regexm(city, "Columbus")
replace city = "Youngstown" if regexm(city, "Youngstown")
replace city = "Cincinnati" if regexm(city, "Cincinnati")
replace city = "Springfield" if regexm(city, "Springfield")
replace city = "Newport" if regexm(city, "Newport")
replace city = "Sault Ste. Marie" if regexm(city, "Sault Ste.")
replace city = "Akron" if regexm(city, "Akron")
replace city = "Akron" if regexm(city, "Canton")
replace city = "Dayton" if regexm(city, "Dayton")
replace city = "Boston" if regexm(city, "Boston")
replace city = "Chicago" if regexm(city, "Chicago")
replace city = "Seattle" if regexm(city, "Seattle")
replace city = "Rochester" if regexm(city, "Rochester")
replace city = "Yonkers" if regexm(city, "Yonkers")
replace city = "Milwaukee" if regexm(city, "Milwaukee")
replace city = "Sacramento" if regexm(city, "Sacramento")
replace city = "Oakland" if regexm(city, "Oakland")
replace city = "San Diego" if regexm(city, "San Diego")
replace city = "San Francisco" if regexm(city, "San Francisco")
replace city = "Los Angeles" if regexm(city, "Los Angeles")
replace city = "Long Beach" if regexm(city, "Long Beach")
replace city = "San Antonio" if regexm(city, "San Antonio")
replace city = "Baltimore" if regexm(city, "Baltimore")
replace city = "St. Louis" if regexm(city, "St. Louis")
replace city = "Washington" if state=="District of Columbia"
replace county = "Washington" if state=="District of Columbia"


split city, p("&amp;")
replace city=city1 if !missing(city2)
drop city1-city5
**/

replace city = subinstr(city,"St.","St",.)
replace city = subinstr(city,"Mt.","Mt",.)
replace city = subinstr(city,"'","",.)
replace city = subinstr(city," city","",.)
replace city = regexr(city,"-[0-9][0-9]?","")
replace city = regexr(city, "Ward [A-Z0-9][0-9]? ","")
replace city = regexr(city, "Justice Precinct [A-Z0-9][0-9]? ","")
replace city = regexr(city, "Precincts? ?[A-Z0-9][0-9]? ","")
replace city = regexr(city, "Township [A-Z0-9][0-9]? ","")
replace city = regexr(city, "Election Districts? [A-Z0-9][0-9]?,? ","")
replace city = regexr(city, "Civil District [A-Z0-9][0-9]? ","")
replace city = regexr(city, "Militia District [A-Z0-9][0-9]?[0-9]?[0-9]?[0-9]? ","")
replace city = regexr(city, "Magisterial District [A-Z0-9][0-9]?,? ","")
replace city = regexr(city, "District [A-Z0-9][0-9]? ","")
replace city = regexr(city, "Ward [A-Z0-9][0-9]?$","")
replace city = regexr(city, "Justice Precinct [A-Z0-9][0-9]?$","")
replace city = regexr(city, "Precincts? [A-Z0-9][0-9]?$","")
replace city = regexr(city, "Election District [A-Z0-9][0-9]?$","")
replace city = regexr(city, "Civil District [A-Z0-9][0-9]?$","")
replace city = regexr(city, "Militia District [A-Z0-9][0-9]?[0-9]?[0-9]?[0-9]?$","")
replace city = regexr(city, "Justice ","")
replace city = regexr(city, "Voting ","")
replace city = regexr(city, "[A-Za-z]+ [A-Za-z]+ Township,? ","")
replace city = regexr(city, "[A-Za-z]+ Township,? ","")
replace city = regexr(city, ", Precinct [A-Z0-9]","")
replace city = regexr(city, "Magisterial\) ","")
replace city = regexr(city, "[0-9][0-9]?[a-z][a-z] Precinct ","")
replace city = regexr(city, "[A-Za-z]+ Hundred,? ","")
replace city = regexr(city, " township","")
replace city = regexr(city, " borough$","")
replace city = regexr(city, " town$","")
replace city = regexr(city, " village$","")
replace city = regexr(city, " district$","")
replace city = regexr(city, " District$","")
replace city = regexr(city, "Beat [0-9][0-9]? ","")
replace city = regexr(city, "Beat ","")
replace city = regexr(city, "Congressional ","")
replace city = regexr(city, "Magisterial ","")
replace city = regexr(city, " Precinct ","")
replace city = regexr(city, " City ","")
replace city = regexr(city, ", [a-z]+ side","")
replace city= regexs(1) if regexm(city,"[A-Z][a-z][a-z]+([A-Z][a-z]+)$")
replace city= regexs(1) if regexm(city,"[A-Z][a-z][a-z][a-z]+([A-Z][a-z]+ [A-Z][a-z]+)$")
replace city= regexs(1) if regexm(city,"[A-Z][a-z][a-z][a-z]+([A-Z][a-z]. [A-Z][a-z]+)$")
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?,? ([A-Za-z]+ [A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? ([A-Za-z]+ [A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? [0-9][0-9]?,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? [0-9][0-9]?,? ([A-Za-z]+ [A-Za-z]+),")
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? [0-9][0-9]?,? [0-9][0-9]?,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Precincts? [0-9][0-9]?, [0-9][0-9]?,? [0-9][0-9]?,? [0-9][0-9]?,? ([A-Za-z]+ [A-Za-z]+),")
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ([A-Za-z]+ [A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ?[0-9]+,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ?[0-9]+,? ([A-Za-z]+ [A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ?[0-9]+,? ?[0-9]+,? ([A-Za-z]+),") 
replace city = regexs(1) if regexm(city, "Militia Districts? [0-9]+,? ?[0-9]+,? ?[0-9]+,? ([A-Za-z]+ [A-Za-z]+),") 
replace city = regexr(city, "^[0-9][0-9]? ","")
replace city = regexr(city, "^ [0-9][0-9]? ","")


split city, p(",")
replace city = city1 if !missing(city2) & missing(city3)
drop city1-city11
**/

split city
replace city=city1 if city1==city2 & !missing(city1)
replace city=city1+" "+city2 if city1==city3 & city2==city4 & !missing(city4) & missing(city5)
drop city1-city7 

replace city=trim(proper(city))
drop if missing(city)

forval x = 1/9{
drop if city=="`x'"
}

replace county = regexr(county," County","")
replace county = regexr(county," \(.*\)","")
replace county = subinstr(county,"St.","St",.)
replace county = subinstr(county,"Mt.","Mt",.)
replace state="Oklahoma" if state=="Indian Territory"
replace state = regexr(state," Territory","")
collapse (sum) pop1900, by(state county city)

replace city = substr(city, 1, 50)
compress

save event_township1910_clean.dta, replace
