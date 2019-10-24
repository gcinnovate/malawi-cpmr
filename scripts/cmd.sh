psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'physicalviolence')::int + (values->>'suicide')::int + (values->>'rape')::int + (values->>'defilement')::int + (values->>'humantrafficking')::int + (values->>'kidnapping')::int + (values->>'indecentassault')::int + (values->>'economicabuse')::int + (values->>'childneglect')::int + (values->>'maritalconflict')::int + (values->>'breachofpeace')::int + (values->>'sexualoffences')::int) AS total_case FROM flow_data WHERE report_type = 'pvsu' and year::int < 2019;" >total.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'boys_physicalviolence')::int + (values->>'boys_suicide')::int + (values->>'boys_humantrafficking')::int + (values->>'boys_kidnapping')::int + (values->>'boys_indecentassault')::int + (values->>'boys_economicabuse')::int + (values->>'boys_childneglect')::int + (values->>'boys_maritalconflict')::int + (values->>'boys_breachofpeace')::int + (values->>'boys_sexualoffences')::int) AS total_case FROM flow_data WHERE report_type = 'pvsu' and year::int < 2019;" > boys_total.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'girls_physicalviolence')::int + (values->>'girls_suicide')::int + (values->>'girls_rape')::int + (values->>'girls_defilement')::int + (values->>'girls_humantrafficking')::int + (values->>'girls_kidnapping')::int + (values->>'girls_indecentassault')::int + (values->>'girls_economicabuse')::int + (values->>'girls_childneglect')::int + (values->>'girls_maritalconflict')::int + (values->>'girls_breachofpeace')::int + (values->>'girls_sexualoffences')::int) AS total_case FROM flow_data WHERE report_type = 'pvsu' and year::int < 2019;" > girls_total.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'men_physicalviolence')::int + (values->>'men_suicide')::int + (values->>'men_humantrafficking')::int + (values->>'men_kidnapping')::int + (values->>'men_indecentassault')::int + (values->>'men_economicabuse')::int + (values->>'men_childneglect')::int + (values->>'men_maritalconflict')::int + (values->>'men_breachofpeace')::int + (values->>'men_sexualoffences')::int) AS total_case FROM flow_data WHERE report_type = 'pvsu' and year::int < 2019;" > men_total.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'women_physicalviolence')::int + (values->>'women_suicide')::int + (values->>'women_rape')::int + (values->>'women_humantrafficking')::int + (values->>'women_kidnapping')::int + (values->>'women_indecentassault')::int + (values->>'women_economicabuse')::int + (values->>'women_childneglect')::int + (values->>'women_maritalconflict')::int + (values->>'women_breachofpeace')::int + (values->>'women_sexualoffences')::int) AS total_case FROM flow_data WHERE report_type = 'pvsu' and year::int < 2019;" > women_total.txt

less total.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{total_cases}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less boys_total.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{boys_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less girls_total.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{girls_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less men_total.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{men_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less women_total.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{women_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr
