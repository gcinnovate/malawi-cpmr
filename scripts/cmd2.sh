
psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'physicalviolence')::int + (values->>'defilement')::int + (values->>'childneglect')::int + (values->>'childmarriage')::int + (values->>'humantrafficking')::int + (values->>'economicabuse')::int + (values->>'economicexploitation')::int + (values->>'emotionalabuse')::int + (values->>'maritalconflict')::int + (values->>'sexualviolence')::int) AS total_case FROM flow_data WHERE report_type = 'cvsu'" >total2.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'boys_physicalviolence')::int + (values->>'boys_sexualviolence')::int + (values->>'boys_humantrafficking')::int + (values->>'boys_childmarriage')::int + (values->>'boys_emotionalabuse')::int + (values->>'boys_economicabuse')::int + (values->>'boys_childneglect')::int + (values->>'boys_maritalconflict')::int + (values->>'boys_economicexploitation')::int)  AS total_case FROM flow_data WHERE report_type = 'cvsu'" > boys_total2.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'girls_physicalviolence')::int + (values->>'girls_sexualviolence')::int + (values->>'girls_humantrafficking')::int + (values->>'girls_defilement')::int + (values->>'girls_childmarriage')::int + (values->>'girls_childneglect')::int + (values->>'girls_economicexploitation')::int + (values->>'girls_economicabuse')::int + (values->>'girls_maritalconflict')::int)  AS girls_total FROM flow_data WHERE report_type = 'cvsu'" > girls_total2.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'men_physicalviolence')::int + (values->>'men_sexualviolence')::int + (values->>'men_humantrafficking')::int + (values->>'men_emotionalabuse')::int + (values->>'men_economicabuse')::int + (values->>'men_maritalconflict')::int + (values->>'men_economicexploitation')::int)  AS men_total FROM flow_data WHERE report_type = 'cvsu'" > men_total2.txt

psql -U postgres -d cpmr -t -P border=0 -c "SELECT id, ((values->>'women_physicalviolence')::int + (values->>'women_sexualviolence')::int + (values->>'women_humantrafficking')::int + (values->>'women_emotionalabuse')::int + (values->>'women_economicabuse')::int + (values->>'women_maritalconflict')::int + (values->>'women_economicexploitation')::int)  AS women_total FROM flow_data WHERE report_type = 'cvsu'" > women_total2.txt

less total2.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{total_cases}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less boys_total2.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{boys_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less girls_total2.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{girls_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less men_total2.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{men_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr

less women_total2.txt | awk -F" " '/^[0-9]/ {print "UPDATE flow_data SET values = jsonb_set(values, \47{women_total}\47, \47" $2 "\47) WHERE id = " $1 ";" }' |psql -U postgres -d cpmr
