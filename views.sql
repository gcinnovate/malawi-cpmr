--CREATE INDEX IF NOT EXISTS flow_data_idx1 ON flow_data USING GIN(values);
DROP VIEW IF EXISTS cases_by_police_station;
DROP VIEW IF EXISTS pvsu_casetypes_view;
DROP VIEW IF EXISTS pvsu_cases_demographics_view;
DROP VIEW IF EXISTS flow_data_pvsu_view;
CREATE OR REPLACE VIEW flow_data_pvsu_view  AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        b.name AS district,
        c.name AS region,
        d.name AS police_station,
        d.longitude, d.latitude,
        (a.values->>'suicide')::int suicide,
        (a.values->>'physicalviolence')::int physicalviolence,
        (a.values->>'rape')::int  rape,
        (a.values->>'defilement')::int defilement,
        (a.values->>'indecentassault')::int indecentassault,
        (a.values->>'kidnapping')::int kidnapping,
        (a.values->>'humantrafficking')::int humantrafficking,
        (a.values->>'sexualoffences')::int sexualoffences,
        (a.values->>'maritalconflict')::int maritalconflict,
        (a.values->>'childneglect')::int childneglect,
        (a.values->>'economicabuse')::int economicabuse,
        (a.values->>'breachofpeace')::int breachofpeace,
        (a.values->>'total_cases')::int total_cases,
        (a.values->>'boys_total')::int boys_total,
        (a.values->>'girls_total')::int girls_total,
        (a.values->>'men_total')::int men_total,
        (a.values->>'women_total')::int women_total,
        created,
        (a.month || '-01')::date rdate,
        'Malawi' nation
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
    WHERE
        a.report_type = 'pvsu';

DROP VIEW IF EXISTS flow_data_diversion_view;
CREATE OR REPLACE VIEW flow_data_diversion_view  AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        b.name AS district,
        c.name AS region,
        d.name AS police_station,
        d.longitude, d.latitude,
        (a.values->>'arrested')::int arrested,
        (a.values->>'divertedatpolice')::int divertedatpolice,
        (a.values->>'takentocourt')::int  takentocourt,
        (a.values->>'bailed')::int bailed,
        (a.values->>'releasedfreely')::int releasedfreely,
        (a.values->>'releasedin48hrs')::int releasedin48hrs,
        (a.values->>'total_cases')::int total_cases,
        created
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
    WHERE
        a.report_type = 'diversion';


-- MORE QUERIES DERIVED
-- Individual cases in a region
-- SELECT sum(suicide) suicide, sum(physicalviolence) physicalviolence, region from flow_data_pvsu_view group by region;
-- you can add the other types

-- Sum of all cases in a region
-- SELECT sum((suicide + physicalviolence)) cases, region from flow_data_pvsu_view group by region;
-- you can add the other types in the equation

CREATE OR REPLACE FUNCTION get_long(p text) RETURNS TEXT AS
$$
    DECLARE
    x text;
    BEGIN
        SELECT longitude INTO x FROM police_stations WHERE name = p;
        IF FOUND THEN
            RETURN x;
        END IF;
        RETURN '';
    END;
$$ language 'plpgsql';

CREATE OR REPLACE FUNCTION get_lat(p text) RETURNS TEXT AS
$$
    DECLARE
    x text;
    BEGIN
        SELECT latitude INTO x FROM police_stations WHERE name = p;
        IF FOUND THEN
            RETURN x;
        END IF;
        RETURN '';
    END;
$$ language 'plpgsql';

CREATE VIEW cases_by_police_station AS
    SELECT flow_data_pvsu_view.police_station,
        sum(flow_data_pvsu_view.total_cases) AS total,
        get_long(flow_data_pvsu_view.police_station::text) AS long,
        get_lat(flow_data_pvsu_view.police_station::text) AS lat
    FROM flow_data_pvsu_view
    GROUP BY flow_data_pvsu_view.police_station
     ORDER BY flow_data_pvsu_view.police_station;

DROP VIEW IF EXISTS flow_data_ncjf_view;
CREATE OR REPLACE VIEW flow_data_ncjf_view  AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        1 AS nation,
        b.name AS district,
        c.name AS region,
        d.name AS court,
        d.longitude, d.latitude,
        (a.values->>'fromprevmonth_cvc')::int fromprevmonth_cvc,
        (a.values->>'newlyregistered_cvc')::int newlyregistered_cvc,
        (a.values->>'newlyregconcluded_cvc')::int newlyregconcluded_cvc,
        (a.values->>'concluded_cvc')::int concluded_cvc,
        (a.values->>'cvc')::int cvc,
        (a.values->>'fromprevmonth_cbc')::int fromprevmonth_cbc,
        (a.values->>'newlyregistered_cbc')::int newlyregistered_cbc,
        (a.values->>'newlyregconcluded_cbc')::int newlyregconcluded_cbc,
        (a.values->>'concluded_cbc')::int concluded_cbc,
        (a.values->>'cbc')::int cbc,
        (a.values->>'fromprevmonth_inconflict')::int fromprevmonth_inconflict,
        (a.values->>'newlyregistered_inconflict')::int newlyregistered_inconflict,
        (a.values->>'newlyregconcluded_inconflict')::int newlyregconcluded_inconflict,
        (a.values->>'concluded_inconflict')::int concluded_inconflict,
        (a.values->>'inconflict')::int inconflict,
        (a.values->>'childmaintenance_cbctype')::int childmaintenance_cbctype,
        (a.values->>'childcustody_cbctype')::int childcustody_cbctype,
        (a.values->>'childfosterage_cbctype')::int childfosterage_cbctype,
        (a.values->>'childadoption_cbctype')::int childadoption_cbctype,
        (a.values->>'childparentage_cbctype')::int childparentage_cbctype,
        (a.values->>'childguardianship_cbctype')::int childguardianship_cbctype,
        (a.values->>'childaccess_cbctype')::int childaccess_cbctype,
        (a.values->>'estatedistribution_cbctype')::int estatedistribution_cbctype,
        (a.values->>'total_custodialorder')::int total_custodialorder,
        (a.values->>'reformatories_custodialorder')::int reformatories_custodialorder,
        (a.values->>'prisons_custodialorder')::int prisons_custodialorder,
        (a.values->>'total_remanded')::int total_remanded,
        (a.values->>'safetyhomes_remanded')::int safetyhomes_remanded,
        (a.values->>'reformatorycentres_remanded')::int reformatorycentres_remanded,
        (a.values->>'policecells_remanded')::int policecells_remanded,
        (a.values->>'preliminaryinquiry_diverted')::int preliminaryinquiry_diverted,
        (a.values->>'aftertrial_diverted')::int aftertrial_diverted,
        (a.values->>'diverted')::int diverted,
        (a.values->>'bailed')::int bailed,
        (a.values->>'perpetrators')::int perpetrators,
        (a.values->>'imprisoned_perpetrators')::int imprisoned_perpetrators,
        (a.values->>'acquited_perpetrators')::int acquited_perpetrators,
        (a.values->>'fined_perpetrators')::int fined_perpetrators,
        (a.values->>'specialreferrals')::int specialreferrals,
        (a.values->>'caseswithdrawn')::int caseswithdrawn,
        (a.values->>'referredchildsurvivors')::int referredchildsurvivors,
        created,
        (a.month || '-01')::date rdate
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN justice_courts AS d ON a.court = d.id
    WHERE
        a.report_type = 'ncjf';

-- pvsu_pie_chart
DROP VIEW IF EXISTS pvsu_casetypes_view;
CREATE VIEW pvsu_casetypes_view AS
    SELECT
        sum(physicalviolence) physicalviolence,
        sum(suicide) suicide,
        sum(defilement) defilement,
        sum(rape) rape,
        sum(indecentassault) indecentassault,
        sum(sexualoffences) sexualoffences,
        sum(humantrafficking) humantrafficking,
        sum(kidnapping) kidnapping,
        sum(maritalconflict) maritalconflict,
        sum(childneglect) childneglect,
        sum(economicabuse) economicabuse,
        sum(breachofpeace) breachofpeace,
        month, year
    FROM flow_data_pvsu_view
    GROUP BY month, year;

DROP VIEW IF EXISTS pvsu_cases_demographics_view;
CREATE VIEW pvsu_cases_demographics_view AS
    SELECT
        sum(boys_total) boys_total,
        sum(girls_total) girls_total,
        sum(men_total) men_total,
        sum(women_total) women_total,
        month, year
    FROM flow_data_pvsu_view
    GROUP BY month, year;


DROP VIEW IF EXISTS summary_cases_view;
CREATE OR REPLACE VIEW summary_cases_view  AS
    SELECT
        a.casetype, a.value,
        a.month, a.year, a.report_type,
        a.summary_for,
        a.summary_slug,
        b.name AS district,
        c.name AS region,
        d.name AS police_station,
        d.longitude, d.latitude,
        (a.month || '-01')::date rdate
    FROM
        summary_cases a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
        LEFT OUTER JOIN justice_courts AS e ON a.court = e.id;
