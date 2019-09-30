--CREATE INDEX IF NOT EXISTS flow_data_idx1 ON flow_data USING GIN(values);
DROP VIEW IF EXISTS ncjf_childcases_view;
DROP VIEW IF EXISTS ncjf_casetypes_national_view;
DROP VIEW IF EXISTS cases_dealtwith_regional_view;
DROP VIEW IF EXISTS cases_dealtwith_national_view;
DROP VIEW IF EXISTS cases_by_police_station;
DROP VIEW IF EXISTS pvsu_cases_by_region_view;
DROP VIEW IF EXISTS pvsu_casetypes_regional_view;
DROP VIEW IF EXISTS pvsu_cases_demographics_regional_view;
DROP VIEW IF EXISTS flow_data_pvsu_view;
CREATE OR REPLACE VIEW flow_data_pvsu_view  AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        c.name AS region,
        b.name AS district,
        c.id AS region_id,
        d.name AS police_station,
        d.longitude::numeric, d.latitude::numeric,
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
        CASE
            WHEN a.month ~ '(0[13578]|1[02])$' THEN
                (a.month || '-31')::date
            WHEN a.month ~ '(0[469]|1[1])$' THEN
                (a.month || '-30')::date
            ELSE
                (a.month || '-28')::date
        END AS rdate,
        'Malawi'::text AS nation
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
        created,
        CASE
            WHEN a.month ~ '(0[13578]|1[02])$' THEN
                (a.month || '-31')::date
            WHEN a.month ~ '(0[469]|1[1])$' THEN
                (a.month || '-30')::date
            ELSE
                (a.month || '-28')::date
        END AS rdate
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
    WHERE
        a.report_type = 'diversion';

DROP VIEW IF EXISTS diversion_data_view;
CREATE OR REPLACE VIEW diversion_data_view  AS
    SELECT
        a.month, a.year, a.msisdn,
        c.name AS region,
        b.name AS district,
        d.name AS police_station,
        created,
        a.values->>'boys_arrested'boys_arrested,
        a.values->>'girls_arrested' girls_arrested,
        (a.values->>'arrested')::int arrested,
        a.values->>'boys_divertedatpolice' boys_divertedatpolice,
        a.values->>'girls_divertedatpolice' girls_divertedatpolice,
        (a.values->>'divertedatpolice')::int divertedatpolice,
        a.values->>'boys_takentocourt'  boys_takentocourt,
        a.values->>'girls_takentocourt'  girls_takentocourt,
        (a.values->>'takentocourt')::int  takentocourt,
        a.values->>'boys_bailed' boys_bailed,
        a.values->>'girls_bailed' girls_bailed,
        (a.values->>'bailed')::int bailed,
        a.values->>'boys_releasedfreely' boys_releasedfreely,
        a.values->>'girls_releasedfreely' girls_releasedfreely,
        (a.values->>'releasedfreely')::int releasedfreely,
        a.values->>'boys_releasedin48hrs' boys_releasedin48hrs,
        a.values->>'girls_releasedin48hrs' girls_releasedin48hrs,
        (a.values->>'releasedin48hrs')::int releasedin48hrs,
        (a.values->>'total_cases')::int total_cases
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
    WHERE
        a.report_type = 'diversion'
    ORDER BY
        year desc, month desc, region, district;



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

DROP VIEW IF EXISTS cases_by_police_station;
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
        c.name AS region,
        c.id AS region_id,
        b.name AS district,
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
        CASE
            WHEN a.month ~ '(0[13578]|1[02])$' THEN
                (a.month || '-31')::date
            WHEN a.month ~ '(0[469]|1[1])$' THEN
                (a.month || '-30')::date
            ELSE
                (a.month || '-28')::date
        END AS rdate
    FROM
        flow_data a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN justice_courts AS d ON a.court = d.id
    WHERE
        a.report_type = 'ncjf';

-- pvsu_pie_chart
-- this is for the regional case types
DROP VIEW IF EXISTS pvsu_casetypes_regional_view;
CREATE VIEW pvsu_casetypes_regional_view AS
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
        month, year, region_id
    FROM flow_data_pvsu_view
    GROUP BY month, year, region_id;

DROP VIEW IF EXISTS pvsu_cases_demographics_regional_view;
CREATE VIEW pvsu_cases_demographics_regional_view AS
    SELECT
        sum(boys_total) boys_total,
        sum(girls_total) girls_total,
        sum(men_total) men_total,
        sum(women_total) women_total,
        month, year, region_id
    FROM flow_data_pvsu_view
    GROUP BY month, year, region_id;


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
        CASE
            WHEN a.month ~ '(0[13578]|1[02])$' THEN
                (a.month || '-31')::date
            WHEN a.month ~ '(0[469]|1[1])$' THEN
                (a.month || '-30')::date
            ELSE
                (a.month || '-28')::date
        END AS rdate
    FROM
        summary_cases a
        LEFT OUTER JOIN locations AS b ON a.district = b.id
        LEFT OUTER JOIN locations AS c ON a.region = c.id
        LEFT OUTER JOIN police_stations AS d ON a.station = d.id
        LEFT OUTER JOIN justice_courts AS e ON a.court = e.id;

DROP VIEW IF EXISTS cases_dealtwith_national_view;
CREATE VIEW cases_dealtwith_national_view AS
    SELECT
        CASE
            WHEN sum(arrested) > 0 THEN
            round((((sum(releasedin48hrs)+0.0)/sum(arrested))*100)::numeric, 2)
            ELSE 0 END AS dealtwith, month, region, rdate,
        CASE
            WHEN region = 'Central' THEN 33.780388 -- Lilongwe
            WHEN region = 'Eastern' THEN 35.341996 -- Zomba
            WHEN region = 'Northern' THEN 34.009877 -- Mzuzu
            WHEN region = 'Southern' THEN 35.009247 -- Blantyre
        END AS longitude,

        CASE
            WHEN region = 'Central' THEN -13.960508 -- Lilongwe
            WHEN region = 'Eastern' THEN -15.377539 -- Zomba
            WHEN region = 'Northern' THEN -11.437847 -- Mzuzu
            WHEN region = 'Southern' THEN -15.748002 --Blantyre
        END AS latitude

    FROM
        flow_data_diversion_view
    GROUP BY month, region, rdate
    ORDER BY month, region;

DROP VIEW IF EXISTS cases_dealtwith_regional_view;
CREATE VIEW cases_dealtwith_regional_view AS
    SELECT
        CASE
            WHEN sum(arrested) > 0 THEN
                round((((sum(releasedin48hrs)+0.0)/sum(arrested))*100)::numeric, 2)
            ELSE 0 END AS dealtwith,
        month, police_station, rdate, region, longitude, latitude
    FROM
        flow_data_diversion_view
    GROUP BY
        month, police_station, region, rdate, longitude, latitude;

DROP VIEW IF EXISTS pvsu_cases_by_region_view;
CREATE VIEW pvsu_cases_by_region_view AS
    SELECT region, sum(total_cases) total_cases, rdate, month,
    (CASE
            WHEN region = 'Central' THEN 33.780388 -- Lilongwe
            WHEN region = 'Eastern' THEN 35.341996 -- Zomba
            WHEN region = 'Northern' THEN 34.009877 -- Mzuzu
            WHEN region = 'Southern' THEN 35.009247 -- Blantyre
        END)::NUMERIC AS longitude,

        (CASE
            WHEN region = 'Central' THEN -13.960508 -- Lilongwe
            WHEN region = 'Eastern' THEN -15.377539 -- Zomba
            WHEN region = 'Northern' THEN -11.437847 -- Mzuzu
            WHEN region = 'Southern' THEN -15.748002 --Blantyre
        END)::NUMERIC AS latitude
    FROM flow_data_pvsu_view
    GROUP BY region, month, rdate;

DROP VIEW IF EXISTS ncjf_casetypes_national_view;
CREATE VIEW ncjf_casetypes_national_view AS
    SELECT
        sum(fromprevmonth_cvc) fromprevmonth_cvc,
        sum(newlyregistered_cvc) newlyregistered_cvc,
        sum(newlyregconcluded_cvc) newlyregconcluded_cvc,
        sum(concluded_cvc) concluded_cvc,

        sum(fromprevmonth_cbc) fromprevmonth_cbc,
        sum(newlyregistered_cbc) newlyregistered_cbc,
        sum(newlyregconcluded_cbc) newlyregconcluded_cbc,
        sum(concluded_cbc) concluded_cbc,

        sum(fromprevmonth_inconflict) fromprevmonth_inconflict,
        sum(newlyregistered_inconflict) newlyregistered_inconflict,
        sum(newlyregconcluded_inconflict) newlyregconcluded_inconflict,
        sum(concluded_inconflict) concluded_inconflict,
        month, year
    FROM
        flow_data_ncjf_view group by month, year;

DROP VIEW IF EXISTS ncjf_childcases_view;
CREATE VIEW ncjf_childcases_view AS
    SELECT
        casetype,
        (json_values->>'fromprevmonth')::int fromprevmonth,
        (json_values->>'newlyregistered')::int newlyregistered,
        (json_values->>'newlyregconcluded')::int newlyregconcluded,
        (json_values->>'concluded')::int concluded,
        month,
        year,
        CASE
            WHEN month ~ '(0[13578]|1[02])$' THEN
                (month || '-31')::date
            WHEN month ~ '(0[469]|1[1])$' THEN
                (month || '-30')::date
            ELSE
                (month || '-28')::date
        END AS rdate
    FROM
        summary_cases
    WHERE
        report_type = 'ncjf'
        AND
        summary_slug = 'child_cases'
        AND
        summary_for = 'nation';
