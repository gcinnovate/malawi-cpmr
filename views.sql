--CREATE INDEX IF NOT EXISTS flow_data_idx1 ON flow_data USING GIN(values);
DROP VIEW IF EXISTS diversion_data_view_eastern;
DROP VIEW IF EXISTS diversion_data_view_southern;
DROP VIEW IF EXISTS diversion_data_view_northern;
DROP VIEW IF EXISTS diversion_data_view_central;
--
DROP VIEW IF EXISTS pvsu_victims_data_eastern;
DROP VIEW IF EXISTS pvsu_victims_data_southern;
DROP VIEW IF EXISTS pvsu_victims_data_northern;
DROP VIEW IF EXISTS pvsu_victims_data_central;
--
DROP VIEW IF EXISTS cases_dealtwith_regional_view_eastern;
DROP VIEW IF EXISTS cases_dealtwith_regional_view_southern;
DROP VIEW IF EXISTS cases_dealtwith_regional_view_northern;
DROP VIEW IF EXISTS cases_dealtwith_regional_view_central;
--
DROP VIEW IF EXISTS flow_data_diversion_view_eastern;
DROP VIEW IF EXISTS flow_data_diversion_view_southern;
DROP VIEW IF EXISTS flow_data_diversion_view_northern;
DROP VIEW IF EXISTS flow_data_diversion_view_central;
--
DROP VIEW IF EXISTS summary_cases_view_ncjf;
DROP VIEW IF EXISTS summary_cases_view_mog;
DROP VIEW IF EXISTS summary_cases_view_eastern;
DROP VIEW IF EXISTS summary_cases_view_southern;
DROP VIEW IF EXISTS summary_cases_view_northern;
DROP VIEW IF EXISTS summary_cases_view_central;
--
DROP VIEW IF EXISTS flow_data_pvsu_view_eastern;
DROP VIEW IF EXISTS flow_data_pvsu_view_southern;
DROP VIEW IF EXISTS flow_data_pvsu_view_northern;
DROP VIEW IF EXISTS flow_data_pvsu_view_central;
--
DROP VIEW IF EXISTS cc_attendance_regional_view;
DROP VIEW IF EXISTS cvsu_cases_demographics_regional_view;
DROP VIEW IF EXISTS cvsu_casetypes_regional_view;
DROP VIEW IF EXISTS flow_data_cvsu_view_all;
DROP VIEW IF EXISTS flow_data_cvsu_view;
DROP VIEW IF EXISTS ncjf_childvictim_cases_stats_view;
DROP VIEW IF EXISTS ncjf_chilcases_concluded_by_courts;
DROP VIEW IF EXISTS ncjf_childcases_view;
DROP VIEW IF EXISTS ncjf_casetypes_national_view;
DROP VIEW IF EXISTS ncjf_childvictim_categories_view;
DROP VIEW IF EXISTS cases_dealtwith_regional_view;
DROP VIEW IF EXISTS cases_dealtwith_national_view;
DROP VIEW IF EXISTS cases_by_police_station;
DROP VIEW IF EXISTS pvsu_cases_by_region_view;
DROP VIEW IF EXISTS pvsu_casetypes_regional_view;
DROP VIEW IF EXISTS pvsu_cases_demographics_regional_view;
DROP VIEW IF EXISTS flow_data_pvsu_view;
DROP VIEW IF EXISTS flow_data_diversion_view;
-- view for all pvsu data
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

-- view for diversion data total per category
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

-- view for all the diversion data
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

-- view for all NCJF data
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
        (a.values->>'defilement_cvccategory')::int defilement_cvccategory,
        (a.values->>'childmarriage_cvccategory')::int childmarriage_cvccategory,
        (a.values->>'childtrafficking_cvccategory')::int childtrafficking_cvccategory,
        (a.values->>'sexualviolence_cvccategory')::int sexualviolence_cvccategory,
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
        (a.values->>'concluded')::int concluded,
        (a.values->>'total_childcases')::int total_childcases,
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
        (a.values->>'acquitted_perpetrators')::int acquitted_perpetrators,
        (a.values->>'fined_perpetrators')::int fined_perpetrators,
        (a.values->>'specialreferrals')::int specialreferrals,
        (a.values->>'caseswithdrawn')::numeric::int caseswithdrawn,
        (a.values->>'referredchildsurvivors')::numeric::int referredchildsurvivors,
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

-- view for pvsu demographics data at regional level
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


-- view for summary_cases table used for pie chart data
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

DROP VIEW IF EXISTS summary_cases_view_ncjf;
CREATE VIEW summary_cases_view_ncjf AS
    SELECT * FROM summary_cases_view WHERE report_type = 'ncjf';

DROP VIEW IF EXISTS summary_cases_view_mog;
CREATE VIEW summary_cases_view_mog AS
    SELECT * FROM summary_cases_view WHERE report_type IN ('cc', 'cvsu');


-- view for diversion cases dealt with at national level
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

-- view for diversion cases dealt with at regional level
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

-- view for PVSU cases by regions
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

-- view for the sum of child victim cases by category, for pie chart data
DROP VIEW IF EXISTS ncjf_childvictim_categories_view;
CREATE VIEW ncjf_childvictim_categories_view AS
    SELECT
        sum(defilement_cvccategory) defilement,
        sum(childmarriage_cvccategory) childmarriage,
        sum(childtrafficking_cvccategory) childtrafficking,
        sum(sexualviolence_cvccategory) sexualviolence,
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

DROP VIEW IF EXISTS ncjf_chilcases_concluded_by_courts;
CREATE VIEW ncjf_chilcases_concluded_by_courts AS
    SELECT
        CASE
            WHEN sum(total_childcases) > 0 THEN
                round((((sum(concluded)+0.0)/sum(total_childcases))*100)::numeric, 2)
            ELSE 0
        END AS cases_concluded,
        month, court, rdate, longitude, latitude
    FROM
        flow_data_ncjf_view
    GROUP BY
        month, court, rdate, longitude, latitude
    ORDER by rdate DESC, court;

DROP VIEW IF EXISTS ncjf_childvictim_cases_stats_view;
CREATE VIEW ncjf_childvictim_cases_stats_view AS
    SELECT
        sum(imprisoned_perpetrators) imprisoned,
        sum(acquitted_perpetrators) acquited,
        sum(fined_perpetrators) fined,
        sum(caseswithdrawn) caseswithdrawn,
        sum(referredchildsurvivors) referredchildsurvivors,
        --CASE
        --    WHEN sum(cvc) > 0 THEN -- total child victim cases
        --        round((((sum(caseswithdrawn)+0.0)/sum(cvc))*100)::numeric, 2)
        --    ELSE
        --        0
        --END AS caseswithdrawn,
        --CASE
        --    WHEN sum(cvc) > 0 THEN -- total child victim cases
        --        round((((sum(referredchildsurvivors)+0.0)/sum(cvc))*100)::numeric, 2)
        --    ELSE
        --        0
        --END AS referredchildsurvivors,
        sum(bailed) bailed,
        -- CASE
        --     WHEN sum(inconflict) > 0 THEN -- total child victim cases
        --         round((((sum(bailed)+0.0)/sum(inconflict))*100)::numeric, 2)
        --     ELSE
        --         0
        -- END AS bailed,
        sum(preliminaryinquiry_diverted) preliminaryinquiry_diverted,
        sum(aftertrial_diverted) aftertrial_diverted,
        sum(reformatories_custodialorder) reformatories_custodialorder,
        -- CASE
        --     WHEN sum(inconflict) > 0 THEN -- total child victim cases
        --         round((((sum(reformatories_custodialorder)+0.0)/sum(inconflict))*100)::numeric, 2)
        --     ELSE
        --         0
        -- END AS reformatories_custodialorder,
        sum(prisons_custodialorder) prisons_custodialorder,
        --
        sum(safetyhomes_remanded) safetyhomes_remanded,
        sum(reformatorycentres_remanded) reformatorycentres_remanded,
        sum(policecells_remanded) policecells_remanded,
        sum(specialreferrals) specialreferrals,
        -- CASE
        --     WHEN sum(inconflict) > 0 THEN -- total child victim cases
        --         round((((sum(specialreferrals)+0.0)/sum(inconflict))*100)::numeric, 2)
        --     ELSE
        --         0
        -- END AS specialreferrals,
        'National':: text as nation,
        month, rdate
    FROM
        flow_data_ncjf_view
    GROUP BY month, rdate
    ORDER BY month DESC;

DROP VIEW IF EXISTS flow_data_cvsu_view;
-- view for all cvsu data
CREATE OR REPLACE VIEW flow_data_cvsu_view  AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        c.name AS region,
        b.name AS district,
        c.id AS region_id,
        d.name AS cvsu,
        (a.values->>'physicalviolence')::int physicalviolence,
        (a.values->>'defilement')::int defilement,
        (a.values->>'sexualviolence')::int sexualviolence,
        (a.values->>'childneglect')::int childneglect,
        (a.values->>'childmarriage')::int childmarriage,
        (a.values->>'emotionalabuse')::int emotionalabuse,
        (a.values->>'economicexploitation')::int economicexploitation,
        (a.values->>'humantrafficking')::int humantrafficking,
        (a.values->>'economicabuse')::int economicabuse,
        (a.values->>'maritalconflict')::int maritalconflict,
        (a.values->>'boys_total')::int boys_total,
        (a.values->>'girls_total')::int girls_total,
        (a.values->>'men_total')::int men_total,
        (a.values->>'women_total')::int women_total,
        (a.values->>'total_cases')::int total_cases,
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
        LEFT OUTER JOIN community_victim_support_units AS d ON a.cvsu = d.id
    WHERE
        a.report_type = 'cvsu';

DROP VIEW IF EXISTS flow_data_cvsu_view_all;
CREATE VIEW flow_data_cvsu_view_all AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        c.name AS region,
        b.name AS district,
        c.id AS region_id,
        d.name AS cvsu,
        (a.values->>'boys_physicalviolence')::int boys_physicalviolence,
        (a.values->>'girls_physicalviolence')::int girls_physicalviolence,
        (a.values->>'men_physicalviolence')::int men_physicalviolence,
        (a.values->>'women_physicalviolence')::int women_physicalviolence,
        (a.values->>'physicalviolence')::int physicalviolence,
        (a.values->>'defilement')::int defilement,
        (a.values->>'boys_sexualviolence')::int boys_sexualviolence,
        (a.values->>'girls_sexualviolence')::int girls_sexualviolence,
        (a.values->>'men_sexualviolence')::int men_sexualviolence,
        (a.values->>'women_sexualviolence')::int women_sexualviolence,
        (a.values->>'sexualviolence')::int sexualviolence,
        (a.values->>'boys_childneglect')::int boys_childneglect,
        (a.values->>'girls_childneglect')::int girls_childneglect,
        (a.values->>'childneglect')::int childneglect,
        (a.values->>'boys_childmarriage')::int boys_childmarriage,
        (a.values->>'girls_childmarriage')::int girls_childmarriage,
        (a.values->>'childmarriage')::int childmarriage,
        (a.values->>'boys_emotionalabuse')::int boys_emotionalabuse,
        (a.values->>'girls_emotionalabuse')::int girls_emotionalabuse,
        (a.values->>'men_emotionalabuse')::int men_emotionalabuse,
        (a.values->>'women_emotionalabuse')::int women_emotionalabuse,
        (a.values->>'emotionalabuse')::int emotionalabuse,
        (a.values->>'boys_economicexploitation')::int boys_economicexploitation,
        (a.values->>'girls_economicexploitation')::int girls_economicexploitation,
        (a.values->>'men_economicexploitation')::int men_economicexploitation,
        (a.values->>'women_economicexploitation')::int women_economicexploitation,
        (a.values->>'economicexploitation')::int economicexploitation,
        (a.values->>'boys_humantrafficking')::int boys_humantrafficking,
        (a.values->>'girls_humantrafficking')::int girls_humantrafficking,
        (a.values->>'men_humantrafficking')::int men_humantrafficking,
        (a.values->>'women_humantrafficking')::int women_humantrafficking,
        (a.values->>'humantrafficking')::int humantrafficking,
        (a.values->>'boys_economicabuse')::int boys_economicabuse,
        (a.values->>'girls_economicabuse')::int girls_economicabuse,
        (a.values->>'men_economicabuse')::int men_economicabuse,
        (a.values->>'women_economicabuse')::int women_economicabuse,
        (a.values->>'economicabuse')::int economicabuse,
        (a.values->>'boys_maritalconflict')::int boys_maritalconflict,
        (a.values->>'girls_maritalconflict')::int girls_maritalconflict,
        (a.values->>'men_maritalconflict')::int men_maritalconflict,
        (a.values->>'women_maritalconflict')::int women_maritalconflict,
        (a.values->>'maritalconflict')::int maritalconflict,
        (a.values->>'boys_total')::int boys_total,
        (a.values->>'girls_total')::int girls_total,
        (a.values->>'men_total')::int men_total,
        (a.values->>'women_total')::int women_total,
        (a.values->>'total_cases')::int total_cases,
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
        LEFT OUTER JOIN community_victim_support_units AS d ON a.cvsu = d.id
    WHERE
        a.report_type = 'cvsu';

-- cvsu_pie_chart
-- this is for the regional case types
DROP VIEW IF EXISTS cvsu_casetypes_regional_view;
CREATE VIEW cvsu_casetypes_regional_view AS
    SELECT
        sum(physicalviolence) physicalviolence,
        sum(defilement) defilement,
        sum(sexualviolence) sexualviolence,
        sum(childneglect) childneglect,
        sum(childmarriage) childmarriage,
        sum(emotionalabuse) emotionalabuse,
        sum(economicexploitation) economicexploitation,
        sum(humantrafficking) humantrafficking,
        sum(economicabuse) economicabuse,
        sum(maritalconflict) maritalconflict,
        month, year, region_id
    FROM flow_data_cvsu_view
    GROUP BY month, year, region_id;

-- view for cvsu demographics data at regional level
DROP VIEW IF EXISTS cvsu_cases_demographics_regional_view;
CREATE VIEW cvsu_cases_demographics_regional_view AS
    SELECT
        sum(boys_total) boys_total,
        sum(girls_total) girls_total,
        sum(men_total) men_total,
        sum(women_total) women_total,
        month, year, region_id
    FROM flow_data_cvsu_view
    GROUP BY month, year, region_id;

DROP VIEW IF EXISTS flow_data_cc_view;
CREATE VIEW flow_data_cc_view AS
    SELECT
        a.month, a.year, a.report_type, a.msisdn,
        c.name AS region,
        b.name AS district,
        c.id AS region_id,
        d.name AS childrens_corner,
        b.longitude::numeric, b.latitude::numeric,
        ceil((a.values->>'boys_attendance')::numeric)::int boys_attendance,
        ceil((a.values->>'girls_attendance')::numeric)::int girls_attendance,
        ceil((a.values->>'attendance')::numeric)::int attendance,
        (a.values->>'boys_referred')::int boys_referred,
        (a.values->>'girls_referred')::int girls_referred,
        (a.values->>'referred')::int referred,
        (a.values->>'boys_violence')::int boys_violence,
        (a.values->>'girls_violence')::int girls_violence,
        (a.values->>'violence')::int violence,
        (a.values->>'men_trainedfacilitators')::int men_trainedfacilitators,
        (a.values->>'women_trainedfacilitators')::int women_trainedfacilitators,
        (a.values->>'trainedfacilitators')::int trainedfacilitators,
        (a.values->>'men_nontrainedfacilitators')::int men_nontrainedfacilitators,
        (a.values->>'women_nontrainedfacilitators')::int women_nontrainedfacilitators,
        (a.values->>'nontrainedfacilitators')::int nontrainedfacilitators,
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
        LEFT OUTER JOIN childrens_corners AS d ON a.children_corner = d.id
    WHERE
        a.report_type = 'cc';

-- view for childrens corners attendance at regional level
DROP VIEW IF EXISTS cc_attendance_regional_view;
CREATE VIEW cc_attendance_regional_view AS
    SELECT
        sum(trainedfacilitators) trainedfacilitators,
        sum(nontrainedfacilitators) nontrainedfacilitators,
        month, year, region_id
    FROM flow_data_cc_view
    GROUP BY month, year, region_id;


-- Create region level views to help with permissions
DROP VIEW IF EXISTS flow_data_pvsu_view_eastern;
CREATE VIEW flow_data_pvsu_view_eastern AS
    SELECT * FROM flow_data_pvsu_view WHERE region = 'Eastern';

DROP VIEW IF EXISTS flow_data_pvsu_view_southern;
CREATE VIEW flow_data_pvsu_view_southern AS
    SELECT * FROM flow_data_pvsu_view WHERE region = 'Southern';

DROP VIEW IF EXISTS flow_data_pvsu_view_northern;
CREATE VIEW flow_data_pvsu_view_northern AS
    SELECT * FROM flow_data_pvsu_view WHERE region = 'Northern';

DROP VIEW IF EXISTS flow_data_pvsu_view_central;
CREATE VIEW flow_data_pvsu_view_central AS
    SELECT * FROM flow_data_pvsu_view WHERE region = 'Central';

-- summary_cases_views
DROP VIEW IF EXISTS summary_cases_view_eastern;
CREATE VIEW summary_cases_view_eastern AS
    SELECT * FROM summary_cases_view WHERE region = 'Eastern';

DROP VIEW IF EXISTS summary_cases_view_southern;
CREATE VIEW summary_cases_view_southern AS
    SELECT * FROM summary_cases_view WHERE region = 'Southern';

DROP VIEW IF EXISTS summary_cases_view_northern;
CREATE VIEW summary_cases_view_northern AS
    SELECT * FROM summary_cases_view WHERE region = 'Northern';

DROP VIEW IF EXISTS summary_cases_view_central;
CREATE VIEW summary_cases_view_central AS
    SELECT * FROM summary_cases_view WHERE region = 'Central';

-- Diversion regional views
DROP VIEW IF EXISTS flow_data_diversion_view_eastern;
CREATE VIEW flow_data_diversion_view_eastern AS
    SELECT * FROM flow_data_diversion_view WHERE region = 'Eastern';

DROP VIEW IF EXISTS flow_data_diversion_view_southern;
CREATE VIEW flow_data_diversion_view_southern AS
    SELECT * FROM flow_data_diversion_view WHERE region = 'Southern';

DROP VIEW IF EXISTS flow_data_diversion_view_northern;
CREATE VIEW flow_data_diversion_view_northern AS
    SELECT * FROM flow_data_diversion_view WHERE region = 'Northern';

DROP VIEW IF EXISTS flow_data_diversion_view_central;
CREATE VIEW flow_data_diversion_view_central AS
    SELECT * FROM flow_data_diversion_view WHERE region = 'Central';

-- Cases Delt with
DROP VIEW IF EXISTS cases_dealtwith_regional_view_eastern;
CREATE VIEW cases_dealtwith_regional_view_eastern AS
    SELECT * from cases_dealtwith_regional_view WHERE region = 'Eastern';

DROP VIEW IF EXISTS cases_dealtwith_regional_view_southern;
CREATE VIEW cases_dealtwith_regional_view_southern AS
    SELECT * from cases_dealtwith_regional_view WHERE region = 'Southern';

DROP VIEW IF EXISTS cases_dealtwith_regional_view_northern;
CREATE VIEW cases_dealtwith_regional_view_northern AS
    SELECT * from cases_dealtwith_regional_view WHERE region = 'Northern';

DROP VIEW IF EXISTS cases_dealtwith_regional_view_central;
CREATE VIEW cases_dealtwith_regional_view_central AS
    SELECT * from cases_dealtwith_regional_view WHERE region = 'Central';

DROP VIEW IF EXISTS pvsu_victims_data;
CREATE VIEW pvsu_victims_data AS
    SELECT a.month,
        a.year,
        a.msisdn,
        c.name AS region,
        b.name AS district,
        d.name AS police_station,
        to_char(created, 'YYYY-MM-DD HH:MI:SS') created,
        a.values->>'boys_suicide' boys_suicide,
        a.values->>'girls_suicide' girls_suicide,
        a.values->>'men_suicide' men_suicide,
        a.values->>'women_suicide' women_suicide,
        (a.values->>'suicide')::int suicide,
        a.values->>'boys_physicalviolence' boys_physicalviolence,
        a.values->>'girls_physicalviolence' girls_physicalviolence,
        a.values->>'men_physicalviolence' men_physicalviolence,
        a.values->>'women_physicalviolence' women_physicalviolence,
        (a.values->>'physicalviolence')::int physicalviolence,
        a.values->>'girls_rape' girls_rape,
        a.values->>'women_rape' women_rape,
        (a.values->>'rape')::int rape,
        a.values->>'girls_defilement' girls_defilement,
        (a.values->>'defilement')::int defilement,
        a.values->>'boys_indecentassault' boys_indecentassault,
        a.values->>'girls_indecentassault' girls_indecentassault,
        a.values->>'men_indecentassault' men_indecentassault,
        a.values->>'women_indecentassault' women_indecentassault,
        (a.values->>'indecentassault')::int indecentassault,
        a.values->>'boys_kidnapping' boys_kidnapping,
        a.values->>'girls_kidnapping' girls_kidnapping,
        a.values->>'men_kidnapping' men_kidnapping,
        a.values->>'women_kidnapping' women_kidnapping,
        (a.values->>'kidnapping')::int kidnapping,
        a.values->>'boys_humantrafficking' boys_humantrafficking,
        a.values->>'girls_humantrafficking' girls_humantrafficking,
        a.values->>'men_humantrafficking' men_humantrafficking,
        a.values->>'women_humantrafficking' women_humantrafficking,
        (a.values->>'humantrafficking')::int humantrafficking,
        a.values->>'boys_sexualoffences' boys_sexualoffences,
        a.values->>'girls_sexualoffences' girls_sexualoffences,
        a.values->>'men_sexualoffences' men_sexualoffences,
        a.values->>'women_sexualoffences' women_sexualoffences,
        (a.values->>'sexualoffences')::int sexualoffences,
        a.values->>'boys_maritalconflict' boys_maritalconflict,
        a.values->>'girls_maritalconflict' girls_maritalconflict,
        a.values->>'men_maritalconflict' men_maritalconflict,
        a.values->>'women_maritalconflict' women_maritalconflict,
        (a.values->>'maritalconflict')::int maritalconflict,
        a.values->>'boys_childneglect' boys_childneglect,
        a.values->>'girls_childneglect' girls_childneglect,
        (a.values->>'childneglect')::int childneglect,
        a.values->>'boys_economicabuse' boys_economicabuse,
        a.values->>'girls_economicabuse' girls_economicabuse,
        a.values->>'men_economicabuse' men_economicabuse,
        a.values->>'women_economicabuse' women_economicabuse,
        (a.values->>'economicabuse')::int economicabuse,
        a.values->>'boys_breachofpeace' boys_breachofpeace,
        a.values->>'girls_breachofpeace' girls_breachofpeace,
        a.values->>'men_breachofpeace' men_breachofpeace,
        a.values->>'women_breachofpeace' women_breachofpeace,
        (a.values->>'breachofpeace')::int breachofpeace,
        (a.values->>'total_cases')::int total_cases,
        (a.values->>'boys_total')::int boys_total,
        (a.values->>'girls_total')::int girls_total,
        (a.values->>'men_total')::int men_total,
        (a.values->>'women_total')::int women_total
    FROM flow_data a
    LEFT OUTER JOIN locations AS b ON a.district = b.id
    LEFT OUTER JOIN locations AS c ON a.region = c.id
    LEFT OUTER JOIN police_stations AS d ON a.station = d.id
    WHERE
        a.report_type = 'pvsu'
    ORDER BY
        month, region, district, police_station;

DROP VIEW IF EXISTS pvsu_victims_data_eastern;
CREATE VIEW pvsu_victims_data_eastern AS
    SELECT * FROM pvsu_victims_data WHERE region = 'Eastern';

DROP VIEW IF EXISTS pvsu_victims_data_southern;
CREATE VIEW pvsu_victims_data_southern AS
    SELECT * FROM pvsu_victims_data WHERE region = 'Southern';

DROP VIEW IF EXISTS pvsu_victims_data_northern;
CREATE VIEW pvsu_victims_data_northern AS
    SELECT * FROM pvsu_victims_data WHERE region = 'Northern';

DROP VIEW IF EXISTS pvsu_victims_data_central;
CREATE VIEW pvsu_victims_data_central AS
    SELECT * FROM pvsu_victims_data WHERE region = 'Central';

-- Diversion Data
DROP VIEW IF EXISTS diversion_data_view_eastern;
CREATE VIEW diversion_data_view_eastern AS
    SELECT * FROM diversion_data_view WHERE region = 'Eastern';

DROP VIEW IF EXISTS diversion_data_view_southern;
CREATE VIEW diversion_data_view_southern AS
    SELECT * FROM diversion_data_view WHERE region = 'Southern';

DROP VIEW IF EXISTS diversion_data_view_northern;
CREATE VIEW diversion_data_view_northern AS
    SELECT * FROM diversion_data_view WHERE region = 'Northern';

DROP VIEW IF EXISTS diversion_data_view_central;
CREATE VIEW diversion_data_view_central AS
    SELECT * FROM diversion_data_view WHERE region = 'Central';


