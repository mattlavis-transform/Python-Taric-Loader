-- DROP SCHEMA utils;

CREATE SCHEMA utils AUTHORIZATION postgres;

-- utils.import_files definition

-- Drop table

-- DROP TABLE utils.import_files;

CREATE TABLE utils.import_files (
	import_file varchar(255) NULL,
	import_started timestamp NULL,
	import_completed timestamp NULL,
	status varchar NULL
);

-- utils.measures_real_end_dates source

CREATE OR REPLACE VIEW utils.measures_real_end_dates
AS SELECT m.measure_sid,
    m.goods_nomenclature_item_id,
    m.geographical_area_id,
    m.measure_type_id,
    m.measure_generating_regulation_id,
    m.ordernumber,
    m.reduction_indicator,
    m.additional_code_type_id,
    m.additional_code_id,
    m.additional_code_type_id || m.additional_code_id::text AS additional_code,
    m.measure_generating_regulation_role,
    m.justification_regulation_role,
    m.justification_regulation_id,
    m.stopped_flag,
    m.geographical_area_sid,
    m.goods_nomenclature_sid,
    m.additional_code_sid,
    m.export_refund_nomenclature_sid,
    to_char(m.validity_start_date, 'YYYY-MM-DD'::text) AS validity_start_date,
    LEAST(to_char(m.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.effective_end_date, 'YYYY-MM-DD'::text)) AS validity_end_date,
    'Published'::text AS status
   FROM measures m,
    base_regulations r
  WHERE m.measure_generating_regulation_id::text = r.base_regulation_id::text
UNION
 SELECT m.measure_sid,
    m.goods_nomenclature_item_id,
    m.geographical_area_id,
    m.measure_type_id,
    m.measure_generating_regulation_id,
    m.ordernumber,
    m.reduction_indicator,
    m.additional_code_type_id,
    m.additional_code_id,
    m.additional_code_type_id || m.additional_code_id::text AS additional_code,
    m.measure_generating_regulation_role,
    m.justification_regulation_role,
    m.justification_regulation_id,
    m.stopped_flag,
    m.geographical_area_sid,
    m.goods_nomenclature_sid,
    m.additional_code_sid,
    m.export_refund_nomenclature_sid,
    to_char(m.validity_start_date, 'YYYY-MM-DD'::text) AS validity_start_date,
    LEAST(to_char(m.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.effective_end_date, 'YYYY-MM-DD'::text)) AS validity_end_date,
    'Published'::text AS status
   FROM measures m,
    modification_regulations r
  WHERE m.measure_generating_regulation_id::text = r.modification_regulation_id::text;

  CREATE OR REPLACE FUNCTION utils.goods_nomenclature_export_new(pchapter text, key_date character varying)
 RETURNS TABLE(goods_nomenclature_sid integer, goods_nomenclature_item_id character varying, producline_suffix character varying, validity_start_date timestamp without time zone, validity_end_date timestamp without time zone, description text, number_indents integer, chapter text, node text, leaf text, significant_digits integer)
 LANGUAGE plpgsql
AS $function$

#variable_conflict use_column

DECLARE key_date2 date := key_date::date;

BEGIN

IF pchapter = '' THEN
pchapter = '%';
END IF;

/* temporary table contains results of query plus a placeholder column for leaf - defaulted to 0
node column has the significant digits used to find child nodes having the same significant digits.
The basic query retrieves all current (and future) nomenclature with indents and descriptions */

DROP TABLE IF EXISTS tmp_nomenclature;

CREATE TEMP TABLE tmp_nomenclature ON COMMIT DROP AS
SELECT gn.goods_nomenclature_sid, gn.goods_nomenclature_item_id, gn.producline_suffix, gn.validity_start_date, gn.validity_end_date, 
regexp_replace(gnd.description, E'[\\n\\r]+', ' ', 'g') as description,
gni.number_indents, 
left (gn.goods_nomenclature_item_id, 2) "chapter",
REGEXP_REPLACE (gn.goods_nomenclature_item_id, '(00)+$', '') AS "node",
'0' AS "leaf",
CASE
WHEN RIGHT(gn.goods_nomenclature_item_id, 8) = '00000000' THEN 2
WHEN RIGHT(gn.goods_nomenclature_item_id, 6) = '000000' THEN 4
WHEN RIGHT(gn.goods_nomenclature_item_id, 4) = '0000' THEN 6
WHEN RIGHT(gn.goods_nomenclature_item_id, 2) = '00' THEN 8
ELSE 10
END As significant_digits
FROM goods_nomenclatures gn
JOIN goods_nomenclature_descriptions gnd ON gnd.goods_nomenclature_sid = gn.goods_nomenclature_sid
JOIN goods_nomenclature_description_periods gndp ON gndp.goods_nomenclature_description_period_sid = gnd.goods_nomenclature_description_period_sid
JOIN goods_nomenclature_indents gni ON gni.goods_nomenclature_sid = gn.goods_nomenclature_sid

WHERE (gn.validity_end_date IS NULL OR gn.validity_end_date >= key_date2)
AND gn.goods_nomenclature_item_id LIKE pchapter
AND gndp.goods_nomenclature_description_period_sid IN
(
    SELECT MAX (gndp2.goods_nomenclature_description_period_sid)
    FROM goods_nomenclature_description_periods gndp2
    WHERE gndp2.goods_nomenclature_sid = gnd.goods_nomenclature_sid
    AND gndp2.validity_start_date <= key_date2
)
AND gni.goods_nomenclature_indent_sid IN
(
    SELECT MAX (gni2.goods_nomenclature_indent_sid)
    FROM goods_nomenclature_indents gni2
    WHERE gni2.goods_nomenclature_sid = gn.goods_nomenclature_sid
    AND gni2.validity_start_date <= key_date2
);



/* Index to speed up child node matching - need to perf test to see if any use */
CREATE INDEX t1_i_nomenclature 
ON tmp_nomenclature (goods_nomenclature_sid, goods_nomenclature_item_id);

/* Cursor loops through result set to identify if nodes are leaf and updates the flag if so */
declare cur_nomenclature CURSOR FOR SELECT * FROM tmp_nomenclature;

BEGIN

	FOR nom_record IN cur_nomenclature LOOP
		Raise Notice 'goods nomenclature item id %', nom_record.goods_nomenclature_item_id;
		
		/* Leaf nodes have to have pls of 80 and no children having the same nomenclature code */
		IF nom_record.producline_suffix = '80' THEN
			IF LENGTH (nom_record.node) = 10 OR NOT EXISTS (SELECT 1 
			FROM tmp_nomenclature 
			WHERE goods_nomenclature_item_id LIKE CONCAT(nom_record.node,'%')
			AND goods_nomenclature_item_id <> nom_record.goods_nomenclature_item_id) THEN
			
				UPDATE tmp_nomenclature tn
				SET leaf = '1'
				WHERE goods_nomenclature_sid = nom_record.goods_nomenclature_sid;
			
			END IF;
		END IF;
	
	END LOOP;

END;

RETURN QUERY 
SELECT * FROM tmp_nomenclature;

END;

$function$
;
CREATE OR REPLACE FUNCTION utils.clear_data(from_date character varying, my_import_file character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $function$

DECLARE d timestamp;
BEGIN
	d = TO_TIMESTAMP(from_date, 'YYYY-MM-DD HH24:MI:SS');

	delete from utils.import_files where import_file = my_import_file;

	DELETE FROM additional_code_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM additional_code_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM additional_code_type_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM additional_code_type_measure_types_oplog WHERE operation_date >= d;
	DELETE FROM additional_code_types_oplog WHERE operation_date >= d;
	DELETE FROM additional_codes_oplog WHERE operation_date >= d;
	DELETE FROM base_regulations_oplog WHERE operation_date >= d;
	DELETE FROM certificate_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM certificate_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM certificate_type_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM certificate_types_oplog WHERE operation_date >= d;
	DELETE FROM certificates_oplog WHERE operation_date >= d;
	DELETE FROM complete_abrogation_regulations_oplog WHERE operation_date >= d;
	DELETE FROM duty_expression_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM duty_expressions_oplog WHERE operation_date >= d;
	DELETE FROM export_refund_nomenclatures_oplog WHERE operation_date >= d;
	DELETE FROM export_refund_nomenclature_indents_oplog WHERE operation_date >= d;
	DELETE FROM export_refund_nomenclature_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM export_refund_nomenclature_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM footnote_association_erns_oplog WHERE operation_date >= d;
	DELETE FROM explicit_abrogation_regulations_oplog WHERE operation_date >= d;
	DELETE FROM footnote_association_additional_codes_oplog WHERE operation_date >= d;
	DELETE FROM footnote_association_goods_nomenclatures_oplog WHERE operation_date >= d;
	DELETE FROM footnote_association_measures_oplog WHERE operation_date >= d;
	DELETE FROM footnote_association_meursing_headings_oplog WHERE operation_date >= d;
	DELETE FROM footnote_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM footnote_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM footnote_type_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM footnote_types_oplog WHERE operation_date >= d;
	DELETE FROM footnotes_oplog WHERE operation_date >= d;
	DELETE FROM fts_regulation_actions_oplog WHERE operation_date >= d;
	DELETE FROM full_temporary_stop_regulations_oplog WHERE operation_date >= d;
	DELETE FROM geographical_area_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM geographical_area_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM geographical_area_memberships_oplog WHERE operation_date >= d;
	DELETE FROM geographical_areas_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_description_periods_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_group_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_indents_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_origins_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclature_successors_oplog WHERE operation_date >= d;
	DELETE FROM goods_nomenclatures_oplog WHERE operation_date >= d;
	DELETE FROM languages_oplog WHERE operation_date >= d;
	DELETE FROM language_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measure_action_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measure_actions_oplog WHERE operation_date >= d;
	DELETE FROM measure_components_oplog WHERE operation_date >= d;
	DELETE FROM measure_condition_code_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measure_condition_codes_oplog WHERE operation_date >= d;
	DELETE FROM measure_condition_components_oplog WHERE operation_date >= d;
	DELETE FROM measure_conditions_oplog WHERE operation_date >= d;
	DELETE FROM measure_excluded_geographical_areas_oplog WHERE operation_date >= d;
	DELETE FROM measure_partial_temporary_stops_oplog WHERE operation_date >= d;
	DELETE FROM measure_type_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measure_type_series_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measure_type_series_oplog WHERE operation_date >= d;
	DELETE FROM measure_types_oplog WHERE operation_date >= d;
	DELETE FROM measurement_unit_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measurement_unit_qualifier_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM measurement_unit_qualifiers_oplog WHERE operation_date >= d;
	DELETE FROM measurement_units_oplog WHERE operation_date >= d;
	DELETE FROM measurements_oplog WHERE operation_date >= d;
	DELETE FROM measures_oplog WHERE operation_date >= d;
	DELETE FROM meursing_additional_codes_oplog WHERE operation_date >= d;
	DELETE FROM meursing_heading_texts_oplog WHERE operation_date >= d;
	DELETE FROM meursing_headings_oplog WHERE operation_date >= d;
	DELETE FROM meursing_subheadings_oplog WHERE operation_date >= d;
	DELETE FROM meursing_table_cell_components_oplog WHERE operation_date >= d;
	DELETE FROM meursing_table_plans_oplog WHERE operation_date >= d;
	DELETE FROM modification_regulations_oplog WHERE operation_date >= d;
	DELETE FROM monetary_exchange_periods_oplog WHERE operation_date >= d;
	DELETE FROM monetary_exchange_rates_oplog WHERE operation_date >= d;
	DELETE FROM monetary_unit_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM monetary_units_oplog WHERE operation_date >= d;
	DELETE FROM nomenclature_group_memberships_oplog WHERE operation_date >= d;
	DELETE FROM prorogation_regulation_actions_oplog WHERE operation_date >= d;
	DELETE FROM prorogation_regulations_oplog WHERE operation_date >= d;
	DELETE FROM publication_sigles_oplog WHERE operation_date >= d;
	DELETE FROM quota_associations_oplog WHERE operation_date >= d;
	DELETE FROM quota_balance_events_oplog WHERE operation_date >= d;
	DELETE FROM quota_blocking_periods_oplog WHERE operation_date >= d;
	DELETE FROM quota_critical_events_oplog WHERE operation_date >= d;
	DELETE FROM quota_definitions_oplog WHERE operation_date >= d;
	DELETE FROM quota_exhaustion_events_oplog WHERE operation_date >= d;
	DELETE FROM quota_order_number_origin_exclusions_oplog WHERE operation_date >= d;
	DELETE FROM quota_order_number_origins_oplog WHERE operation_date >= d;
	DELETE FROM quota_order_numbers_oplog WHERE operation_date >= d;
	DELETE FROM quota_reopening_events_oplog WHERE operation_date >= d;
	DELETE FROM quota_suspension_periods_oplog WHERE operation_date >= d;
	DELETE FROM quota_unblocking_events_oplog WHERE operation_date >= d;
	DELETE FROM quota_unsuspension_events_oplog WHERE operation_date >= d;
	DELETE FROM regulation_replacements_oplog WHERE operation_date >= d;
	DELETE FROM regulation_role_types_oplog WHERE operation_date >= d;
	DELETE FROM regulation_role_type_descriptions_oplog WHERE operation_date >= d;
	DELETE FROM regulation_groups_oplog WHERE operation_date >= d;
	DELETE FROM regulation_group_descriptions_oplog WHERE operation_date >= d;

	DELETE FROM utils.workbasket_events WHERE created_at >= d;
	DELETE FROM utils.workbasket_item_events WHERE created_at >= d;
	DELETE FROM utils.workbasket_items WHERE created_at >= d;
	DELETE FROM utils.workbaskets WHERE created_at >= d;


END
$function$
;
CREATE OR REPLACE FUNCTION utils.rollback_file(import_filename character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $function$

BEGIN
	delete from utils.import_files where import_file = import_filename;

	delete from additional_code_description_periods_oplog where filename = import_filename;
	delete from additional_code_descriptions_oplog where filename = import_filename;
	delete from additional_code_type_descriptions_oplog where filename = import_filename;
	delete from additional_code_type_measure_types_oplog where filename = import_filename;
	delete from additional_code_types_oplog where filename = import_filename;
	delete from additional_codes_oplog where filename = import_filename;
	delete from base_regulations_oplog where filename = import_filename;
	delete from certificate_description_periods_oplog where filename = import_filename;
	delete from certificate_descriptions_oplog where filename = import_filename;
	delete from certificate_type_descriptions_oplog where filename = import_filename;
	delete from certificate_types_oplog where filename = import_filename;
	delete from certificates_oplog where filename = import_filename;
	delete from complete_abrogation_regulations_oplog where filename = import_filename;
	delete from duty_expression_descriptions_oplog where filename = import_filename;
	delete from duty_expressions_oplog where filename = import_filename;
	delete from export_refund_nomenclatures_oplog where filename = import_filename;
	delete from export_refund_nomenclature_indents_oplog where filename = import_filename;
	delete from export_refund_nomenclature_description_periods_oplog where filename = import_filename;
	delete from export_refund_nomenclature_descriptions_oplog where filename = import_filename;
	delete from footnote_association_erns_oplog where filename = import_filename;
	delete from explicit_abrogation_regulations_oplog where filename = import_filename;
	delete from footnote_association_additional_codes_oplog where filename = import_filename;
	delete from footnote_association_goods_nomenclatures_oplog where filename = import_filename;
	delete from footnote_association_measures_oplog where filename = import_filename;
	delete from footnote_association_meursing_headings_oplog where filename = import_filename;
	delete from footnote_description_periods_oplog where filename = import_filename;
	delete from footnote_descriptions_oplog where filename = import_filename;
	delete from footnote_type_descriptions_oplog where filename = import_filename;
	delete from footnote_types_oplog where filename = import_filename;
	delete from footnotes_oplog where filename = import_filename;
	delete from fts_regulation_actions_oplog where filename = import_filename;
	delete from full_temporary_stop_regulations_oplog where filename = import_filename;
	delete from geographical_area_description_periods_oplog where filename = import_filename;
	delete from geographical_area_descriptions_oplog where filename = import_filename;
	delete from geographical_area_memberships_oplog where filename = import_filename;
	delete from geographical_areas_oplog where filename = import_filename;
	delete from goods_nomenclature_description_periods_oplog where filename = import_filename;
	delete from goods_nomenclature_descriptions_oplog where filename = import_filename;
	delete from goods_nomenclature_group_descriptions_oplog where filename = import_filename;
	delete from goods_nomenclature_indents_oplog where filename = import_filename;
	delete from goods_nomenclature_origins_oplog where filename = import_filename;
	delete from goods_nomenclature_successors_oplog where filename = import_filename;
	delete from goods_nomenclatures_oplog where filename = import_filename;
	delete from languages_oplog where filename = import_filename;
	delete from language_descriptions_oplog where filename = import_filename;
	delete from measure_action_descriptions_oplog where filename = import_filename;
	delete from measure_actions_oplog where filename = import_filename;
	delete from measure_components_oplog where filename = import_filename;
	delete from measure_condition_code_descriptions_oplog where filename = import_filename;
	delete from measure_condition_codes_oplog where filename = import_filename;
	delete from measure_condition_components_oplog where filename = import_filename;
	delete from measure_conditions_oplog where filename = import_filename;
	delete from measure_excluded_geographical_areas_oplog where filename = import_filename;
	delete from measure_partial_temporary_stops_oplog where filename = import_filename;
	delete from measure_type_descriptions_oplog where filename = import_filename;
	delete from measure_type_series_descriptions_oplog where filename = import_filename;
	delete from measure_type_series_oplog where filename = import_filename;
	delete from measure_types_oplog where filename = import_filename;
	delete from measurement_unit_descriptions_oplog where filename = import_filename;
	delete from measurement_unit_qualifier_descriptions_oplog where filename = import_filename;
	delete from measurement_unit_qualifiers_oplog where filename = import_filename;
	delete from measurement_units_oplog where filename = import_filename;
	delete from measurements_oplog where filename = import_filename;
	delete from measures_oplog where filename = import_filename;
	delete from meursing_additional_codes_oplog where filename = import_filename;
	delete from meursing_heading_texts_oplog where filename = import_filename;
	delete from meursing_headings_oplog where filename = import_filename;
	delete from meursing_subheadings_oplog where filename = import_filename;
	delete from meursing_table_cell_components_oplog where filename = import_filename;
	delete from meursing_table_plans_oplog where filename = import_filename;
	delete from modification_regulations_oplog where filename = import_filename;
	delete from monetary_exchange_periods_oplog where filename = import_filename;
	delete from monetary_exchange_rates_oplog where filename = import_filename;
	delete from monetary_unit_descriptions_oplog where filename = import_filename;
	delete from monetary_units_oplog where filename = import_filename;
	delete from nomenclature_group_memberships_oplog where filename = import_filename;
	delete from prorogation_regulation_actions_oplog where filename = import_filename;
	delete from prorogation_regulations_oplog where filename = import_filename;
	delete from publication_sigles_oplog where filename = import_filename;
	delete from quota_associations_oplog where filename = import_filename;
	delete from quota_balance_events_oplog where filename = import_filename;
	delete from quota_blocking_periods_oplog where filename = import_filename;
	delete from quota_critical_events_oplog where filename = import_filename;
	delete from quota_definitions_oplog where filename = import_filename;
	delete from quota_exhaustion_events_oplog where filename = import_filename;
	delete from quota_order_number_origin_exclusions_oplog where filename = import_filename;
	delete from quota_order_number_origins_oplog where filename = import_filename;
	delete from quota_order_numbers_oplog where filename = import_filename;
	delete from quota_reopening_events_oplog where filename = import_filename;
	delete from quota_suspension_periods_oplog where filename = import_filename;
	delete from quota_unblocking_events_oplog where filename = import_filename;
	delete from quota_unsuspension_events_oplog where filename = import_filename;
	delete from regulation_replacements_oplog where filename = import_filename;
	delete from regulation_role_types_oplog where filename = import_filename;
	delete from regulation_role_type_descriptions_oplog where filename = import_filename;
	delete from regulation_groups_oplog where filename = import_filename;
	delete from regulation_group_descriptions_oplog where filename = import_filename;

END
$function$
;

DROP MATERIALIZED VIEW IF EXISTS utils.materialized_measures_real_end_dates;

CREATE MATERIALIZED VIEW utils.materialized_measures_real_end_dates
AS SELECT m.measure_sid,
    m.goods_nomenclature_item_id,
    m.geographical_area_id,
    m.measure_type_id,
    m.measure_generating_regulation_id,
    m.ordernumber,
    m.reduction_indicator,
    m.additional_code_type_id,
    m.additional_code_id,
    m.additional_code_type_id || m.additional_code_id::text AS additional_code,
    m.measure_generating_regulation_role,
    m.justification_regulation_role,
    m.justification_regulation_id,
    m.stopped_flag,
    m.geographical_area_sid,
    m.goods_nomenclature_sid,
    m.additional_code_sid,
    m.export_refund_nomenclature_sid,
    to_char(m.validity_start_date, 'YYYY-MM-DD'::text) AS validity_start_date,
    LEAST(to_char(m.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.effective_end_date, 'YYYY-MM-DD'::text)) AS validity_end_date,
    'Published'::text AS status
   FROM measures m,
    base_regulations r
  WHERE m.measure_generating_regulation_id::text = r.base_regulation_id::text
UNION
 SELECT m.measure_sid,
    m.goods_nomenclature_item_id,
    m.geographical_area_id,
    m.measure_type_id,
    m.measure_generating_regulation_id,
    m.ordernumber,
    m.reduction_indicator,
    m.additional_code_type_id,
    m.additional_code_id,
    m.additional_code_type_id || m.additional_code_id::text AS additional_code,
    m.measure_generating_regulation_role,
    m.justification_regulation_role,
    m.justification_regulation_id,
    m.stopped_flag,
    m.geographical_area_sid,
    m.goods_nomenclature_sid,
    m.additional_code_sid,
    m.export_refund_nomenclature_sid,
    to_char(m.validity_start_date, 'YYYY-MM-DD'::text) AS validity_start_date,
    LEAST(to_char(m.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.validity_end_date, 'YYYY-MM-DD'::text), to_char(r.effective_end_date, 'YYYY-MM-DD'::text)) AS validity_end_date,
    'Published'::text AS status
   FROM measures m,
    modification_regulations r
  WHERE m.measure_generating_regulation_id::text = r.modification_regulation_id::text;