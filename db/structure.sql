select goods_nomenclature_item_id, producline_suffix, description from utils.goods_nomenclature_export_start_end_date('%', '2021-01-01', '2021-01-01')
order by 1, 2;

select goods_nomenclature_item_id, producline_suffix, description, number_indents, leaf
from utils.goods_nomenclature_export_start_end_date('44%', '2021-01-01', '2021-01-01')
order by 1, 2;

select * from goods_nomenclature_descriptions gnd where description ilike '%mask worn%'