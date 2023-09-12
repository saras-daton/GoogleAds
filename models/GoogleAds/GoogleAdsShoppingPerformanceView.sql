
{% if var('GoogleAdsShoppingPerformanceView') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

{% set table_name_query %}
{{set_table_name('%googleads%shopping_performance_view')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}

{% if execute %}
    {# Return the first column #}
    {% set results_list = results.columns[0].values() %}
    {% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
    {% set results_list = [] %}
    {% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
        {% if var('get_brandname_from_tablename_flag') %}
            {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
        {% else %}
            {% set brand = var('default_brandname') %}
        {% endif %}

        {% if var('get_storename_from_tablename_flag') %}
            {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
        {% else %}
            {% set store = var('default_storename') %}
        {% endif %}


    SELECT * {{exclude()}} (row_num)
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then a.currency_code else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        coalesce({{extract_nested_value("customer","resource_name","string")}},'NA') as customer_resource_name,
        {{extract_nested_value("customer","currency_code","string")}} as currency_code,
        {{extract_nested_value("campaign","resource_name","string")}} as campaign_resource_name,
        {{extract_nested_value("campaign","name","string")}} as campaign_name,
        coalesce({{extract_nested_value("campaign","id","string")}},'NA') as campaign_id,
        {{extract_nested_value("campaign","advertising_channel_type","string")}} as campaign_advertising_channel_type,
        {{extract_nested_value("campaign","advertising_channel_sub_type","string")}} as campaign_advertising_channel_sub_type,
        {{extract_nested_value("ad_group","resource_name","string")}} as ad_group_resource_name,
        coalesce({{extract_nested_value("ad_group","id","string")}},'NA') as ad_group_id,
        {{extract_nested_value("ad_group","name","string")}} as ad_group_name,
        {{extract_nested_value("metrics","clicks","INT64")}} as clicks,
        {{extract_nested_value("metrics","conversions_value","NUMERIC")}} as conversions_value,
        {{extract_nested_value("metrics","conversions","NUMERIC")}} as conversions,
        {{extract_nested_value("metrics","cost_micros","string")}} as cost_micros,
        {{extract_nested_value("metrics","impressions","INT64")}} as impressions,
        cast(coalesce({{extract_nested_value("segments","date","string")}},'NA') as date) as date,
        coalesce({{extract_nested_value("segments","product_item_id","string")}},'NA') as product_item_id,
        coalesce({{extract_nested_value("segments","product_title","string")}},'NA') as product_title,
        {{extract_nested_value("shopping_performance_view","resource_name","string")}} as shopping_performance_view_resource_name,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            Dense_Rank() OVER (PARTITION BY {{extract_nested_value("customer","resource_name","string")}}, {{extract_nested_value("segments","date","date")}}, 
            {{extract_nested_value("ad_group","id","string")}},  {{extract_nested_value("campaign","id","string")}}, {{extract_nested_value("segments","product_item_id","string")}}, 
            {{extract_nested_value("segments","product_title","string")}} order by {{daton_batch_runtime()}} desc) row_num
	    from {{i}} 
            {{unnesting("customer")}}
            {{unnesting("campaign")}}
            {{unnesting("ad_group")}}
            {{unnesting("metrics")}}
            {{unnesting("segments")}}
            {{unnesting("shopping_performance_view")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and currency_code = c.to_currency_code  
            {% endif %}
    )
        where row_num = 1
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
