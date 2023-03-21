
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
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
        {% if target.type =='snowflake' %}
        CUSTOMER.VALUE:resource_name::VARCHAR as customer_resource_name,
        COALESCE(CUSTOMER.VALUE:currency_code::VARCHAR,'') as currency_code,
        CAMPAIGN.VALUE:resource_name::VARCHAR as campaign_resource_name,
        CAMPAIGN.VALUE:name::VARCHAR as campaign_name,
        CAMPAIGN.VALUE:id::VARCHAR as campaign_id,
        AD_GROUP.VALUE:resource_name::VARCHAR as ad_group_resource_name,
        AD_GROUP.VALUE:id::VARCHAR as ad_group_id,
        AD_GROUP.VALUE:name::VARCHAR as ad_group_name,
        METRICS.VALUE:clicks::NUMERIC as clicks,
        METRICS.VALUE:conversions as conversions,
        METRICS.VALUE:cost_micros::FLOAT as cost_micros,
        METRICS.VALUE:conversions_value AS conversions_value,
        METRICS.VALUE:impressions::NUMERIC as impressions,
        SEGMENTS.VALUE:date::DATE as date,
        SEGMENTS.VALUE:product_item_id::VARCHAR as product_item_id,
        SEGMENTS.VALUE:product_title::VARCHAR as product_title,
        SHOPPING_PERFORMANCE_VIEW.VALUE:resource_name::VARCHAR as shopping_performance_view_resource_name,
        {% else %}
        CUSTOMER.resource_name as customer_resource_name,
        COALESCE(CUSTOMER.currency_code,'') as currency_code,
        CAMPAIGN.resource_name as campaign_resource_name,
        CAMPAIGN.name as campaign_name,
        COALESCE(CAMPAIGN.id,0) as campaign_id,
        AD_GROUP.resource_name as ad_group_resource_name,
        AD_GROUP.id as ad_group_id,
        AD_GROUP.name as ad_group_name,
        METRICS.clicks,
        METRICS.conversions,
        METRICS.cost_micros,
        METRICS.conversions_value,
        METRICS.impressions,
        SEGMENTS.date,
        COALESCE(SEGMENTS.product_item_id,0) as product_item_id,
        COALESCE(SEGMENTS.product_title,'') as product_title,
        SHOPPING_PERFORMANCE_VIEW.resource_name as shopping_performance_view_resource_name,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
            Dense_Rank() OVER (PARTITION BY CUSTOMER.VALUE:resource_name, SEGMENTS.VALUE:date, AD_GROUP.VALUE:id, CAMPAIGN.VALUE:id, SEGMENTS.VALUE:product_item_id, SEGMENTS.VALUE:product_title order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
            Dense_Rank() OVER (PARTITION BY CUSTOMER.resource_name, SEGMENTS.date, AD_GROUP.id,CAMPAIGN.id,SEGMENTS.product_item_id,SEGMENTS.product_title order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
	    from {{i}} 
            {{unnesting("CUSTOMER")}}
            {{unnesting("CAMPAIGN")}}
            {{unnesting("AD_GROUP")}}
            {{unnesting("METRICS")}}
            {{unnesting("SEGMENTS")}}
            {{unnesting("SHOPPING_PERFORMANCE_VIEW")}}
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
