
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
select coalesce(max(_daton_batch_runtime) - 2592000000,0) from {{ this }}
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

        {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}

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
        {{extract_nested_value("customer","resource_name","string")}} as customer_resource_name
        {{extract_nested_value("customer","currency_code","string")}} as customer_currency_code
        {{extract_nested_value("campaign","resource_name","string")}} as campaign_resource_name
        {{extract_nested_value("campaign","name","string")}} as campaign_name
        {{extract_nested_value("campaign","id","string")}} as campaign_id
        {{extract_nested_value("campaign","advertising_channel_type","string")}} as campaign_advertising_channel_type
        {{extract_nested_value("campaign","advertising_channel_sub_type","string")}} as campaign_advertising_channel_sub_type
        {{extract_nested_value("ad_group","resource_name","string")}} as ad_group_resource_name
        {{extract_nested_value("ad_group","currency_code","string")}} as currency_code
        {{extract_nested_value("ad_group","currency_code","string")}} as currency_code
        {{extract_nested_value("metrics","clicks","int")}} as clicks
        {{extract_nested_value("metrics","conversions","numeric")}} as conversions
        {{extract_nested_value("metrics","cost_micros","numeric")}} as cost_micros
        {{extract_nested_value("metrics","conversions_value","numeric")}} as conversions_value
        {{extract_nested_value("metrics","impressions","numeric")}} as impressions
        {{extract_nested_value("segments","date","date")}} as date
        {{extract_nested_value("segments","product_item_id","string")}} as product_item_id
        {{extract_nested_value("segments","product_title","string")}} as product_title
        {{extract_nested_value("shopping_performance_view","resource_name","string")}} as shopping_performance_view_resource_name
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        
	    from {{i}} 
            {{unnesting("CUSTOMER")}}
            {{unnesting("CAMPAIGN")}}
            {{unnesting("AD_GROUP")}}
            {{unnesting("METRICS")}}
            {{unnesting("SEGMENTS")}}
            {{unnesting("SHOPPING_PERFORMANCE_VIEW")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}

            qualify
            {% if target.type =='snowflake' %}
                dense_rank() over (partition by CUSTOMER.VALUE:resource_name, SEGMENTS.VALUE:date, AD_GROUP.VALUE:id, CAMPAIGN.VALUE:id, SEGMENTS.VALUE:product_item_id, SEGMENTS.VALUE:product_title order by {{daton_batch_runtime()}} desc) = 1
            {% else %}
                dense_rank() over (partition by CUSTOMER.resource_name, SEGMENTS.date, AD_GROUP.id,CAMPAIGN.id,SEGMENTS.product_item_id,SEGMENTS.product_title order by {{daton_batch_runtime()}} desc) = 1
            {% endif %}
    
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and currency_code = c.to_currency_code  
            {% endif %}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
