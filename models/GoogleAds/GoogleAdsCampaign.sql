{% if var('GoogleAdsCampaign') %}
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
{{set_table_name('%googleads%campaign')}}    
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
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
        {% if target.type =='snowflake' %}
        COALESCE(CUSTOMER.VALUE:currency_code::VARCHAR,'') as currency_code,
        CAMPAIGN.VALUE:name::VARCHAR as campaign_name,
        COALESCE(CAMPAIGN.VALUE:id::VARCHAR,'') as campaign_id,
        CAMPAIGN.VALUE:advertising_channel_type::VARCHAR as campaign_advertising_channel_type,
        CAMPAIGN.VALUE:advertising_channel_sub_type::VARCHAR as campaign_advertising_channel_sub_type,
        METRICS.VALUE:clicks::NUMERIC as clicks,
        METRICS.VALUE:conversions as conversions,
        METRICS.VALUE:cost_micros::FLOAT as cost_micros,
        METRICS.VALUE:conversions_value AS conversions_value,
        METRICS.VALUE:impressions::NUMERIC as impressions,
        SEGMENTS.VALUE:date::DATE as date,
        {% else %}
        customer.currency_code,
        campaign.name as campaign_name,
        coalesce(campaign.id,0) as campaign_id,
        campaign.advertising_channel_type as campaign_advertising_channel_type,
        campaign.advertising_channel_sub_type as campaign_advertising_channel_sub_type,
        metrics.clicks,
        metrics.conversions,
        metrics.cost_micros,
        metrics.conversions_value,
        metrics.impressions,
        segments.date,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
            Dense_Rank() OVER (PARTITION BY SEGMENTS.VALUE:date, CAMPAIGN.VALUE:id, CAMPAIGN.VALUE:name  order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
            Dense_Rank() OVER (PARTITION BY SEGMENTS.date,CAMPAIGN.id, CAMPAIGN.name order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
	    from {{i}} 
            {{unnesting("customer")}}
            {{unnesting("campaign")}}
            {{unnesting("metrics")}}
            {{unnesting("segments")}}
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
    