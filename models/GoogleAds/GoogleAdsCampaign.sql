{% if var('GoogleAdsCampaign') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("gads_campaign_tbl_ptrn","gads_campaign_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'customer_currency_code') }},
        a.* from (
        select
        {{extract_nested_value("customer","currency_code","string")}} as customer_currency_code,
        {{extract_nested_value("campaign","name","string")}} as campaign_name,
        {{extract_nested_value("campaign","id","string")}} as campaign_id,
        {{extract_nested_value("campaign","advertising_channel_type","string")}} as campaign_advertising_channel_type,
        {{extract_nested_value("campaign","advertising_channel_sub_type","string")}} as campaign_advertising_channel_sub_type,
        {{extract_nested_value("metrics","clicks","int")}} as clicks,
        {{extract_nested_value("metrics","conversions","numeric")}} as conversions,
        {{extract_nested_value("metrics","cost_micros","string")}} as cost_micros,
        {{extract_nested_value("metrics","conversions_value","numeric")}} as conversions_value,
        {{extract_nested_value("metrics","impressions","int")}} as impressions,
        {{extract_nested_value("segments","date","date")}} as date,
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
	    from {{i}} a
            {{unnesting("customer")}}
            {{unnesting("campaign")}}
            {{unnesting("metrics")}}
            {{unnesting("segments")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('gads_campaign_lookback') }},0) from {{ this }})
            {% endif %}
            qualify dense_rank() over (partition by {{extract_nested_value("segments","date","date")}},{{extract_nested_value("campaign","id","string")}}, 
            {{extract_nested_value("campaign","name","string")}} order by {{daton_batch_runtime()}} desc) = 1
        ) a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and customer_currency_code = c.to_currency_code  
            {% endif %}
{% if not loop.last %} union all {% endif %}
{% endfor %}
    