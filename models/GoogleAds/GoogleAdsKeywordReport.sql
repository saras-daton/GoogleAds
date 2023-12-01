{% if var('GoogleAdsKeywordReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("gads_keyword_tbl_ptrn","gads_keyword_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {{extract_nested_value("customer","resource_name","string")}} as customer_resource_name,
        {{extract_nested_value("customer","id","string")}} as customer_id,
        {{extract_nested_value("customer","status","string")}} as customer_status,
        {{extract_nested_value("campaign","resource_name","string")}} as campaign_resource_name,
        {{extract_nested_value("campaign","status","string")}} as campaign_status,
        {{extract_nested_value("campaign","name","string")}} as campaign_name,
        {{extract_nested_value("campaign","id","string")}} as campaign_id,
        {{extract_nested_value("ad_group","resource_name","string")}} as ad_group_resource_name,
        {{extract_nested_value("ad_group","status","string")}} as ad_group_status,
        {{extract_nested_value("ad_group","type","string")}} as ad_group_type,
        {{extract_nested_value("ad_group","id","string")}} as ad_group_id,
        {{extract_nested_value("ad_group","name","string")}} as ad_group_name,
        {{extract_nested_value("ad_group","base_ad_group","string")}} as ad_group_base_ad_group,
        {{extract_nested_value("ad_group","campaign","string")}} as ad_group_base_campaign,
        {{extract_nested_value("metrics","clicks","int")}} as clicks,
        {{extract_nested_value("metrics","cost_micros","string")}} as cost_micros,
        {{extract_nested_value("metrics","conversions_value","numeric")}} as conversions_value,
        {{extract_nested_value("metrics","impressions","int")}} as impressions,
        {{extract_nested_value("metrics","conversions_value_bn","bignumeric")}} as conversions_value_bn,
        {{extract_nested_value("keyword","match_type","string")}} as keyword_match_type,
        {{extract_nested_value("keyword","text","string")}} as keyword_text,
        {{extract_nested_value("ad_group_criterion","resource_name","string")}} as ad_group_criterion_resource_name,
        {{extract_nested_value("ad_group_criterion","criterion_id","string")}} as ad_group_criterion_criterion_id,
        {{extract_nested_value("ad_group_criterion","ad_group","string")}} as ad_group_criterion_ad_group,
        {{extract_nested_value("ad_group_criterion","criterion_id_in","int")}} as ad_group_criterion_criterion_id_in,
        {{extract_nested_value("ad_group_criterion","criterion_id_dt","date")}} as ad_group_criterion_criterion_id_dt,
        {{extract_nested_value("keyword_view","resource_name","string")}} as keyword_view_resource_name,
        {{extract_nested_value("segments","device","string")}} as device,
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
            {{unnesting("ad_group")}}
            {{unnesting("ad_group_criterion")}}
            {{multi_unnesting("ad_group_criterion","keyword")}}
            {{unnesting("keyword_view")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('gads_keyword_lookback') }},0) from {{ this }})
            {% endif %}
            qualify dense_rank() over (partition by date, device, campaign_id, ad_group_id, keyword_text, keyword_match_type order by {{daton_batch_runtime()}} desc) = 1       
{% if not loop.last %} union all {% endif %}
{% endfor %}
    