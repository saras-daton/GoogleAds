{% if var('GoogleAdsSearchTermReport') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("gads_search_term_tbl_ptrn","gads_search_term_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {{extract_nested_value("customer","resource_name","string")}} as customer_resource_name,
        {{extract_nested_value("customer","id","string")}} as customer_id,
        {{extract_nested_value("campaign","resource_name","string")}} as campaign_resource_name,
        {{extract_nested_value("campaign","name","string")}} as campaign_name,
        {{extract_nested_value("campaign","id","string")}} as campaign_id,
        {{extract_nested_value("ad_group","resource_name","string")}} as ad_group_resource_name,
        {{extract_nested_value("ad_group","id","string")}} as ad_group_id,
        {{extract_nested_value("ad_group","name","string")}} as ad_group_name,
        {{extract_nested_value("metrics","clicks","int")}} as clicks,
        {{extract_nested_value("metrics","conversions","numeric")}} as conversions,
        {{extract_nested_value("metrics","cost_micros","string")}} as cost_micros,
        {{extract_nested_value("metrics","conversions_value","numeric")}} as conversions_value,
        {{extract_nested_value("metrics","impressions","int")}} as impressions,
        {{extract_nested_value("metrics","conversions_value_bn","bignumeric")}} as conversions_value_bn,
        {{extract_nested_value("search_term_view","resource_name","string")}} as search_term_view_resource_name,
        {{extract_nested_value("search_term_view","status","string")}} as search_term_view_status,
        {{extract_nested_value("search_term_view","search_term","string")}} as search_term,
        {{extract_nested_value("search_term_view","ad_group","string")}} as search_term_view_ad_group,
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
            {{unnesting("search_term_view")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('gads_search_term_lookback') }},0) from {{ this }})
            {% endif %}
            qualify dense_rank() over (partition by date, device, campaign_id, ad_group_id, search_term order by {{daton_batch_runtime()}} desc) = 1       
{% if not loop.last %} union all {% endif %}
{% endfor %}
    