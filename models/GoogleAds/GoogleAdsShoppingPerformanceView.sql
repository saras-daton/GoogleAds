
{% if var('GoogleAdsShoppingPerformanceView') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
 --depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("gads_shopping_performance_view_tbl_ptrn","gads_shopping_performance_view_exclude_tbl_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {{ currency_conversion('c.value', 'c.from_currency_code', 'customer_currency_code') }},
        a.* from (
        select
        {{extract_nested_value("call_reporting_setting","call_reporting_enabled","boolean")}} as call_reporting_enabled,
        {{extract_nested_value("call_reporting_setting","call_conversion_reporting_enabled","boolean")}} as call_conversion_reporting_enabled,
        {{extract_nested_value("call_reporting_setting","call_conversion_action","string")}} as call_conversion_action,
        {{extract_nested_value("conversion_tracking_setting","conversion_tracking_id","string")}} as conversion_tracking_id,
        {{extract_nested_value("conversion_tracking_setting","accepted_customer_data_terms","boolean")}} as conversion_tracking_setting_accepted_customer_data_terms,
        {{extract_nested_value("conversion_tracking_setting","conversion_tracking_status","string")}} as conversion_tracking_setting_conversion_tracking_status,
        {{extract_nested_value("conversion_tracking_setting","google_ads_conversion_customer","string")}} as conversion_tracking_setting_google_ads_conversion_customer,
        {{extract_nested_value("remarketing_setting","google_global_site_tag","string")}} as remarketing_setting_google_global_site_tag,
        {{extract_nested_value("customer","resource_name","string")}} as customer_resource_name,
        {{extract_nested_value("customer","pay_per_conversion_eligibility_failure_reasons","string")}} as pay_per_conversion_eligibility_failure_reasons,
        {{extract_nested_value("customer","id","string")}} as customer_id,
        {{extract_nested_value("customer","descriptive_name","string")}} as customer_descriptive_name,
        {{extract_nested_value("customer","currency_code","string")}} as customer_currency_code,
        {{extract_nested_value("customer","time_zone","string")}} as customer_time_zone,
        {{extract_nested_value("customer","final_url_suffix","string")}} as customer_final_url_suffix,
        {{extract_nested_value("customer","auto_tagging_enabled","boolean")}} as customer_auto_tagging_enabled,
        {{extract_nested_value("customer","has_partners_badge","boolean")}} as customer_has_partners_badge,
        {{extract_nested_value("customer","manager","boolean")}} as customer_manager,
        {{extract_nested_value("customer","test_account","boolean")}} as customer_test_account,
        {{extract_nested_value("customer","status","string")}} as customer_status,
        {{extract_nested_value("network_settings","target_google_search","boolean")}} as network_settings_target_google_search,
        {{extract_nested_value("network_settings","target_search_network","boolean")}} as network_settings_target_search_network,
        {{extract_nested_value("network_settings","target_content_network","boolean")}} as network_settings_target_content_network,
        {{extract_nested_value("network_settings","target_partner_search_network","boolean")}} as network_settings_target_partner_search_network,
        {{extract_nested_value("maximize_conversion_value","target_roas","numeric")}} as maximize_conversion_value_target_roas,
        {{extract_nested_value("maximize_conversion_value","target_roas_bn","bignumeric")}} as maximize_conversion_value_target_roas_bn,
        {{extract_nested_value("shopping_setting","merchant_id","string")}} as shopping_setting_merchant_id,
        {{extract_nested_value("shopping_setting","sales_country","string")}} as shopping_setting_sales_country,
        {{extract_nested_value("shopping_setting","campaign_priority","string")}} as shopping_setting_campaign_priority,
        {{extract_nested_value("shopping_setting","enable_local","string")}} as shopping_setting_enable_local,
        {{extract_nested_value("geo_target_type_setting","positive_geo_target_type","string")}} as geo_target_type_setting_positive_geo_target_type,
        {{extract_nested_value("geo_target_type_setting","negative_geo_target_type","string")}} as geo_target_type_setting_negative_geo_target_type,
        {{extract_nested_value("performance_max_upgrade","performance_max_campaign","string")}} as performance_max_campaign,
        {{extract_nested_value("performance_max_upgrade","status","string")}} as performance_max_upgrade_status,
        {{extract_nested_value("campaign","resource_name","string")}} as campaign_resource_name,
        {{extract_nested_value("campaign","status","string")}} as campaign_status,
        {{extract_nested_value("campaign","ad_serving_optimization_status","string")}} as campaign_ad_serving_optimization_status,
        {{extract_nested_value("campaign","advertising_channel_type","string")}} as campaign_advertising_channel_type,
        {{extract_nested_value("campaign","advertising_channel_sub_type","string")}} as campaign_advertising_channel_sub_type,
        {{extract_nested_value("campaign","experiment_type","string")}} as campaign_experiment_type,
        {{extract_nested_value("campaign","serving_status","string")}} as campaign_serving_status,
        {{extract_nested_value("campaign","bidding_strategy_type","string")}} as campaign_bidding_strategy_type,
        {{extract_nested_value("campaign","payment_mode","string")}} as campaign_payment_mode,
        {{extract_nested_value("campaign","base_campaign","string")}} as campaign_base_campaign,
        {{extract_nested_value("campaign","name","string")}} as campaign_name,
        {{extract_nested_value("campaign","id","string")}} as campaign_id,
        {{extract_nested_value("campaign","labels","string")}} as campaign_labels,
        {{extract_nested_value("campaign","campaign_budget","string")}} as campaign_campaign_budget,
        {{extract_nested_value("campaign","start_date","date")}} as campaign_start_date,
        {{extract_nested_value("campaign","end_date","date")}} as campaign_end_date,
        {{extract_nested_value("campaign","final_url_suffix","string")}} as campaign_final_url_suffix,
        {{extract_nested_value("campaign","id_nu","numeric")}} as campaign_id_nu,
        {{extract_nested_value("explorer_auto_optimizer_setting","opt_in","boolean")}} as explorer_auto_optimizer_setting_opt_in,
        {{extract_nested_value("ad_group","resource_name","string")}} as ad_group_resource_name,
        {{extract_nested_value("ad_group","status","string")}} as ad_group_status,
        {{extract_nested_value("ad_group","type","string")}} as ad_group_type,
        {{extract_nested_value("ad_group","id","string")}} as ad_group_id,
        {{extract_nested_value("ad_group","name","string")}} as ad_group_name,
        {{extract_nested_value("ad_group","base_ad_group","string")}} as ad_group_base_ad_group,
        {{extract_nested_value("ad_group","campaign","string")}} as ad_group_campaign,
        {{extract_nested_value("ad_group","cpc_bid_micros","int")}} as ad_group_cpc_bid_micros,
        {{extract_nested_value("ad_group","cpm_bid_micros","int")}} as ad_group_cpm_bid_micros,
        {{extract_nested_value("ad_group","target_cpa_micros","int")}} as ad_group_target_cpa_micros,
        {{extract_nested_value("ad_group","cpv_bid_micros","int")}} as ad_group_cpv_bid_micros,
        {{extract_nested_value("ad_group","target_cpm_micros","int")}} as ad_group_target_cpm_micros,
        {{extract_nested_value("ad_group","effective_target_cpa_micros","int")}} as ad_group_effective_target_cpa_micros,
        {{extract_nested_value("metrics","clicks","int")}} as clicks,
        {{extract_nested_value("metrics","conversions","numeric")}} as conversions,
        {{extract_nested_value("metrics","cost_micros","numeric")}} as cost_micros,
        {{extract_nested_value("metrics","conversions_value","numeric")}} as conversions_value,
        {{extract_nested_value("metrics","impressions","numeric")}} as impressions,
        {{extract_nested_value("segments","date","date")}} as date,
        {{extract_nested_value("segments","product_item_id","string")}} as product_item_id,
        {{extract_nested_value("segments","product_title","string")}} as product_title,
        {{extract_nested_value("segments","product_item_id_st","string")}} as product_item_id_st,
        {{extract_nested_value("shopping_performance_view","resource_name","string")}} as shopping_performance_view_resource_name,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        
	    from {{i}}
            {{unnesting("CUSTOMER")}}
            {{multi_unnesting("customer","call_reporting_setting")}}
            {{multi_unnesting("customer","conversion_tracking_setting")}}
            {{multi_unnesting("customer","remarketing_setting")}}
            {{unnesting("CAMPAIGN")}}
            {{multi_unnesting("campaign","network_settings")}}
            {{multi_unnesting("campaign","maximize_conversion_value")}}
            {{multi_unnesting("campaign","shopping_setting")}}
            {{multi_unnesting("campaign","targeting_setting")}}
            {{multi_unnesting("campaign","geo_target_type_setting")}}
            {{multi_unnesting("campaign","performance_max_upgrade")}}
            {{unnesting("AD_GROUP")}}
            {{multi_unnesting("ad_group","explorer_auto_optimizer_setting")}}
            {{multi_unnesting("ad_group","targeting_setting")}}
            {{unnesting("METRICS")}}
            {{unnesting("SEGMENTS")}}
            {{unnesting("SHOPPING_PERFORMANCE_VIEW")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            where {{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{var('gads_shopping_performance_view_lookback') }},0) from {{ this }})
            {% endif %}

            qualify dense_rank() over (partition by {{extract_nested_value("customer","resource_name","string")}}, {{extract_nested_value("segments","date","date")}}, 
            {{extract_nested_value("ad_group","id","string")}},  {{extract_nested_value("campaign","id","string")}}, {{extract_nested_value("segments","product_item_id","string")}}, 
            {{extract_nested_value("segments","product_title","string")}} order by {{daton_batch_runtime()}} desc) = 1
        ) a
            {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(a.date) = c.date and customer_currency_code = c.to_currency_code  
            {% endif %}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
