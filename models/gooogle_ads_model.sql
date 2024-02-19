CREATE OR REPLACE TABLE metricmaven_prod.google_ads
AS (
with ad_groups as (
    SELECT 
        ad_group_id,
        REGEXP_EXTRACT(ad_group_final_url_suffix, 'utm_source=([^&]*)') utm_source,
        REGEXP_EXTRACT(ad_group_final_url_suffix, 'utm_medium=([^&]*)') utm_medium,
        REGEXP_EXTRACT(ad_group_final_url_suffix, 'utm_campaign=([^&]*)') utm_campaign
    FROM `$projectID.metricmaven.$sourceName_$clientName_$clientId_ad_groups`
),
ad_group_ad_report as (
    SELECT 
        segments_date date,
        campaign_name,
        campaign_id,
        ad_group_name,
        ad_group_id,
        SUM(metrics_cost_micros/1000000) spend,
        SUM(metrics_impressions) impressions,
        SUM(metrics_interactions) interactions,
        SUM(metrics_clicks) clicks,
        SUM(metrics_conversions) conversions 
    FROM 
        `$projectID.metricmaven.$sourceName_$clientName_$clientId_ad_group_ad_legacy`
    GROUP BY 1,2,3,4,5
),
campaign_report as (
    SELECT 
    segments_date date,
    campaign_name,
    campaign_id,
    'n/a' as ad_group_name,
    'n/a' as ad_group_id,
    SUM(metrics_cost_micros/1000000) spend,
    SUM(metrics_impressions) impressions,
    SUM(metrics_interactions) interactions,
    SUM(metrics_clicks) clicks,
    SUM(metrics_conversions) conversions,
    REGEXP_EXTRACT(campaign_final_url_suffix, 'utm_source=([^&]*)') utm_source,
    REGEXP_EXTRACT(campaign_final_url_suffix, 'utm_medium=([^&]*)') utm_medium,
    REGEXP_EXTRACT(campaign_final_url_suffix, 'utm_campaign=([^&]*)') utm_campaign,

    FROM `$projectID.metricmaven.$sourceName_$clientName_$clientId_campaign`
    GROUP BY 1,2,3,4,5,11,12,13 
),
campaign_report_pmax as (
    SELECT * FROM {{ ref('src__veritex__google_ads__campaign_report') }} where campaign_id not in (select campaign_id from ad_report)
),
data as (
    SELECT 
        ar.*, 
        ag.* EXCEPT(ad_group_id) 
    FROM ad_report ar 
    LEFT JOIN ad_groups ag 
    USING(ad_group_id)
    union all 
    SELECT * FROM campaign_report_pmax
)

SELECT DISTINCT * FROM data where spend>0 or impressions>0 or interactions>0 or clicks>0 or all_conversions>0;
)