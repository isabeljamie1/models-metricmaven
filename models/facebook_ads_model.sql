CREATE OR REPLACE TABLE metricmaven_prod.facebook_ads
AS (
    with ads as (
    SELECT
        name as ad_name,
        id as ad_id,
        adset_id,
        campaign_id,
        JSON_EXTRACT_SCALAR(creative, '$.id') creative_id
    FROM `$projectID.metricmaven.$sourceName_$clientName_$clientId_ads`
),
ads_creative as (
    SELECT 
        thumbnail_data_url,
        body,
        title,
        thumbnail_url,
        name creative_name,
        id creative_id,
        REGEXP_EXTRACT(url_tags, 'utm_source=([^&]*)') AS utm_source,
        REGEXP_EXTRACT(url_tags, 'utm_medium=([^&]*)') AS utm_medium,
        REGEXP_EXTRACT(url_tags, 'utm_campaign=([^&]*)') AS utm_campaign,
    FROM `$projectID.metricmaven.$sourceName_$clientName_$clientId_ad_creatives`
),
insights as (
    SELECT 
        date_start,
        campaign_name,
        adset_name,
        ad_name,
        campaign_id,
        adset_id,
        ad_id,
        spend,
        impressions,
        clicks,
        IFNULL((SELECT SUM(CAST(JSON_EXTRACT_SCALAR(c, '$.value') as int64)) FROM UNNEST(JSON_EXTRACT_ARRAY(conversions, "$")) as c),0) conversions 
    FROM `$projectID.metricmaven.$sourceName_$clientName_$clientId_ads_insights`
)

SELECT i.*, a.* EXCEPT(ad_id, adset_id, campaign_id), ac.* EXCEPT(creative_id) FROM insights i 
LEFT JOIN ads a 
USING(ad_id)
LEFT JOIN ads_creative ac
USING(creative_id)
WHERE spend>0 or impressions>0 or clicks>0 or conversions>0;
)