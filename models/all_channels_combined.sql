CREATE OR REPLACE TABLE metricmaven_prod.all_channels_combined
AS (
with facebook_ads as (
    SELECT 
        date,
        campaign_name,
        adset_name,
        ad_name,
        campaign_id,
        adset_id,
        ad_id,
        spend,
        impressions,
        clicks,
        conversions,
        utm_source,
        utm_medium,
        utm_campaign
    FROM $projectID.metricmaven.$sourceName_$clientName_$clientId_facebook_ads
),
google_ads as (
    SELECT 
        date,
        campaign_name,
        adset_name,
        'N/A' as ad_name,
        campaign_id,
        adset_id,
        'N/A' as ad_id,
        spend,
        impressions,
        clicks,
        conversions,
        utm_source,
        utm_medium,
        utm_campaign
    FROM $projectID.metricmaven.$sourceName_$clientName_$clientId_google_ads
),
linkedin_ads as (
    SELECT 
        date,
        campaign_name,
        adset_name,
        'N/A' as ad_name,
        campaign_id,
        adset_id,
        'N/A' as ad_id,
        spend,
        impressions,
        clicks,
        conversions,
        '' as utm_source,
        '' as utm_medium,
        '' as utm_campaign
    FROM $projectID.metricmaven.$sourceName_$clientName_$clientId_linkedin_ads
)

SELECT * FROM facebook_ads
UNION ALL 
SELECT * FROM google_ads
UNION ALL 
SELECT * FROM linkedin_ads;
);