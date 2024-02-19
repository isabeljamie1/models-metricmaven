CREATE OR REPLACE TABLE metricmaven_prod.linkedin_ads
AS (
with analytics as (
    SELECT 
        start_date date,
        pivotvalue creative_id,
        costinusd spend,
        impressions,
        clicks,
        externalWebsiteConversions
    FROM `$projectID.metricmaven.linkedin_ads_$clientName_$clientId_ad_creative_analytics`
),
campaigns as (
    SELECT
        name campaign_name,
        cast(id as string) campaign_id,
        REGEXP_EXTRACT(campaignGroup,'urn\\:li\\:sponsoredCampaignGroup\\:(.*)') campaign_group_id
    FROM `$projectID.metricmaven.linkedin_ads_$clientName_$clientId_campaigns`
),
campaign_groups as (
    SELECT 
        name as campaign_group_name,
        cast(id as string) campaign_group_id,
    FROM `$projectID.metricmaven.linkedin_ads_$clientName_$clientId_campaign_groups`
),
creatives as (
    SELECT 
        id as creative_id,
        REGEXP_EXTRACT(campaign,'urn\\:li\\:sponsoredCampaign\\:(.*)') campaign_id
    FROM `$projectID.metricmaven.linkedin_ads_$clientName_$clientId_creatives`
),
data as (
    SELECT  
        date,
        campaign_group_name campaign_name,
        campaign_name adset_name,
        campaign_group_id,
        campaign_id,
        spend,
        impressions,
        clicks,
        externalWebsiteConversions conversions
    FROM analytics a 
    LEFT JOIN creatives using(creative_id) 
    LEFT JOIN campaigns using(campaign_id)
    LEFT JOIN campaign_groups using(campaign_group_id)
)

SELECT * FROM data
)