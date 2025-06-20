WITH cte_pre_media_insights AS
         (SELECT JSONExtractString(_airbyte_data, 'page_id')                                             AS page_id,
                 JSONExtractRaw(JSONExtractRaw(_airbyte_data, 'media'), 'media_info_id')                 AS media_info,
                 JSONExtractString(media_info, 'id')                                                     AS content_id,
                 JSONExtractString(media_info, 'media_product_type')                                     AS media_product_type,
                 JSONExtractString(media_info, 'media_type')                                             AS media_type,
                 arrayMap(x -> JSONExtractString(x, 'name'), JSONExtractArrayRaw(_airbyte_data, 'data')) AS metric_keys,
                 arrayMap(x -> toInt64OrNull(JSON_VALUE(JSONExtractRaw(x, 'values'), '$[0].value')),
                          JSONExtractArrayRaw(_airbyte_data, 'data'))                                    AS metric_values,
                 _airbyte_data                                                                           AS _raw,
                 _airbyte_extracted_at                                                                   AS _crawl_at
          FROM {{ source('instagram_raw_data', 'raw_instagram_meida_insights') }}),
     cte_media_insights AS
         (SELECT page_id,
                 content_id,
                 media_product_type,
                 media_type,
                 2                                                                     AS platform_id,
                 'INSTAGRAM'                                                           AS platform_name,
                 metric_values[indexOf(metric_keys, 'comments')]                       AS comments,
                 metric_values[indexOf(metric_keys, 'ig_reels_avg_watch_time')]        AS ig_reels_avg_watch_time,
                 metric_values[indexOf(metric_keys, 'ig_reels_video_view_total_time')] AS ig_reels_video_view_total_time,
                 metric_values[indexOf(metric_keys, 'likes')]                          AS likes,
                 metric_values[indexOf(metric_keys, 'reach')]                          AS reach,
                 metric_values[indexOf(metric_keys, 'saved')]                          AS saved,
                 metric_values[indexOf(metric_keys, 'shares')]                         AS shares,
                 metric_values[indexOf(metric_keys, 'total_interactions')]             AS total_interactions,
                 metric_values[indexOf(metric_keys, 'views')]                          AS views,
                 metric_values[indexOf(metric_keys, 'follows')]                        AS follows,
                 metric_values[indexOf(metric_keys, 'profile_activity')]               AS profile_activity,
                 metric_values[indexOf(metric_keys, 'profile_visits')]                 AS profile_visits,
                 metric_values[indexOf(metric_keys, 'impressions')]                    AS impressions,
                 _raw,
                 _crawl_at
          FROM cte_pre_media_insights),
     cte_ig_media AS
         (SELECT cmis.platform_id                              AS platform_id,
                 cmis.platform_name                            AS platform_name,
                 cmis.page_id                                  AS page_id,
                 cmis.content_id                               AS content_id,
                 cmis.media_product_type                       AS media_product_type,
                 cmis.media_type                               AS media_type,
                 cmh.platform_account_id                       AS account_id,
                 cmis.comments                                 AS comments,
                 round(cmis.ig_reels_avg_watch_time / 1000, 2) AS ig_reels_avg_watch_time,
                 cmis.ig_reels_video_view_total_time           AS ig_reels_video_view_total_time,
                 cmis.likes                                    AS likes,
                 cmis.reach                                    AS reach,
                 cmis.saved                                    AS saved,
                 cmis.shares                                   AS shares,
                 cmis.total_interactions                       AS total_interactions,
                 cmis.views                                    AS views,
                 cmis.follows                                  AS follows,
                 cmis.profile_activity                         AS profile_activity,
                 cmis.profile_visits                           AS profile_visits,
                 cmis.impressions                              AS impressions,
                 cmis._raw                                     AS _raw,
                 cmis._crawl_at                                AS _crawl_at
          FROM cte_media_insights AS cmis
                   LEFT JOIN {{ ref('stg_instagram__metadata') }} AS cmh ON cmis.content_id = cmh.platform_content_id)
SELECT cim.platform_id                               AS platform_id,
       concat(cim.platform_id, '__', cim.account_id) AS account_id,
       cim.account_id                                AS platform_account_id,
       concat(cim.platform_id, '__', cim.page_id)    AS page_id,
       cim.page_id                                   AS platform_page_id,
       concat(cim.platform_id, '__', cim.content_id) AS content_id,
       cim.content_id                                AS platform_content_id,
       toStartOfHour(cim._crawl_at)                  AS timestamp,
       cim.views                                     AS views,
       coalesce(toInt64(cim.impressions),0)                   AS impressions,
       cim.likes                                     AS reactions,
       cim.comments                                  AS comments,
       cim.shares                                    AS shares,
       0                                             AS saves,
       coalesce(cim.ig_reels_avg_watch_time,0)                   AS avg_watch_time,
       0                                             AS paid_views,
       0                                             AS organic_views,
       0                                             AS paid_impressions,
       0                                             AS organic_impressions,
       cim._raw                                      AS _raw,
       cim._crawl_at                                 AS _crawl_at
FROM cte_ig_media AS cim
WHERE (views > 0)
  AND (platform_account_id != '')



{% if is_incremental() %}
    and _crawl_at > (select max(_crawl_at) from {{ this }})
{% endif %}