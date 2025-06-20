with cte_feed_insights AS
         (SELECT 1                                                                                             AS platform_id,
                 JSONExtractString(_airbyte_data, 'page_id')                                                   AS platform_account_id,
                 concat(platform_id, '__', platform_account_id)                                                AS account_id,
                 splitByChar('_', JSONExtractString(_airbyte_data, 'id'))[-1]                                  AS platform_content_id,
                 concat(platform_id, '__', platform_content_id)                                                AS content_id,
                 account_id                                                                                    AS page_id,
                 platform_account_id                                                                           AS platform_page_id,
                 toInt64OrZero(JSONExtractString(_airbyte_data, 'shares', 'count'))                            as shares,
                 0                                                                                             AS saves,

                 JSONExtractString(_airbyte_data, 'id')                                                        as id,
                 JSONExtract(_airbyte_data, 'insights', 'data',
                             'Array(Tuple(id String,values Array(Tuple (value Float))))')                      as metrics,
                 arrayFilter(x -> (x.id like '%/post_impressions_unique/%'), metrics)[1].values[1].value         as impressions,
                 arrayFilter(x -> (x.id like '%/post_video_views_paid/%'), metrics)[1].values[1].value           as paid_views,
                 arrayFilter(x -> (x.id like '%/post_video_views_organic/%'), metrics)[1].values[1].value        as organic_views,
                 arrayFilter(x -> (x.id like '%/post_impressions_paid_unique/%'), metrics)[1].values[1].value    as paid_impressions,
                 arrayFilter(x -> (x.id like '%/post_impressions_organic_unique/%'), metrics)[1].values[1].value as organic_impressions,
                 arraySum(arrayMap(
                         x -> if(multiSearchAny(x.id,
                                                ['like_total', 'love_total', 'wow_total', 'haha_total', 'sorry_total', 'anger_total']),
                                 x.values[1].value, 0),
                         metrics))                                                                             AS reactions,
                 0                                                                                             AS views,
                 0                                                                                             AS avg_watch_time,
                 JSONExtractInt(_airbyte_data, 'comments', 'summary', 'total_count')         AS comments,
                 extract(JSONExtractString(_airbyte_data, 'permalink_url'),
                         '/(?:reel|videos)/(\\d+)')                                                            AS video_id,
                 _airbyte_extracted_at                                                                         AS _crawl_at,
                 toStartOfHour(_airbyte_extracted_at)                                                          AS timestamp,
                 _airbyte_data                                                                                 AS _raw
          FROM {{ source('facebook_raw_data', 'raw_feed_insights') }}   )
select
       platform_id,
       account_id,
       platform_account_id,
       page_id,
       platform_page_id,
       content_id,
       platform_content_id,
       timestamp,
       views,
       coalesce(toInt64(impressions),0) as impressions,
       reactions,
       comments,
       shares,
       saves,
       avg_watch_time,
       paid_views,
       organic_views,
       paid_impressions,
       organic_impressions,
       video_id,
       _raw,
       _crawl_at
from cte_feed_insights


{% if is_incremental() %}
    where _crawl_at > (select max(_crawl_at) from {{ this }})
{% endif %}