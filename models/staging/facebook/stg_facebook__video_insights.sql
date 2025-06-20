WITH cte_video AS (SELECT '1'                                                                                       AS platform_id,
                          JSONExtractString(_airbyte_data, 'page_id')                                             AS page_id,
                          JSONExtractString(_airbyte_data, 'id')                                                  AS video_id,
                          JSONExtract(_airbyte_data, 'video_insights', 'data',
                                      'Array(Tuple(name String, values Array(Tuple(value Float))))')              as metrics,
                          arrayFilter(x -> (x.name = 'fb_reels_total_plays'), metrics)[1].values[1].value         as fb_reels_total_plays,
                          arrayFilter(x -> (x.name = 'total_video_play_count'), metrics)[1].values[1].value       as total_video_play_count,
                          greatest(toInt64(fb_reels_total_plays),
                                   toInt64(total_video_play_count))                                               AS views,
                          toStartOfHour(_airbyte_extracted_at)                                                    AS timestamp,

                          arrayFilter(x -> (x.name = 'total_video_avg_time_watched'), metrics)[1].values[1].value as total_video_avg_time_watched,
                          arrayFilter(x -> (x.name = 'post_video_avg_time_watched'), metrics)[1].values[1].value  as post_video_avg_time_watched,
                          greatest(toInt64(total_video_avg_time_watched),
                                   toInt64(post_video_avg_time_watched))                                          as avg_watch_time,
                          _airbyte_data                                                                           as _raw,
                          _airbyte_extracted_at                                                                   as _crawl_at

                   FROM {{ source('facebook_raw_data', 'raw_video_insights') }})

SELECT platform_id,
       page_id,
       video_id,
       timestamp,
       views,
       avg_watch_time,
       _raw,
       _crawl_at
FROM cte_video




{% if is_incremental() %}
    where _crawl_at > (select max(_crawl_at) from {{ this }})
{% endif %}
