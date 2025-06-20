WITH cte_media_info AS
         (SELECT JSONExtractString(_airbyte_data, 'id')                                                        AS content_id,
                 2                                                                                             AS platform_id,
                 'INSTAGRAM'                                                                                   AS platform_name,
                 JSONExtractString(_airbyte_data, 'caption')                                                   AS caption,
                 extractAll(caption, '#(?:[\\wÀ-ỹđĐ_]+)')                                                      AS tags,
                 JSONExtractBool(_airbyte_data, 'is_comment_enabled')                                          AS is_comment_enabled,
                 JSONExtractBool(_airbyte_data, 'is_shared_to_feed')                                           AS is_shared_to_feed,
                 JSONExtractString(_airbyte_data, 'like_count')                                                AS like_count,
                 JSONExtractString(_airbyte_data, 'media_product_type')                                        AS media_product_type,
                 JSONExtractString(_airbyte_data, 'media_type')                                                AS media_type,
                 JSONExtractString(_airbyte_data, 'media_url')                                                 AS media_url,
                 JSONExtractString(JSONExtractString(_airbyte_data, 'owner'), 'id')                            AS owner_id,
                 JSONExtractString(_airbyte_data, 'permalink')                                                 AS permalink,
                 JSONExtractString(_airbyte_data, 'thumbnail_url')                                             AS thumbnail_url,
                 JSONExtractString(_airbyte_data, 'timestamp')                                                 AS timestamp,
                 JSONExtractString(_airbyte_data, 'username')                                                  AS username,
                 JSONExtractString(_airbyte_data, 'page_id')                                                   AS page_id,
                 JSONExtractString(_airbyte_data, 'media_id', 'account_information',
                                   'profile_picture_url')                                                      AS old_page_thumbnail_url,
                 JSONExtractString(_airbyte_data, 'media_id', 'account_information', 'picture', 'data',
                                   'url')                                                                      AS new_page_thumbnail_url,
                 JSONExtractString(_airbyte_data, 'media_id', 'account_information',
                                   'name')                                                                     AS page_name,
                 _airbyte_data                                                                                 AS _raw,
                 _airbyte_extracted_at                                                                         AS _crawl_at
          FROM {{ source ('instagram_raw_data', 'raw_instagram_metadata') }})
SELECT cmi.platform_id,
       cmi.platform_name,
       concat(cmi.platform_id, '__', cmi.owner_id)                                         AS account_id,
       cmi.owner_id                                                                        AS platform_account_id,
       cmi.username                                                                        AS user_name,
       cmi.username                                                                        AS account_name,
       cmi.tags,
       concat(cmi.platform_id, '__', cmi.page_id)                                          AS page_id,
       cmi.page_id                                                                         AS platform_page_id,
       cmi.page_name                                                                       AS page_name,
       concat(cmi.platform_id, '__', cmi.content_id)                                       AS content_id,
       cmi.content_id                                                                      AS platform_content_id,
       multiIf((cmi.media_product_type = 'FEED') AND (cmi.media_type = 'IMAGE'), 'IMAGE',
               (cmi.media_product_type = 'FEED') AND (cmi.media_type = 'CAROUSEL_ALBUM'), 'CAROUSEL_ALBUM',
               (cmi.media_product_type = 'FEED') AND (cmi.media_type NOT IN ('IMAGE', 'CAROUSEL_ALBUM')), 'N/ A',
               cmi.media_product_type = 'REELS', 'REELS', NULL)                            AS content_type,
       parseDateTime64BestEffortOrNull(cmi.timestamp)                                      AS created_time,
       cmi.caption,
       concat(cmi.old_page_thumbnail_url, cmi.new_page_thumbnail_url)                      AS page_thumbnail_url,
       if(cmi.media_type IN ('IMAGE', 'CAROUSEL_ALBUM'), cmi.media_url, cmi.thumbnail_url) AS thumbnail_url,
       cmi.permalink,
       account_id                                                                          AS created_user,
       cmi._raw,
       cmi._crawl_at
FROM cte_media_info AS cmi


QUALIFY ROW_NUMBER() OVER(PARTITION BY content_id ORDER BY _crawl_at DESC) = 1



