with 
feed_insights as (
    -- đọc từ bảng staging vật lý, đã được tối ưu
    select * from {{ ref('stg_facebook__feed_insights') }}
),

video_insights as (
    -- đọc từ bảng staging vật lý, đã được tối ưu
    select * from {{ ref('stg_facebook__video_insights') }}
),

joined as (
    select
        feed_insights.*,
        coalesce(video_insights.views, feed_insights.views) as combined_views,
        coalesce(video_insights.avg_watch_time, feed_insights.avg_watch_time) as combined_avg_watch_time

    from feed_insights
    left join video_insights 
        on feed_insights.video_id = video_insights.video_id 
        and feed_insights.timestamp = video_insights.timestamp
),

final_select as (
    select
        platform_id,
        account_id,
        platform_account_id,
        page_id,
        platform_page_id,
        content_id,
        platform_content_id,
        timestamp,
        video_id,
        combined_views as views,
        coalesce(toInt64(impressions), 0) as impressions,
        reactions as reactions,
        comments as comments,
        shares as shares,
        saves as saves,
        coalesce(toFloat64(combined_avg_watch_time), 0) as avg_watch_time,
        paid_views as paid_views,
        organic_views as organic_views,
        paid_impressions as paid_impressions,
        organic_impressions as organic_impressions,
        _raw,
        _crawl_at
    from joined
)

select * from final_select