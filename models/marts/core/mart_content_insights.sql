-- models/marts/core/fct_content_insights.sql

with unioned_sources as (

    {{ dbt_utils.union_relations(
        relations=[
            ref('int_facebook__content_joined'),
            ref('stg_instagram__media_insights'),
            ref('stg_tiktok__video_insights')
        ]
    ) }}

)

select * from unioned_sources


