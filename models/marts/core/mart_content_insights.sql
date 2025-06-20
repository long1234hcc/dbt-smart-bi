select * from {{ ref('int_facebook__content_joined') }}

-- Áp dụng logic tăng trưởng để chỉ nạp các dòng mới vào bảng mart
{% if is_incremental() %}

  where _crawl_at > (select max(_crawl_at) from {{ this }})

{% endif %}