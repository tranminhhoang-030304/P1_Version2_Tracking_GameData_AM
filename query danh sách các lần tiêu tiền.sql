WITH Spending AS (
    -- Lấy danh sách các lần tiêu tiền (Coin Price > 0)
    SELECT 
        user_id, 
        level_id, 
        event_time, 
        (raw_json->>'coin_price')::int as price
    FROM view_game_stats_cleaned
    WHERE event_name = 'priceSpendLevel' 
      AND (raw_json->>'coin_price')::int > 0
),
Usage AS (
    -- Lấy danh sách dùng Booster ngay sau đó (trong cùng level)
    SELECT 
        user_id, 
        level_id, 
        event_time,
        -- Kiểm tra xem dùng cái gì
        CASE 
            WHEN (raw_json->>'booster_Hammer')::int > 0 THEN 'Hammer 🔨'
            WHEN (raw_json->>'booster_Magnet')::int > 0 THEN 'Magnet 🧲'
            WHEN (raw_json->>'booster_Add')::int > 0 THEN 'Add Moves ➕'
            WHEN (raw_json->>'booster_Unlock')::int > 0 THEN 'Unlock 🔓'
            WHEN (raw_json->>'booster_Clear')::int > 0 THEN 'Clear 🧹'
            WHEN (raw_json->>'revive_boosterClear')::int > 0 THEN 'Revive ❤️'
            ELSE 'Unknown' 
        END as used_item
    FROM view_game_stats_cleaned
    WHERE event_name IN ('missionComplete', 'missionFail')
)
-- Kết hợp lại để tìm thủ phạm
SELECT 
    S.price as coin_price,
    U.used_item,
    COUNT(*) as frequency -- Số lần khớp lệnh
FROM Spending S
JOIN Usage U ON S.user_id = U.user_id 
    AND S.level_id = U.level_id
    -- Chỉ lấy cặp sự kiện xảy ra gần nhau (trong vòng 5 phút)
    AND ABS(EXTRACT(EPOCH FROM (U.event_time - S.event_time))) < 300 
GROUP BY 1, 2
ORDER BY frequency DESC;