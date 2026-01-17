-- Kiểm tra xem có ai dùng Booster không (Tìm các dòng có giá trị > 0)
SELECT 
    event_time,
    user_id,
    event_name,
    level_id,
    -- Trích xuất thử vài loại booster phổ biến
    (raw_json->>'booster_Hammer')::int as hammer,
    (raw_json->>'booster_Magnet')::int as magnet,
    (raw_json->>'booster_Add')::int as add_moves,
	(raw_json->>'booster_Ck')::int as add_moves,
    raw_json -- Xem full để đối chiếu
FROM view_game_stats_cleaned
WHERE 
    -- Chỉ check các event kết thúc màn chơi
    event_name IN ('missionComplete', 'missionFail') 
    AND (
        -- Lọc ra dòng nào mà có ít nhất 1 booster > 0
        (raw_json->>'booster_Hammer')::int > 0 OR
        (raw_json->>'booster_Magnet')::int > 0 OR
        (raw_json->>'booster_Add')::int > 0 OR
        (raw_json->>'booster_Unlock')::int > 0
    )
ORDER BY event_time DESC
LIMIT 10;