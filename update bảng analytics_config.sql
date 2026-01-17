UPDATE analytics_config
SET updated_at = NOW(),
    config_json = '{
    "events": {
        "start": ["missionStart", "missionStart_Daily", "missionStart_WeeklyQuestTutor"],
        "win": ["missionComplete", "missionComplete_Daily", "missionComplete_WeeklyQuestTutor"],
        "progress": ["missionProgress"],  
        "fail": ["missionFail", "missionFail_Daily", "missionFail_WeeklyQuestTutor"],
        "transaction": {
            "real_currency": ["iapSuccess", "firstIAP"],
            "virtual_currency_exclude": ["iapSuccess", "firstIAP", "iapPurchase", "priceSpendLevel"], 
            "offer_and_reward": ["FirstReward", "adsRewardComplete", "iapOfferGet", "dailyReward"]
        }
    },
    "boosters": [
        {"key": "booster_Hammer", "name": "Hammer 🔨", "type": "booster"},
        {"key": "booster_Magnet", "name": "Magnet 🧲", "type": "booster"},
        {"key": "booster_Add", "name": "Add Moves ➕", "type": "booster"},
        {"key": "booster_Unlock", "name": "Unlock 🔓", "type": "booster"},
        {"key": "booster_Clear", "name": "Clear 🧹", "type": "booster"},
        {"key": "revive_boosterClear", "name": "Revive 💖", "type": "revive"}
    ],
    "currency": {
        "real": ["VND", "USD", "₫", "$"],
        "virtual": ["Coin", "Gem"]
    }
}'::jsonb
WHERE app_id = 1;