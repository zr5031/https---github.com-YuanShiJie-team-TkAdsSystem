# 数据库配置
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '123456',
    'database': 'tiktok',
    'port': 3306
}

# TikTok API配置
TIKTOK_CONFIG = {
    'advertiser_id': '7368184445817487376',
    'access_token': '8305566fc996739125f8823b414ffd3e0642bc35',
    'api_base_url': 'https://business-api.tiktok.com/open_api/v1.3'
}

# 代理配置
PROXY_CONFIG = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}

# 应用配置
APP_CONFIG = {
    'port': 5000,
    'debug': True,
    'update_interval': 5  # 更新间隔（分钟）
} 