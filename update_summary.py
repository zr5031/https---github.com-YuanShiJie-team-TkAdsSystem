from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import func
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import requests
import json
from config import DB_CONFIG, TIKTOK_CONFIG, APP_CONFIG
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship
import time
import os
from decimal import Decimal

# 设置代理
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'
# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    "?charset=utf8mb4"
)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class CampaignInfo(Base):
    __tablename__ = 'campaign_info'
    id = Column(Integer, primary_key=True)
    tiktok_campaign_id = Column(String(32))
    name = Column(String(255))
    status = Column(Integer)
    create_time = Column(DateTime)
    total_spend = Column(Float(6,2), default=0)
    total_installs = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    total_impressions = Column(Integer, default=0)
    total_purchases = Column(Integer, default=0)
    start_date = Column(Date)
    end_date = Column(Date)
    cpi = Column(Float(6,2), default=0)
    cpm = Column(Float(6,2), default=0)
    cpc = Column(Float(6,2), default=0)
    cpa = Column(Float(6,2), default=0)
    ctr = Column(Float(6,2), default=0)
    cvr = Column(Float(6,2), default=0)
    metrics = relationship('CampaignMetrics', backref='campaign', lazy=True)

class CampaignMetrics(Base):
    __tablename__ = 'campaign_metrics'
    id = Column(Integer, primary_key=True)
    campaign_id = Column(Integer, ForeignKey('campaign_info.id'))
    installs = Column(Integer)
    spend = Column(Float)
    date = Column(Date)
    clicks = Column(Integer)
    impressions = Column(Integer)
    purchase_count = Column(Integer)

def calculate_rate(numerator, denominator, multiplier=1):
    """安全地计算比率"""
    try:
        # 转换为float类型
        numerator = float(numerator or 0)
        denominator = float(denominator or 0)
        
        if denominator > 0:  # 使用 > 0 而不是 != 0
            return round((numerator / denominator) * multiplier, 4)
        return 0
    except Exception as e:
        print(f"计算比率错误: {numerator} / {denominator} * {multiplier}")
        print(f"错误信息: {str(e)}")
        return 0

def disable_campaign(tiktok_campaign_id: str):
    """关停TikTok广告"""
    url = f"{TIKTOK_CONFIG['api_base_url']}/adgroup/status/update/"
    
    payload = json.dumps({
        "advertiser_id": TIKTOK_CONFIG['advertiser_id'],
        "adgroup_ids": [tiktok_campaign_id],
        "operation_status": "DISABLE"
    })
    
    headers = {
        'Access-Token': TIKTOK_CONFIG['access_token'],
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        
        result = response.json()
        if result['code'] == 0 and result['message'] == 'OK':
            return True
        else:
            print(f"关停广告失败 {tiktok_campaign_id}: {result['message']}")
            return False
            
    except Exception as e:
        print(f"关停广告失败 {tiktok_campaign_id}: {str(e)}")
        return False

def check_and_disable_campaigns(campaigns):
    """检查并关停不达标的广告计划"""
    campaigns_to_disable = []
    
    print("\n=== 检查广告计划 ===")
    for campaign in campaigns:
        # 跳过已经关停或冻结的计划
        if campaign.status != 1:  # 不是开启状态
            continue
            
        spend = float(campaign.total_spend or 0)
        installs = int(campaign.total_installs or 0)
        
        should_disable = False
        reasons = []
        
        if spend > 2 and installs == 0:
            reasons.append("花费过高无安装")
            should_disable = True
            
        if installs > 0 and campaign.cpi > 2:
            reasons.append("CPI过高")
            should_disable = True

        if should_disable:
            campaigns_to_disable.append((campaign, reasons))

    if campaigns_to_disable:
        print(f"\n🚫 需要关停 {len(campaigns_to_disable)} 个计划:")
        for campaign, reasons in campaigns_to_disable:
            print(f"\n计划: {campaign.name} ({campaign.tiktok_campaign_id})")
            print(f"花费: ${campaign.total_spend:.2f} | 安装: {campaign.total_installs} | "
                  f"CPI: ${campaign.cpi:.2f} | CTR: {campaign.ctr:.2f}%")
            print(f"原因: {', '.join(reasons)}")
            
            if disable_campaign(campaign.tiktok_campaign_id):
                print(f"✅ 已关停")
            else:
                print(f"❌ 关停失败")
    else:
        print("\n✅ 所有计划运行正常")

def convert_utc_to_local(utc_str):
    """将UTC时间字符串转换为UTC+8"""
    utc_time = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S')
    local_time = utc_time + timedelta(hours=8)
    return local_time

def batch_campaigns(campaign_ids, batch_size=10):
    """将campaign_ids分批"""
    for i in range(0, len(campaign_ids), batch_size):
        yield campaign_ids[i:i + batch_size]

def fetch_tiktok_data(campaign_ids):
    """获取TikTok API数据"""
    url = "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/"
    
    payload = json.dumps({
        "advertiser_id": TIKTOK_CONFIG['advertiser_id'],
        "filtering": {
            "adgroup_ids": campaign_ids
        }
    })
    
    headers = {
        'Access-Token': TIKTOK_CONFIG['access_token'],
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers, data=payload)
    return response.json()['data']['list']

def convert_status(tiktok_status):
    """转换TikTok状态为数字状态"""
    status_map = {
        'ENABLE': 1,   # 开启
        'DISABLE': 0,  # 关停
        'FROZEN': 2    # 冻结
    }
    return status_map.get(tiktok_status, 0)  # 默认返回0

def update_summary():
    start_time = datetime.now()  # 添加开始时间
    try:
        session = Session()
        print(f"\n=== 开始更新数据 ({start_time.strftime('%Y-%m-%d %H:%M:%S')}) ===")
        
        # 1. 获取所有campaign_ids
        all_campaigns = session.query(CampaignInfo).all()
        all_campaign_ids = [c.tiktok_campaign_id for c in all_campaigns]
        total_campaigns = len(all_campaign_ids)
        
        print(f"总计 {total_campaigns} 个广告计划需要更新")
        
        # 2. 分批获取TikTok数据并更新
        updated_campaigns = []
        for batch_num, batch_ids in enumerate(batch_campaigns(all_campaign_ids), 1):
            print(f"\n处理第 {batch_num} 批数据 ({len(batch_ids)} 个计划)")
            
            # 获取TikTok数据
            tiktok_data = fetch_tiktok_data(batch_ids)
            campaign_api_data = {item['adgroup_id']: item for item in tiktok_data}
            
            # 更新每个campaign的API数据
            for campaign_id in batch_ids:
                campaign = next((c for c in all_campaigns if c.tiktok_campaign_id == campaign_id), None)
                if campaign and campaign_id in campaign_api_data:
                    api_data = campaign_api_data[campaign_id]
                    # 转换状态为数字
                    campaign.status = convert_status(api_data['operation_status'])
                    campaign.create_time = convert_utc_to_local(api_data['create_time'])
                    updated_campaigns.append(campaign)
            
            session.commit()
            print(f"完成第 {batch_num} 批数据更新")
        
        # 3. 计算汇总指标
        print("\n开始计算汇总指标...")
        summary = session.query(
            CampaignInfo.id,
            CampaignInfo.tiktok_campaign_id,
            CampaignInfo.name,
            func.sum(CampaignMetrics.spend).label('total_spend'),
            func.sum(CampaignMetrics.installs).label('total_installs'),
            func.sum(CampaignMetrics.clicks).label('total_clicks'),
            func.sum(CampaignMetrics.impressions).label('total_impressions'),
            func.sum(CampaignMetrics.purchase_count).label('total_purchases'),
            func.min(CampaignMetrics.date).label('start_date'),
            func.max(CampaignMetrics.date).label('end_date')
        ).join(
            CampaignMetrics,
            CampaignInfo.id == CampaignMetrics.campaign_id
        ).group_by(
            CampaignInfo.id,
            CampaignInfo.tiktok_campaign_id,
            CampaignInfo.name
        ).all()
        
        # 4. 更新汇总数据
        for row in summary:
            campaign = session.get(CampaignInfo, row.id)
            if campaign:
                campaign.total_spend = row.total_spend
                campaign.total_installs = row.total_installs
                campaign.total_clicks = row.total_clicks
                campaign.total_impressions = row.total_impressions
                campaign.total_purchases = row.total_purchases
                campaign.start_date = row.start_date
                campaign.end_date = row.end_date
                campaign.cpm = calculate_rate(row.total_spend, row.total_impressions, 1000)
                campaign.cpc = calculate_rate(row.total_spend, row.total_clicks)
                campaign.cpi = calculate_rate(row.total_spend, row.total_installs)
                campaign.cpa = calculate_rate(row.total_spend, row.total_purchases)
                campaign.ctr = calculate_rate(row.total_clicks, row.total_impressions, 100)
                campaign.cvr = calculate_rate(row.total_installs, row.total_clicks, 100)
        
        session.commit()
        
        # 5. 检查需要关停的计划
        check_and_disable_campaigns(updated_campaigns)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"=== 更新完成 (耗时: {duration:.2f}秒) ===\n")
        
    except Exception as e:
        print(f"❌ 更新失败: {str(e)}")
        session.rollback()
        session.close()
        raise

def init_scheduler():
    """初始化定时任务"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        update_summary, 
        'interval', 
        minutes=5,
        id='update_campaign_stats'
    )
    scheduler.start()
    print("定时任务已启动，每5分钟执行一次更新")

if __name__ == '__main__':
    # 先执行一次
    update_summary()
    # 启动定时任务
    init_scheduler()
    
    try:
        # 保持程序运行
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("\n停止定时任务")