from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from config import DB_CONFIG, APP_CONFIG

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = (
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
)
db = SQLAlchemy(app)

class CampaignInfo(db.Model):
    __tablename__ = 'campaign_info'
    id = db.Column(db.Integer, primary_key=True)
    tiktok_campaign_id = db.Column(db.String(32))
    name = db.Column(db.String(255))
    status = db.Column(db.Integer)
    create_time = db.Column(db.DateTime)
    total_spend = db.Column(db.Float(6,2), default=0)
    total_installs = db.Column(db.Integer, default=0)
    total_clicks = db.Column(db.Integer, default=0)
    total_impressions = db.Column(db.Integer, default=0)
    total_purchases = db.Column(db.Integer, default=0)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    cpi = db.Column(db.Float(6,2), default=0)
    cpm = db.Column(db.Float(6,2), default=0)
    cpc = db.Column(db.Float(6,2), default=0)
    cpa = db.Column(db.Float(6,2), default=0)
    ctr = db.Column(db.Float(6,2), default=0)
    cvr = db.Column(db.Float(6,2), default=0)

def get_status_text(status):
    """获取状态文本"""
    status_map = {
        0: '已关停',
        1: '运行中',
        2: '已冻结'
    }
    return status_map.get(status, '未知')

@app.route('/')
def index():
    sort_by = request.args.get('sort', 'total_spend')
    order = request.args.get('order', 'desc')
    
    query = CampaignInfo.query
    
    # 处理特殊排序字段
    if sort_by == 'status':
        if order == 'asc':
            query = query.order_by(CampaignInfo.status.asc())
        else:
            query = query.order_by(CampaignInfo.status.desc())
    elif sort_by == 'create_time':
        if order == 'asc':
            query = query.order_by(CampaignInfo.create_time.asc())
        else:
            query = query.order_by(CampaignInfo.create_time.desc())
    else:
        sort_column = getattr(CampaignInfo, sort_by, CampaignInfo.total_spend)
        if order == 'asc':
            query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(sort_column.desc())
    
    campaigns = query.all()
    return render_template('index.html', 
                         campaigns=campaigns, 
                         current_sort=sort_by, 
                         current_order=order,
                         get_status_text=get_status_text)

if __name__ == '__main__':
    app.run(
        debug=APP_CONFIG['debug'], 
        port=APP_CONFIG['port']
    )
