import requests
import json
import datetime
from datetime import timedelta
import os
import urllib3
import ssl
import re  # 新增：用于易方达数据正则解析
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 核心修复：SSL 适配器 (解决 Hostname/Cert 冲突)
# ==========================================
class LegacySSLAdapter(HTTPAdapter):
    """
    1. 强制开启 OP_LEGACY_SERVER_CONNECT (解决 Unsafe Legacy Renegotiation)
    2. 显式禁用 check_hostname (解决 Cannot set verify_mode to CERT_NONE)
    """
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        
        # --- 关键修复开始 ---
        # 必须显式关闭 hostname 检查，否则 Python 不允许将 verify_mode 设为 CERT_NONE
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # --- 关键修复结束 ---
        
        # 允许旧版不安全连接
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        # 允许低安全级别的加密套件
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        
        self.poolmanager = urllib3.poolmanager.PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx
        )

# ==========================================
# 配置区域 (从 GitHub Secrets 读取)
# ==========================================
FEISHU_CONFIG = {
    "APP_ID": "cli_a9aac56abc78dbde",
    "APP_SECRET": "zYsXkFulzxMCrqnAjvPTiyVUWCIKFwS5",
    "APP_TOKEN": "Qurjbd950a7XzIsMFZrclwn5n9d",
    "TABLE_ID": "tblHts4IwRE8WCBB"
}

# ==========================================
# 飞书 API 模块
# ==========================================
class FeishuClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token = None
        self.token_expire_time = 0

    def get_tenant_access_token(self):
        if not self.app_id or not self.app_secret:
            print("❌ 错误: 环境变量缺失，请检查 GitHub Secrets。")
            return None
        if self.token and datetime.datetime.now().timestamp() < self.token_expire_time - 600:
            return self.token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {"app_id": self.app_id, "app_secret": self.app_secret}
        try:
            response = requests.post(url, headers=headers, json=data)
            resp_json = response.json()
            if resp_json.get("code") == 0:
                self.token = resp_json.get("tenant_access_token")
                self.token_expire_time = datetime.datetime.now().timestamp() + resp_json.get("expire", 7200)
                return self.token
            else:
                print(f"[Feishu Auth Error] {resp_json}")
                return None
        except Exception as e:
            print(f"[Feishu Auth Exception] {e}")
            return None

    def add_record(self, app_token, table_id, fields):
        token = self.get_tenant_access_token()
        if not token: return False
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
        payload = {"fields": fields}
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.json().get("code") == 0:
                print(f"✅ 成功写入: {fields.get('产品代码')}")
                return True
            else:
                print(f"❌ 写入失败: {response.text}")
                return False
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            return False

# ==========================================
# 工具函数
# ==========================================
def load_purchase_dates(filename="购入日期.txt"):
    info_map = {}
    if not os.path.exists(filename):
        print(f"⚠️ 警告: 未找到 {filename}，请确保已将此文件上传到 GitHub 仓库根目录。")
        return info_map
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 2 and not line.startswith(("{", "}", "source")):
                    code = parts[0].strip()
                    try:
                        c_date = datetime.datetime.strptime(parts[1].strip(), "%Y-%m-%d").date()
                        r_date = None
                        if len(parts) >= 3:
                            try:
                                r_date = datetime.datetime.strptime(parts[2].strip(), "%Y-%m-%d").date()
                            except: pass
                        info_map[code] = {'confirm_date': c_date, 'redeem_date': r_date}
                    except: pass
    except Exception as e:
        print(f"读取文件错误: {e}")
    return info_map

def load_product_codes(filename):
    """从文件加载产品代码列表"""
    codes = []
    if not os.path.exists(filename):
        print(f"⚠️ 警告: 未找到 {filename}")
        return codes
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            codes = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"读取 {filename} 错误: {e}")
    return codes

def get_30_day_prior_record(sorted_data, latest_date):
    """
    获取30天前的记录。
    如果正好30天前无数据，逻辑是“顺延”，即找 >= (latest_date - 30) 的第一条数据。
    """
    target_date = latest_date - timedelta(days=30)
    for item in sorted_data:
        if item['date'] >= target_date: return item
    return None

def get_nav_for_date(sorted_data, target_date):
    if not target_date: return None
    for item in sorted_data:
        if item['date'] >= target_date: return item['nav']
    return 0

# ==========================================
# 爬虫逻辑 (含 SSL 修复)
# ==========================================
def query_bocom(product_code, purchase_date=None, redeem_date=None):
    """交通银行"""
    url = "https://www.bocommwm.com/SITE/queryJylcBreakDetail.do"
    headers = {"User-Agent": "Mozilla/5.0", "X-Requested-With": "XMLHttpRequest"}
    cookies = {"JSESSIONID": "8D2A39697E6E2A04E0B05229A3E75237"}
    payload = {"REQ_MESSAGE": json.dumps({
        "REQ_HEAD": {"TRAN_PROCESS": "", "TRAN_ID": ""},
        "REQ_BODY": {"c_fundcode": product_code, "c_interestway": "0", "c_productcode": "undefined", "type": "max"}
    })}
    try:
        res = requests.post(url, headers=headers, cookies=cookies, data=payload, verify=False, timeout=10)
        data = res.json().get("RSP_BODY", {}).get("result", {}).get("profitList", [])
        clean = []
        for i in data:
            try:
                clean.append({'date': datetime.datetime.strptime(i['d_cdate'], '%Y-%m-%d').date(), 'nav': float(i['f_netvalue'])})
            except: continue
        clean.sort(key=lambda x: x['date'])
        if not clean: return product_code, "No Data", "No Data", None, 0, 0
        
        last = clean[-1]
        prior = get_30_day_prior_record(clean, last['date'])
        return product_code, last['nav'], prior['nav'] if prior else 0, last['date'], get_nav_for_date(clean, purchase_date), get_nav_for_date(clean, redeem_date)
    except: return product_code, "Error", "Error", None, 0, 0

def query_cmbc_fuzhu(product_code, product_name=None, purchase_date=None, redeem_date=None):
    """民生理财 (应用深度 SSL 修复)"""
    if not product_name:
        product_name = product_code
    url = "https://www.cmbcwm.com.cn/gw/po_web/BTADailyQry"
    headers = {'User-Agent': 'Mozilla/5.0'}
    start_date = (datetime.datetime.now() - timedelta(days=1460)).strftime("%Y%m%d")
    payload = {'chart_type': '0', 'real_prd_code': product_code, 'begin_date': start_date, 'end_date': ''}

    try:
        # === 关键修复：挂载 Adapter ===
        session = requests.Session()
        session.mount('https://', LegacySSLAdapter())

        # verify=False 仍然保留，但现在 adapter 内部已经处理好了 check_hostname=False
        res = session.post(url, headers=headers, data=payload, verify=False, timeout=15)
        # ===========================

        nav_list = res.json().get('list', [])
        clean = []
        for i in nav_list:
            try:
                clean.append({'date': datetime.datetime.strptime(str(i['ISS_DATE']), "%Y%m%d").date(), 'nav': float(i['NAV'])})
            except: continue
        clean.sort(key=lambda x: x['date'])
        if not clean: return product_name, "No Data", "No Data", None, 0, 0

        last = clean[-1]
        prior = get_30_day_prior_record(clean, last['date'])
        return product_name, last['nav'], prior['nav'] if prior else 0, last['date'], get_nav_for_date(clean, purchase_date), get_nav_for_date(clean, redeem_date)
    except Exception as e:
        print(f"民生异常: {e}")
        return product_name, "Error", "Error", None, 0, 0

def query_efunds_yizeng(product_code, purchase_date=None, redeem_date=None):
    """易方达 (整合了新版逻辑：合并历史与近期数据，使用正则解析)"""
    url_history = f'https://cdn.efunds.com.cn/market/2.0/his/{product_code}_all.js'
    url_recent = f'https://cdn.efunds.com.cn/market/2.0/{product_code}_1y.js'

    headers = {
        'Referer': 'https://www.efunds.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    }

    session = requests.Session()
    session.headers.update(headers)
    merged_data = {}

    try:
        for url in [url_history, url_recent]:
            try:
                res = session.get(url, timeout=10)
                # 使用正则提取引号内的内容
                match = re.search(r'=\s*"(.*?)";', res.text)
                if match:
                    content = match.group(1)
                    records = content.split(';')
                    for record in records:
                        if not record or '_' not in record or record.startswith('0_'):
                            continue
                        parts = record.split('_')
                        if len(parts) >= 3:
                            # parts[0]: YYYYMMDD, parts[2]: NAV
                            d_str = parts[0]
                            nav_val = float(parts[2])
                            dt = datetime.datetime.strptime(d_str, "%Y%m%d").date()
                            merged_data[dt] = nav_val
            except Exception as e:
                print(f"易方达 URL 请求错误 {url}: {e}")
                continue

        # 转换为列表并排序
        clean = [{'date': k, 'nav': v} for k, v in merged_data.items()]
        clean.sort(key=lambda x: x['date'])

        if not clean:
            return product_code, "No Data", "No Data", None, 0, 0

        last = clean[-1]
        prior = get_30_day_prior_record(clean, last['date'])

        return product_code, last['nav'], prior['nav'] if prior else 0, last['date'], get_nav_for_date(clean, purchase_date), get_nav_for_date(clean, redeem_date)

    except Exception as e:
        print(f"易方达处理异常: {e}")
        return product_code, "Error", "Error", None, 0, 0

def query_citic_wealth(product_code, purchase_date=None, redeem_date=None):
    """中信银行 (安盈象) - 整合自 安盈象...py"""
    url = "https://wechat.citic-wealth.com/cms.product/api/custom/productInfo/getTAProductNav"
    params = {
        "prodCode": product_code,
        "queryUnit": "5" # 查询近5年? 或者单位，原脚本为5
    }
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.citic-wealth.com",
        "Referer": "https://www.citic-wealth.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "channel": "h5_trade_service"
    }
    
    # 注意：JSESSIONID 可能会过期，如果失效需要在 GitHub Secrets 或配置文件中更新
    cookies = {
        "JSESSIONID": "rumlKZY0aDXcnkUn8dvr_lIyapa4KANRZMNXpQ3o"
    }

    try:
        # 使用 LegacySSLAdapter 以防万一，虽然中信一般不需要
        session = requests.Session()
        session.mount('https://', LegacySSLAdapter())
        
        response = session.get(url, headers=headers, params=params, cookies=cookies, verify=False, timeout=10)
        data = response.json()
        
        if data.get("code") != "0000":
            print(f"中信 API 错误 ({product_code}): {data.get('msg')}")
            return product_code, "API Error", 0, None, 0, 0

        nav_list = data.get("data", {}).get("productNavList", [])
        clean = []
        for item in nav_list:
            date_str = item.get("navDate")
            nav_value = item.get("nav")
            if date_str and nav_value is not None:
                try:
                    dt = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                    clean.append({'date': dt, 'nav': float(nav_value)})
                except: continue
        
        clean.sort(key=lambda x: x['date'])
        
        if not clean:
            return product_code, "No Data", "No Data", None, 0, 0

        last = clean[-1]
        prior = get_30_day_prior_record(clean, last['date'])
        
        return product_code, last['nav'], prior['nav'] if prior else 0, last['date'], get_nav_for_date(clean, purchase_date), get_nav_for_date(clean, redeem_date)

    except Exception as e:
        print(f"中信银行异常 ({product_code}): {e}")
        return product_code, "Error", "Error", None, 0, 0

def query_hzbank(product_code, product_name=None, purchase_date=None, redeem_date=None):
    """杭银理财"""
    if not product_name:
        product_name = product_code
    # 注意：这里需要根据杭银的实际API调整URL和请求方式
    # 目前暂时返回错误信息，提示需要实现
    print(f"⚠️ 杭银理财查询功能待实现 (产品代码: {product_code})")
    return product_name, "Not Implemented", "Not Implemented", None, 0, 0

def query_boc_niannianxin(purchase_date=None, redeem_date=None):
    """中行"""
    code, name = "2501240100", "年年鑫最短持有期11号A"
    url = "https://www.bankofchina.com/sourcedb/srfd6_2024/index_2.html"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')
        for row in soup.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) >= 3 and cols[1].get_text(strip=True) == name:
                return code, float(cols[2].get_text(strip=True)), 0, datetime.date.today(), 0, 0
    except: pass
    return code, "Error", 0, None, 0, 0

# ==========================================
# 主程序
# ==========================================
def main():
    print("🚀 开始运行...")
    info_map = load_purchase_dates("购入日期.txt")
    feishu = FeishuClient(FEISHU_CONFIG["APP_ID"], FEISHU_CONFIG["APP_SECRET"])

    tasks = []

    # 1. 交行产品
    print("📂 加载交行产品代码...")
    bocom_codes = load_product_codes("/Users/cyhuang/Desktop/curl/交行产品代码.txt")
    for c in bocom_codes:
        d = info_map.get(c, {})
        tasks.append((query_bocom(c, d.get('confirm_date'), d.get('redeem_date')), d.get('confirm_date')))

    # 2. 民生产品
    print("📂 加载民生产品代码...")
    cmbc_codes = load_product_codes("/Users/cyhuang/Desktop/curl/民生产品代码.txt")
    for c in cmbc_codes:
        d = info_map.get(c, {})
        tasks.append((query_cmbc_fuzhu(c, c, d.get('confirm_date'), d.get('redeem_date')), d.get('confirm_date')))

    # 3. 易方达产品
    print("📂 加载易方达产品代码...")
    efunds_codes = load_product_codes("/Users/cyhuang/Desktop/curl/易方达产品代码.txt")
    for c in efunds_codes:
        d = info_map.get(c, {})
        tasks.append((query_efunds_yizeng(c, d.get('confirm_date'), d.get('redeem_date')), d.get('confirm_date')))

    # 4. 中信银行产品
    print("📂 加载中信银行产品代码...")
    citic_codes = load_product_codes("/Users/cyhuang/Desktop/curl/中信银行产品代码.txt")
    for c in citic_codes:
        d = info_map.get(c, {})
        tasks.append((query_citic_wealth(c, d.get('confirm_date'), d.get('redeem_date')), d.get('confirm_date')))

    # 5. 杭银产品
    print("📂 加载杭银产品代码...")
    hzbank_codes = load_product_codes("/Users/cyhuang/Desktop/curl/杭银产品代码.txt")
    for c in hzbank_codes:
        d = info_map.get(c, {})
        tasks.append((query_hzbank(c, c, d.get('confirm_date'), d.get('redeem_date')), d.get('confirm_date')))

    # 6. 中行 (固定产品)
    print("📂 加载中行产品...")
    boc_dates = info_map.get("2501240100", {})
    tasks.append((query_boc_niannianxin(boc_dates.get('confirm_date'), boc_dates.get('redeem_date')), boc_dates.get('confirm_date')))

    # 执行所有任务并写入飞书
    for (res, specific_c_date) in tasks:
        code, cur, prior, date_obj, pur, red = res
        if isinstance(cur, (int, float)) and date_obj:
            ts = int(datetime.datetime.combine(date_obj, datetime.time.min).timestamp() * 1000)
            c_ts = int(datetime.datetime.combine(specific_c_date, datetime.time.min).timestamp() * 1000) if specific_c_date else None

            fields = {
                "产品代码": code,
                "当日净值": cur,
                "30日前净值": prior,
                "购入当日净值": pur,
                "赎回净值": red if isinstance(red, (int, float)) else 0,
                "确认日": c_ts,
                "数据更新日期": ts
            }
            feishu.add_record(FEISHU_CONFIG["APP_TOKEN"], FEISHU_CONFIG["TABLE_ID"], fields)
        else:
            print(f"⚠️ 跳过: {code} (获取失败或格式错误)")

if __name__ == "__main__":
    main()
