"""
邮件解析 + MySQL 操作服务 — 从原 email_parser.py 迁移
MySQL 凭据改为从环境变量读取，不再硬编码
"""
from __future__ import annotations

import io
import os
import re
from datetime import datetime, date

from core.config import get_mysql_config

# ── 映射表（与 email_parser.py 保持一致） ────────────────────

PORT_MAP = {
    'iloilo': 'Iloilo', 'cebu': 'Cebu', 'davao': 'Davao',
    'cdo': 'CDO', 'cagayan': 'CDO',
    'mnl s': 'Manila South', 'mnl n': 'Manila North',
    'manila south': 'Manila South', 'manila north': 'Manila North',
    'manila': 'Manila South',
    '宿务': 'Cebu', '达沃': 'Davao', '卡加延': 'CDO',
    '马尼拉南': 'Manila South', '马尼拉北': 'Manila North',
    '马南': 'Manila South', '马北': 'Manila North', '伊洛伊洛': 'Iloilo',
}

SITG_PORT_CODE = {'DV': 'Davao', 'CB': 'Cebu', 'IO': 'Iloilo', 'CD': 'CDO', 'MN': 'Manila South'}

BL_PREFIX_LINE = {
    'SITTAG': 'SITC', 'SITG': 'SITC', 'OOLU': 'OOCL', 'COAU': 'COSCO',
    'MCLP': 'MCC', 'CNHU': 'CNC', 'CNH': 'CNC', 'EGLV': 'EVERGREEN',
    'HLCU': 'HAPAG-LLOYD', 'MAEU': 'MAERSK', 'MSCU': 'MSC', 'APHU': 'APL',
}

LINE_KEYWORDS = [
    ('WAN HAI', 'WAN HAI'), ('WANHAI', 'WAN HAI'), ('MAERSK', 'MAERSK'),
    ('COSCO', 'COSCO'), ('OOCL', 'OOCL'), ('SITC', 'SITC'),
    ('EMC', 'EMC'), ('CNC', 'CNC'), ('MCC', 'MCC'),
    ('EVERGREEN', 'EVERGREEN'), ('HAPAG', 'HAPAG-LLOYD'),
]

CONSIGNEE_BROKER = {
    'FULLWAY': 'Alin', 'MAXWAY': 'Arden', 'POWERWAY': 'PGMC',
    'GRANDWAY': 'PGMC', 'UNIONBAY': 'Unionbay施工队',
    'LFM': 'LFM', 'KRT CONSUMER GOODS': 'Arden', 'KRT': 'KRT', 'GBC': 'GBC',
}

FORWARDER_MAP = {
    '王斌': '王斌', '吉永': '吉永', '高阳': '高阳', '昊泽': '昊泽',
    '衍峰': '衍峰', '泓大': '泓大', '于丽英': '于丽英', '誉鼎': '誉鼎',
    '麒成': '麒成', '元一': '元一（工厂自理）', '海潮': '海潮（工厂自理）',
    '齐安': '齐安（工厂自理）',
}

FORWARDER_EMAIL_MAP = {'yongyue': '永越', 'xinfei': '鑫菲航', 'xinhang': '鑫菲航', 'opfs': 'OPFS'}

PRODUCT_EN = {
    'corrugated steel sheet': 'GL Corrugated Steel Sheet 彩涂瓦楞板',
    'corrugated': 'GL Corrugated Steel Sheet 彩涂瓦楞板',
    'welded wire mesh': 'Welded Wire Mesh 铁丝网',
    'welded wire': 'Welded Wire Mesh 铁丝网',
    'welding electrode': 'Welding Electrode 焊条',
    'cyclone wire': 'Cyclone Wire 铁围网',
    'barbed wire': 'Barbed Wire 刺绳',
    'gi wire': 'GI Wire 铁丝', 'steel wire': 'GI Wire 铁丝', 'tie wire': 'GI Wire 铁丝',
    'hog wire': 'Hog Wire 牛栏网', 'hardware cloth': 'Hardware Cloth 铁窗网',
    'steel matting': 'Steel Matting 冲花片', 'phenolic board': 'Phenolic Board 膜板',
    'plywood': 'Plywood 夹板', 'eco-board': 'Eco-Board 生态板', 'eco board': 'Eco-Board 生态板',
    'plastic canvas': 'Plastic Canvas 布', 'plastic resin': 'Plastic Resin 塑料粒',
    'lldpe': 'Plastic Resin 塑料粒', 'ldpe': 'Plastic Resin 塑料粒',
    'concrete nail': 'Concrete Nails 水泥钉', 'umbrella nail': 'Umbrella Nails 雨伞钉',
    'jetmatic pump': 'Jetmatic Pump 抽水泵', 'clamp': 'Clamp 夹码',
    'angle bar': 'Angle Bar 角钢', 'plain bar': 'Plain Bar 圆钢',
    'square bar': 'Square Bar 方钢', 'channel bar': 'Channel Bar 槽钢',
    't bar': 'T Bar T型钢', 'steel tube': 'Steel Tube 钢管', 'steel pipe': 'Steel Pipe 圆管',
    'steel sheet': 'Steel Sheet 钢板', 'steel strip': 'Steel Strips 钢带',
    'galvanized square tube': 'Galvanized Square Tube 镀锌方管',
    'square rectangular steel': 'Square/Rectangular Steel Tube 方矩管',
    'metal purling': 'Metal Purling 钢檩', 'upvc door': 'UPVC Door 塑钢门',
    'steel shovel': 'Steel Shovel 铁铲',
}

PRODUCT_CN = {
    '铁丝': 'GI Wire 铁丝', '牛栏网': 'Hog Wire 牛栏网',
    '勾花网': 'Cyclone Wire 铁围网', '铁围网': 'Cyclone Wire 铁围网',
    '网片': 'Steel Matting 冲花片', '胶合板': 'Plywood 胶合板',
    '夹板': 'Plywood 夹板', '膜板': 'Phenolic Board 膜板',
    '塑料米': 'Plastic Resin 塑料粒', '塑料粒': 'Plastic Resin 塑料粒',
    '钢板': 'Steel Sheet 钢板', '方管': 'Square Bar 方钢',
    '圆管': 'Steel Pipe 圆管', '钢管': 'Steel Tube 钢管',
    '钢带': 'Steel Strips 钢带', '角铁': 'Angle Bar 角钢',
    '角钢': 'Angle Bar 角钢', '龙骨': 'Metal Purling 钢檩',
    '钢檩': 'Metal Purling 钢檩', '刺丝': 'Barbed Wire 刺绳',
    '刺绳': 'Barbed Wire 刺绳', '水泥钉': 'Concrete Nails 水泥钉',
    '铁铲': 'Steel Shovel 铁铲', '彩板': 'GL Corrugated Steel Sheet 彩涂瓦楞板',
    '焊条': 'Welding Electrode 焊条',
}

FACTORY_NAMES = sorted([
    '远尚祥瑞', '君亦德', '森乐邦', '张荣迁', '盈美居', '高领域',
    '永伟', '元一', '东尚', '永达', '君乐', '华思', '力成', '汇隆',
    '捷鹏', '齐安', '昀翼', '山梅', '万通', '恒博', '华金', '璞丰',
    '德弘', '雄兴', '新源', '德蕴', '金星', '悦途', '恺丰', '前进',
    '天诺', '鼎丰', '鸿林', '永联', '莲顺', '浩森', '三发', '盈航',
    '东灏', '台正', '其他', '九维', '恒凯', '港众', '仁盛',
], key=len, reverse=True)

FIELD_TO_DB_COLUMN = {
    'B/L No.': 'B_L_No', 'Container No.': 'Container_No', 'POD': 'POD',
    '清关公司': '清关公司', 'Shipping Line': 'Shipping_Line',
    '实际清关费/柜': '实际清关费_柜', '货代': '货代', '合同号': '合同号',
    '合同号代码': '合同号代码', '订货单位': '订货单位',
    'Shipper': 'Shipper', 'Consignee': 'Consignee', 'Factory': 'Factory',
    '品名': '品名', '品名补充': '品名补充', '集装箱规格': '集装箱规格',
    '集装箱数量': '集装箱数量', 'Free Demurage': 'Free_Demurage',
    'Free Detention': 'Free_Detention', 'ETD': 'ETD', 'ETA': 'ETA',
    'Days to ETA': 'Days_to_ETA', '资料进度': '资料进度',
}

DB_TABLE = "Database_2026"


# ── MySQL 连接 ────────────────────────────────────────────────

def _get_mysql_conn():
    try:
        import pymysql
    except ImportError as exc:
        raise ImportError("请先安装 pymysql") from exc

    cfg = get_mysql_config()
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset='utf8mb4',
        autocommit=False,
        ssl={'ssl_disabled': False},
    )


# ── 解析工具函数 ──────────────────────────────────────────────

def _normalize_date(s: str) -> str:
    if not s:
        return ''
    s = s.strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    m = re.match(r'^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime('%Y-%m-%d')
        except ValueError:
            pass
    m = re.match(r'^(\d{1,2})\s*([A-Za-z]{3})\s*(\d{2,4})?$', s)
    if m:
        day, mon, yr_s = int(m.group(1)), m.group(2).capitalize(), m.group(3) or str(datetime.now().year)
        yr = int(yr_s) if len(yr_s) == 4 else 2000 + int(yr_s)
        mn = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,
              'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}.get(mon)
        if mn:
            try:
                return date(yr, mn, day).strftime('%Y-%m-%d')
            except ValueError:
                pass
    return s


def _normalize_box(box_str: str) -> str:
    return box_str.upper().replace("'", "").replace("HQ", "HC")


def _bl_to_line(bl: str) -> str:
    bl = bl.upper()
    if re.match(r'^2\d{8}$', bl):
        return 'MCC'
    for prefix, line in BL_PREFIX_LINE.items():
        if bl.startswith(prefix):
            return line
    return ''


def _sitg_bl_to_port(bl: str) -> str:
    m = re.match(r'(SITG[A-Z]{2}|SITTAG)([A-Z]{2})', bl.upper())
    if m:
        return SITG_PORT_CODE.get(m.group(2), '')
    return ''


def _extract_port(text: str) -> str:
    for cn, std in PORT_MAP.items():
        if len(cn) > 2 and cn in text:
            return std
    for cn, std in PORT_MAP.items():
        if len(cn) == 2 and cn in text:
            return std
    text_up = text.upper()
    for key in sorted(PORT_MAP.keys(), key=len, reverse=True):
        if key in ('宿务','达沃','卡加延','马尼拉南','马尼拉北','马南','马北','伊洛伊洛'):
            continue
        if re.search(r'\b' + re.escape(key.upper()) + r'\b', text_up):
            return PORT_MAP[key]
    return ''


def _extract_products(text: str) -> str:
    found = []
    text_up = text.upper()
    for kw in sorted(PRODUCT_EN.keys(), key=len, reverse=True):
        if kw.upper() in text_up:
            full = PRODUCT_EN[kw]
            if full not in found:
                found.append(full)
    for cn, std in PRODUCT_CN.items():
        if cn in text and std not in found:
            found.append(std)
    return ','.join(found) if found else ''


def _extract_factories(text: str) -> str:
    matched, matched_set, covered = [], set(), []
    for name in FACTORY_NAMES:
        start = 0
        while True:
            idx = text.find(name, start)
            if idx == -1:
                break
            end = idx + len(name)
            if not any(s <= idx < e or s < end <= e for s, e in covered) and name not in matched_set:
                matched.append(name)
                matched_set.add(name)
                covered.append((idx, end))
            start = end
    matched.sort(key=lambda n: text.find(n))
    return ' '.join(matched) if matched else ''


# ── 核心解析函数 ──────────────────────────────────────────────

def parse_email_text(text: str) -> dict:
    result = {}
    text_up = text.upper()

    bl_patterns = [
        r'\b(2\d{8})\b',
        r'\b(SITTAG[A-Z]{2}\d{6,8})\b', r'\b(SITG[A-Z]{4}\d{6,8})\b',
        r'\b(OOLU[A-Z0-9]{8,12})\b', r'\b(COAU\d{10,13})\b',
        r'\b(MCLP[A-Z0-9]{8,12})\b', r'\b(CNHU[A-Z0-9]{8,12})\b',
        r'\b(EGLV[A-Z0-9]{8,12})\b', r'\b(HLCU[A-Z0-9]{8,12})\b',
        r'\b(MAEU[A-Z0-9]{8,12})\b', r'\b(CNH\d{7,10})\b',
        r'(?:提单号[：:]|提单[：:]?|B[/]?L[#\s]*[：:]?)\s*([A-Z][A-Z0-9]{7,14})',
        r'\b([A-Z]{4}[A-Z0-9]{8,12})\b', r'\b(\d{12,13})\b',
    ]
    for pat in bl_patterns:
        m = re.search(pat, text_up)
        if m:
            bl = m.group(1)
            if re.match(r'^[A-Z]{4}\d{7}$', bl):
                continue
            result['B/L No.'] = bl
            break

    if 'B/L No.' in result:
        bl = result['B/L No.']
        line = _bl_to_line(bl)
        if line:
            result['Shipping Line'] = line
        port = _sitg_bl_to_port(bl)
        if port:
            result['_port_from_bl'] = port

    containers = [c for c in re.findall(r'\b([A-Z]{4}\d{7})\b', text_up)
                  if c not in result.get('B/L No.', '')]
    if containers:
        result['Container No.'] = '\n'.join(containers)

    if '胶合板' in text:
        result['集装箱规格'] = '40HC'
    else:
        box_m = re.search(r'(\d+)\s*[xX×*＊]\s*(20GP|40H[CQ]|20\'?GP|40\'?H[CQ])', text_up)
        if box_m:
            result['集装箱数量'] = box_m.group(1)
            result['集装箱规格'] = _normalize_box(box_m.group(2))

    port = _extract_port(text)
    if port:
        result['POD'] = port
    elif result.get('_port_from_bl'):
        result['POD'] = result['_port_from_bl']
    result.pop('_port_from_bl', None)

    if 'Shipping Line' not in result:
        for kw, line in LINE_KEYWORDS:
            if kw in text_up:
                result['Shipping Line'] = line
                break

    etd_m = re.search(
        r'(?:ETD|装箱时间|开船时间)[：:\s/]*(\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}\s*[A-Za-z]{3}\s*\d{0,4}|\d{1,2}[./]\d{1,2})',
        text
    )
    if etd_m:
        result['ETD'] = _normalize_date(etd_m.group(1))

    eta_m = re.search(
        r'ETA[：:\s/]*(\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}\s*[A-Za-z]{3}\s*\d{0,4}|\d{1,2}[./]\d{1,2})',
        text_up
    )
    if eta_m:
        result['ETA'] = _normalize_date(eta_m.group(1))

    if '胶合板' in text:
        result['品名'] = 'Plywood 胶合板'
    else:
        products = _extract_products(text)
        if products:
            result['品名'] = products

    for key, broker in CONSIGNEE_BROKER.items():
        if key in text_up:
            result['清关公司'] = broker
            break

    for key, forwarder in FORWARDER_MAP.items():
        if key in text:
            result['货代'] = forwarder
            break

    factory = _extract_factories(text)
    if factory:
        result['Factory'] = factory

    free_m = re.search(r'(\d+)\s*[+＋]\s*(\d+)', text)
    if free_m:
        result['Free Demurage'] = free_m.group(1)
        result['Free Detention'] = free_m.group(2)

    if result.get('ETA') and len(result['ETA']) == 10:
        try:
            eta_d = datetime.strptime(result['ETA'], '%Y-%m-%d').date()
            delta = (eta_d - date.today()).days
            result['Days to ETA'] = f"{abs(delta)}天前" if delta < 0 else f"{delta}天后"
        except ValueError:
            pass

    return result


def _merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for k, v in override.items():
        if not k.startswith('_') and v and not merged.get(k):
            merged[k] = v
    return merged


def parse_full_email(subject: str, from_addr: str, body_text: str,
                     attachment_texts: list[dict] | None = None) -> dict:
    if attachment_texts is None:
        attachment_texts = []
    result = parse_email_text(subject or '')
    if body_text:
        result = _merge(result, parse_email_text(body_text))
    for att in attachment_texts:
        if att.get('text'):
            result = _merge(result, parse_email_text(att['text']))

    if not result.get('货代') and from_addr:
        for key, fw in FORWARDER_MAP.items():
            if key in from_addr:
                result['货代'] = fw
                break
        if not result.get('货代'):
            for kw, fw in FORWARDER_EMAIL_MAP.items():
                if kw in from_addr.lower():
                    result['货代'] = fw
                    break

    result['发件人'] = from_addr or ''
    result['邮件主题'] = subject or ''
    result.pop('_bl_fallback', None)
    result.pop('_port_from_bl', None)
    return result


# ── 附件文字提取（服务端） ────────────────────────────────────

def extract_attachment_text(filename: str, data: bytes) -> str:
    fname = filename.lower()
    if fname.endswith(".pdf"):
        return _extract_pdf(data)
    elif fname.endswith(".docx"):
        return _extract_docx(data)
    elif fname.endswith((".xlsx", ".xlsm")):
        return _extract_excel_openpyxl(data)
    elif fname.endswith(".xls"):
        return _extract_excel_xlrd(data)
    return ""


def _extract_pdf(data: bytes) -> str:
    try:
        import pypdf
        reader = pypdf.PdfReader(io.BytesIO(data))
        pages = []
        for i, page in enumerate(reader.pages):
            t = page.extract_text() or ""
            if t.strip():
                pages.append(t)
        return "\n\n".join(pages)
    except Exception as e:
        return ""


def _extract_docx(data: bytes) -> str:
    try:
        from docx import Document
        doc = Document(io.BytesIO(data))
        lines = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n".join(lines)
    except Exception:
        return ""


def _extract_excel_openpyxl(data: bytes) -> str:
    try:
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        lines = []
        for sheet in wb.worksheets:
            for row in sheet.iter_rows(values_only=True):
                cells = [str(c).strip() for c in row if c is not None]
                if any(cells):
                    lines.append("  |  ".join(cells))
        wb.close()
        return "\n".join(lines)
    except Exception:
        return ""


def _extract_excel_xlrd(data: bytes) -> str:
    try:
        import xlrd
        wb = xlrd.open_workbook(file_contents=data)
        lines = []
        for sheet in wb.sheets():
            for row_idx in range(sheet.nrows):
                cells = [str(sheet.cell_value(row_idx, c)).strip() for c in range(sheet.ncols)]
                if any(cells):
                    lines.append("  |  ".join(cells))
        return "\n".join(lines)
    except Exception:
        return ""


# ── 数据库操作 ────────────────────────────────────────────────

def insert_to_database(parsed: dict) -> dict:
    columns, values = [], []
    for field_name, db_col in FIELD_TO_DB_COLUMN.items():
        val = parsed.get(field_name)
        if val is not None and val != '':
            if db_col in ('集装箱数量', 'Free_Demurage', 'Free_Detention'):
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    pass
            columns.append(db_col)
            values.append(val)

    if '资料进度' not in columns:
        columns.append('资料进度')
        values.append('等待确认草稿')

    columns.append('创建时间')
    values.append(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

    bl_no = parsed.get('B/L No.', '')
    col_str = ', '.join(f'`{c}`' for c in columns)
    placeholder_str = ', '.join(['%s'] * len(values))
    sql = f"INSERT INTO `{DB_TABLE}` ({col_str}) VALUES ({placeholder_str})"

    conn = None
    try:
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute(sql, values)
            row_id = cur.lastrowid
        conn.commit()
        return {"success": True, "row_id": row_id, "bl_no": bl_no,
                "message": f"已写入数据库（id={row_id}，提单号={bl_no or '未识别'}）"}
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"success": False, "row_id": None, "bl_no": bl_no, "message": f"写入失败：{e}"}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def update_progress_by_bl(bl_no: str, progress_value: str) -> dict:
    if not bl_no:
        return {"success": False, "rows": 0, "message": "提单号为空"}
    conn = None
    try:
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE `{DB_TABLE}` SET `资料进度` = %s WHERE `B_L_No` = %s",
                (progress_value, bl_no.upper())
            )
            rows = cur.rowcount
        conn.commit()
        return {"success": True, "rows": rows, "message": f"已更新 {rows} 条（{bl_no} → {progress_value}）"}
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"success": False, "rows": 0, "message": f"更新失败：{e}"}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def update_mingxi_progress(bl_nos: list[str]) -> dict:
    if not bl_nos:
        return {"success": False, "updated": 0, "not_found": [], "message": "提单号列表为空"}
    conn = None
    try:
        conn = _get_mysql_conn()
        updated, not_found = 0, []
        with conn.cursor() as cur:
            for bl in bl_nos:
                cur.execute(
                    f"UPDATE `{DB_TABLE}` SET `明细_开单进度` = '已做明细' WHERE `B_L_No` = %s",
                    (bl.strip().upper(),)
                )
                (updated := updated + 1) if cur.rowcount else not_found.append(bl)
        conn.commit()
        msg = f"已更新 {updated} 条记录"
        if not_found:
            msg += f"，未找到：{', '.join(not_found)}"
        return {"success": True, "updated": updated, "not_found": not_found, "message": msg}
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return {"success": False, "updated": 0, "not_found": [], "message": f"更新失败：{e}"}
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def query_broker_by_bl(bl_no: str) -> str:
    if not bl_no:
        return ""
    conn = None
    try:
        conn = _get_mysql_conn()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT `清关公司` FROM `{DB_TABLE}` WHERE `B_L_No` = %s ORDER BY `创建时间` DESC LIMIT 1",
                (bl_no.upper(),)
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else ""
    except Exception:
        return ""
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── 配置表操作（清关公司 / 草稿模板） ─────────────────────────

def _ensure_config_tables():
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `app_config_brokers` (
                    `name` VARCHAR(100) NOT NULL PRIMARY KEY,
                    `emails_json` TEXT NOT NULL,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) CHARSET=utf8mb4
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS `app_config_templates` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `name` VARCHAR(200) NOT NULL UNIQUE,
                    `body` TEXT NOT NULL,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) CHARSET=utf8mb4
            """)
        conn.commit()
    finally:
        conn.close()


def get_brokers() -> dict:
    import json
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT `name`, `emails_json` FROM `app_config_brokers`")
            rows = cur.fetchall()
        return {name: json.loads(emails_json) for name, emails_json in rows}
    finally:
        conn.close()


def upsert_broker(name: str, emails: list[str]) -> bool:
    import json
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `app_config_brokers` (`name`, `emails_json`) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE `emails_json` = VALUES(`emails_json`)",
                (name, json.dumps(emails, ensure_ascii=False))
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


def delete_broker(name: str) -> bool:
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM `app_config_brokers` WHERE `name` = %s", (name,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


def get_templates() -> list[dict]:
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT `name`, `body` FROM `app_config_templates` ORDER BY `id`")
            return [{"name": row[0], "body": row[1]} for row in cur.fetchall()]
    finally:
        conn.close()


def upsert_template(name: str, body: str) -> bool:
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO `app_config_templates` (`name`, `body`) VALUES (%s, %s) "
                "ON DUPLICATE KEY UPDATE `body` = VALUES(`body`)",
                (name, body)
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()


def delete_template(name: str) -> bool:
    conn = _get_mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM `app_config_templates` WHERE `name` = %s", (name,))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()
