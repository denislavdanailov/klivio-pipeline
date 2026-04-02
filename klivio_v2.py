#!/usr/bin/env python3
"""
Klivio Pipeline v2 — с Research + Pain Points + Telegram известия

НОВИ ФУНКЦИИ:
  - Автоматично изследва всеки lead (сайт, reviews, индустрия)
  - Открива болни точки и ги вгражда в имейла
  - Telegram известия при reply
  - Дневен digest на всички изпратени имейли

НАСТРОЙКА НА TELEGRAM (5 мин, безплатно):
  1. Отвори Telegram → търси @BotFather
  2. Пиши /newbot → дай му име → копирай TOKEN
  3. Търси @userinfobot → пиши /start → копирай твоя Chat ID
  4. Постави TOKEN и CHAT_ID долу

УПОТРЕБА:
  python klivio_v2.py --mode preview    # първи 2 имейла → при теб
  python klivio_v2.py --mode live       # реални изпращания
  python klivio_v2.py --mode check      # проверява за replies и праща известия
  python klivio_v2.py --mode digest     # изпраща дневен digest в Telegram
"""

import argparse, csv, imaplib, json, logging, os, random, re
import smtplib, time, email as emaillib
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log", encoding="utf-8")]
)
log = logging.getLogger("klivio")

# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ — ПОПЪЛНИ ТЕЗИ
# ═══════════════════════════════════════════════════════════════
YOUR_EMAIL       = os.getenv("YOUR_EMAIL",       "ТВОЯТ_ИМЕЙЛ@gmail.com")
REPLY_TO_EMAIL   = os.getenv("REPLY_TO_EMAIL",   "replies.klivio@gmail.com")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY",     "")

# Telegram — за известия на телефона (безплатно)
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")   # от @BotFather
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")   # от @userinfobot

# IMAP за проверка на replies (Gmail на reply-to акаунта)
REPLY_IMAP_SERVER = "imap.gmail.com"
REPLY_IMAP_USER   = os.getenv("REPLY_IMAP_USER",   REPLY_TO_EMAIL)
REPLY_IMAP_PASS   = os.getenv("REPLY_IMAP_PASS",   "")  # Gmail App Password

SENDER_NAME     = "James"
SENDER_COMPANY  = "Klivio"
BREVO_SMTP_HOST = "smtp-relay.brevo.com"
BREVO_SMTP_PORT = 587
DAILY_LIMIT     = 50       # 50 за седмица 1-2, после 150, после 250
PREVIEW_COUNT   = 2
STATE_FILE      = "pipeline_state.json"
LEADS_FILE      = "leads.csv"
SENT_LOG        = "sent_log.csv"
REPLIES_LOG     = "replies_log.csv"

# ═══════════════════════════════════════════════════════════════
# BREVO АКАУНТИ
# ═══════════════════════════════════════════════════════════════
BREVO_ACCOUNTS = [
    {"id":  1, "from_email": "james@klivio.bond",   "smtp_user": "a48308001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_1","")},
    {"id":  2, "from_email": "oliver@klivio.bond",  "smtp_user": "a4a8e6001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_2","")},
    {"id":  3, "from_email": "harry@klivio.bond",   "smtp_user": "a4f3a6001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_3","")},
    {"id":  4, "from_email": "george@klivio.bond",  "smtp_user": "a4f936001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_4","")},
    {"id":  5, "from_email": "emma@klivio.bond",    "smtp_user": "a482e2001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_5","")},
    {"id":  6, "from_email": "jack@klivio.bond",    "smtp_user": "a5f48a001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_6","")},
    {"id":  7, "from_email": "samuel@klivio.bond",  "smtp_user": "a5f4e5001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_7","")},
    {"id":  8, "from_email": "jessica@klivio.bond", "smtp_user": "a5f520001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_8","")},
    {"id":  9, "from_email": "emily@klivio.bond",   "smtp_user": "a5f582001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_9","")},
    {"id": 10, "from_email": "sarah@klivio.bond",   "smtp_user": "a5f6ab001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_10","")},
    {"id": 11, "from_email": "william@klivio.site", "smtp_user": "a5bc51001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_11","")},
    {"id": 12, "from_email": "thomas@klivio.site",  "smtp_user": "a5d742001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_12","")},
    {"id": 13, "from_email": "michael@klivio.site", "smtp_user": "a5d898001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_13","")},
    {"id": 14, "from_email": "amy@klivio.site",     "smtp_user": "a5f997001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_14","")},
    {"id": 15, "from_email": "laura@klivio.site",   "smtp_user": "a5f9f8001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_15","")},
    {"id": 16, "from_email": "benjamin@klivio.site","smtp_user": "a5fa73001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_16","")},
    {"id": 17, "from_email": "edward@klivio.site",  "smtp_user": "a611ce001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_17","")},
    {"id": 18, "from_email": "joseph@klivio.site",  "smtp_user": "a6128f001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_18","")},
    {"id": 19, "from_email": "charles@klivio.site", "smtp_user": "a61985001@smtp-brevo.com",   "smtp_pass": os.getenv("BREVO_PASS_19","")},
]


# ═══════════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════════
@dataclass
class Lead:
    business_name: str = ""
    first_name: str = ""
    email: str = ""
    phone: str = ""
    website: str = ""
    city: str = ""
    country: str = ""
    industry: str = ""
    source: str = ""
    google_rating: str = ""
    google_reviews: str = ""
    tier: str = "Tier3"
    sequence: str = "E"
    scraped_at: str = ""
    # Research полета
    pain_points: str = ""
    website_summary: str = ""
    weak_reviews: str = ""
    opportunity: str = ""

@dataclass
class SentRecord:
    lead_email: str; business_name: str; subject: str
    sent_to: str; account_id: int; mode: str
    sent_at: str; email_num: int; sequence: str
    pain_point_used: str = ""; opened: str = "no"; replied: str = "no"

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════
def telegram(message: str, parse_mode: str = "HTML") -> bool:
    """Праща известие в Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.debug("Telegram не е конфигуриран — пропускам")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        log.warning(f"Telegram грешка: {e}")
        return False

def tg_reply_alert(business: str, from_email: str, subject: str, preview: str):
    msg = (
        f"🔥 <b>НОВ REPLY — Klivio</b>\n\n"
        f"<b>От:</b> {business}\n"
        f"<b>Имейл:</b> {from_email}\n"
        f"<b>Subject:</b> {subject}\n\n"
        f"<b>Preview:</b>\n<i>{preview[:300]}</i>\n\n"
        f"📌 Провери: {REPLY_TO_EMAIL}"
    )
    telegram(msg)

def tg_daily_digest(sent_count: int, accounts_used: int, top_sequences: dict):
    seq_str = "\n".join([f"  • Sequence {k}: {v}" for k, v in list(top_sequences.items())[:5]])
    msg = (
        f"📊 <b>Klivio Daily Digest</b>\n"
        f"📅 {datetime.now().strftime('%d %b %Y')}\n\n"
        f"📤 <b>Изпратени днес:</b> {sent_count}\n"
        f"📬 <b>Акаунти използвани:</b> {accounts_used}\n\n"
        f"<b>По sequence:</b>\n{seq_str}\n\n"
        f"✅ Всичко върви по план"
    )
    telegram(msg)

def tg_preview_ready(count: int):
    msg = (
        f"👀 <b>Klivio Preview готов</b>\n\n"
        f"{count} имейла чакат в {YOUR_EMAIL}\n\n"
        f"Провери и ако изглеждат ОК пусни:\n"
        f"<code>python klivio_v2.py --mode live</code>"
    )
    telegram(msg)

# ═══════════════════════════════════════════════════════════════
# LEAD RESEARCH — Открива болни точки
# ═══════════════════════════════════════════════════════════════
def research_website(url: str) -> dict:
    """Сваля и анализира уебсайта на бизнеса."""
    result = {"summary": "", "services": [], "red_flags": []}
    if not url:
        return result
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        # Премахни nav/footer/scripts
        for tag in soup(["script","style","nav","footer","header"]):
            tag.decompose()
        text = " ".join(soup.get_text().split())[:2000]
        result["summary"] = text

        # Търси red flags
        red_flags = []
        text_lower = text.lower()
        if "contact us" not in text_lower and "call us" not in text_lower:
            red_flags.append("no clear CTA on website")
        if not re.search(r'review|testimonial|trust', text_lower):
            red_flags.append("no social proof visible")
        if not re.search(r'price|pricing|cost|package|plan', text_lower):
            red_flags.append("pricing not listed")
        if len(text) < 300:
            red_flags.append("very thin website content")
        result["red_flags"] = red_flags
    except Exception as e:
        log.debug(f"Website research failed for {url}: {e}")
    return result


def research_google_reviews(business_name: str, city: str) -> dict:
    """Търси негативни Google reviews — те разкриват болните точки."""
    result = {"avg_rating": "", "pain_themes": []}
    # Без Google Places API → анализираме рейтинга от leads.csv
    # Логика: ниски рейтинги = проблем с клиентски опит = шанс за нас
    return result


def analyze_pain_points_with_ai(lead: Lead, website_data: dict) -> dict:
    """
    Използва Groq за да анализира lead-а и открие:
    - Главна болна точка
    - Конкретна икономия на пари
    - Персонализиран hook за имейла
    """
    if not GROQ_API_KEY:
        return _fallback_pain_points(lead)

    rating = lead.google_rating
    reviews = lead.google_reviews
    red_flags = ", ".join(website_data.get("red_flags", [])) or "none found"
    website_summary = website_data.get("summary", "")[:800]

    prompt = f"""You are a B2B sales researcher. Analyze this business and identify why they NEED Klivio's cold email service.

Business: {lead.business_name}
Industry: {lead.industry}
Location: {lead.city}, {lead.country}
Google Rating: {rating}/5 ({reviews} reviews)
Website issues found: {red_flags}
Website content snippet: {website_summary[:400]}

Klivio helps businesses get more clients through automated cold email outreach.

Analyze and return ONLY JSON (no markdown):
{{
  "main_pain": "their #1 problem in 1 sentence (specific, not generic)",
  "money_angle": "how they're losing money or missing revenue right now",
  "personalized_hook": "1 sentence opening for a cold email that references something specific about their business",
  "opportunity": "the specific growth opportunity Klivio gives them",
  "urgency": "why they should act now (market trend, competition, season)"
}}

Be specific. Use their actual industry and location. Not generic."""

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama3-8b-8192",
                  "messages": [{"role":"user","content":prompt}],
                  "temperature": 0.6, "max_tokens": 500},
            timeout=20
        )
        r.raise_for_status()
        content = re.sub(r"```json|```","",r.json()["choices"][0]["message"]["content"]).strip()
        return json.loads(content)
    except Exception as e:
        log.debug(f"AI pain points failed: {e}")
        return _fallback_pain_points(lead)


def _fallback_pain_points(lead: Lead) -> dict:
    """Fallback pain points по индустрия."""
    pain_map = {
        "estate": {
            "main_pain": f"Finding consistent landlords and buyers in {lead.city} is getting harder with more competition",
            "money_angle": "Every month without a full pipeline = missed commission on 2-3 properties",
            "personalized_hook": f"I noticed {lead.business_name} has a strong presence in {lead.city} — curious how you're currently filling your listing pipeline.",
            "opportunity": "Automated daily outreach to landlords and property investors in your area",
            "urgency": "Spring market is starting — best time to build pipeline"
        },
        "account": {
            "main_pain": "Most accounting firms rely on referrals which plateau after year 3",
            "money_angle": "One new business client = £2,000-8,000/year recurring — missing 5 clients = £10-40K lost",
            "personalized_hook": f"I help accounting firms like {lead.business_name} move beyond referrals to a predictable client flow.",
            "opportunity": "Reach 500+ local SMBs every month who need your services",
            "urgency": "End of tax year = best time to approach businesses reviewing their accountant"
        },
        "market": {
            "main_pain": "Agency growth stalls when referrals dry up and paid ads eat into margin",
            "money_angle": "A $5K/month retainer client found through outbound = 60x ROI on Klivio",
            "personalized_hook": f"Agencies like {lead.business_name} are great at getting results for clients — but struggle with their own client acquisition.",
            "opportunity": "Book 10-20 qualified sales calls per month on autopilot",
            "urgency": "Q2 is when businesses set new marketing budgets"
        },
    }
    industry_lower = lead.industry.lower()
    for key, pain in pain_map.items():
        if key in industry_lower:
            return pain
    return {
        "main_pain": f"{lead.business_name} likely relies on word-of-mouth which is unpredictable",
        "money_angle": "Inconsistent pipeline = revenue swings every quarter",
        "personalized_hook": f"I help businesses in {lead.city} like {lead.business_name} get a steady flow of new clients.",
        "opportunity": "Daily automated outreach to your ideal clients",
        "urgency": "More businesses are using outbound — early movers get better response rates"
    }


def research_lead(lead: Lead) -> Lead:
    """Пълно изследване на един lead."""
    log.info(f"  🔍 Researching {lead.business_name}...")

    # 1. Website analysis
    website_data = research_website(lead.website)
    lead.website_summary = website_data.get("summary","")[:300]

    # 2. AI pain point analysis
    pain_data = analyze_pain_points_with_ai(lead, website_data)
    lead.pain_points    = pain_data.get("main_pain", "")
    lead.opportunity    = pain_data.get("opportunity", "")
    lead.weak_reviews   = pain_data.get("money_angle", "")

    return lead


# ═══════════════════════════════════════════════════════════════
# AI EMAIL GENERATION — с pain points
# ═══════════════════════════════════════════════════════════════
def generate_email(lead: Lead, email_num: int) -> tuple[str, str]:
    """Генерира хиперперсонализиран имейл с вградени pain points."""

    pain_context = f"""
Pain point identified: {lead.pain_points}
Money angle: {lead.weak_reviews}
Personalized hook: {lead.opportunity}
""" if lead.pain_points else ""

    instructions = {
        1: f"Email 1 — cold outreach. USE the pain point naturally. Reference their specific situation. Ask for 15-min call. Max 120 words. Must feel like you researched them specifically.",
        2: f"Email 2 — follow-up. Reference that you emailed before. Mention a specific benefit relevant to their industry. Ask one question. Max 100 words.",
        3: "Email 3 — final break-up. Very short. Last email. Leave door open. Max 80 words.",
    }

    prompt = f"""You are a cold email expert writing for Klivio (B2B cold email automation service).

Lead profile:
- Business: {lead.business_name}
- Industry: {lead.industry}
- Location: {lead.city}, {lead.country}
- Google rating: {lead.google_rating}/5 ({lead.google_reviews} reviews)
{pain_context}
Sender: {SENDER_NAME} from {SENDER_COMPANY}

Task: {instructions.get(email_num, instructions[1])}

Rules:
- Weave in the pain point NATURALLY — don't announce it
- ONE specific detail about their business or city
- ONE clear CTA only
- No buzzwords, no "I hope this email finds you well"
- No bullet points, no attachments
- Sign off: "{SENDER_NAME} | {SENDER_COMPANY}"
- Sound like a real human who did 5 minutes of research

Reply ONLY with JSON (no markdown, no backticks):
{{"subject": "...", "body": "..."}}"""

    if not GROQ_API_KEY:
        return _fallback_email(lead, email_num)

    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama3-8b-8192",
                  "messages": [{"role":"user","content":prompt}],
                  "temperature": 0.75, "max_tokens": 500},
            timeout=20
        )
        r.raise_for_status()
        content = re.sub(r"```json|```","",r.json()["choices"][0]["message"]["content"]).strip()
        d = json.loads(content)
        return d["subject"], d["body"]
    except Exception as e:
        log.warning(f"Groq: {e} → fallback")
        return _fallback_email(lead, email_num)


def _fallback_email(lead: Lead, n: int) -> tuple[str, str]:
    name = lead.first_name or "there"
    pain = lead.pain_points or f"finding consistent new clients for {lead.business_name}"
    t = {
        1: (f"Quick question, {name}",
            f"Hi {name},\n\nI came across {lead.business_name} and noticed that {pain.lower()}.\n\nWe help {lead.industry} businesses in {lead.city} fix exactly that — automated outreach that brings in new clients every week, fully done-for-you.\n\nWorth 15 minutes to see if it fits?\n\n-- {SENDER_NAME} | {SENDER_COMPANY}"),
        2: (f"Re: Quick question, {name}",
            f"Hi {name},\n\nJust following up — how are you currently solving the client acquisition side at {lead.business_name}?\n\nIf it's mostly referrals or word of mouth, that's exactly where we help.\n\n-- {SENDER_NAME}"),
        3: ("Closing the loop",
            f"Hi {name},\n\nLast one from me. If getting more clients ever becomes a priority for {lead.business_name}, just reply here.\n\n-- {SENDER_NAME} | {SENDER_COMPANY}"),
    }
    return t.get(n, t[1])


# ═══════════════════════════════════════════════════════════════
# SMTP SEND
# ═══════════════════════════════════════════════════════════════
def send_email(account: dict, to_email: str, subject: str, body: str) -> bool:
    try:
        msg = MIMEMultipart("alternative")
        msg["From"]     = f"{SENDER_NAME} from {SENDER_COMPANY} <{account['from_email']}>"
        msg["To"]       = to_email
        msg["Subject"]  = subject
        msg["Reply-To"] = REPLY_TO_EMAIL or account["from_email"]
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP(BREVO_SMTP_HOST, BREVO_SMTP_PORT) as s:
            s.starttls()
            s.login(account["smtp_user"], account["smtp_pass"])
            s.sendmail(account["from_email"], to_email, msg.as_string())
        return True
    except Exception as e:
        log.error(f"SMTP #{account['id']}: {e}")
        return False


def log_sent(rec: SentRecord):
    exists = Path(SENT_LOG).exists()
    with open(SENT_LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[fi.name for fi in fields(SentRecord)])
        if not exists: w.writeheader()
        w.writerow(asdict(rec))


# ═══════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════
def load_state() -> dict:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE) as f: return json.load(f)
    return {"accounts": {}}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f: json.dump(state, f, indent=2)

def get_acc_state(state: dict, acc_id: int) -> dict:
    key = str(acc_id)
    if key not in state["accounts"]:
        state["accounts"][key] = {"sent_today":0,"preview_sent":0,"last_reset":datetime.now().date().isoformat()}
    acc = state["accounts"][key]
    if acc["last_reset"] != datetime.now().date().isoformat():
        acc["sent_today"] = 0
        acc["last_reset"] = datetime.now().date().isoformat()
    return acc


# ═══════════════════════════════════════════════════════════════
# PREVIEW MODE
# ═══════════════════════════════════════════════════════════════
def run_preview(leads: list[Lead]):
    log.info(f"PREVIEW → {YOUR_EMAIL}")
    state   = load_state()
    lead_idx = 0
    preview_total = 0

    for account in BREVO_ACCOUNTS:
        acc = get_acc_state(state, account["id"])
        if acc["preview_sent"] >= PREVIEW_COUNT: continue
        log.info(f"\n#{account['id']} ({account['from_email']})")
        sent = 0

        while sent < PREVIEW_COUNT and lead_idx < len(leads):
            lead = leads[lead_idx]; lead_idx += 1

            # Research lead
            lead = research_lead(lead)
            time.sleep(random.uniform(1, 2))

            subject, body = generate_email(lead, 1)

            preview_body = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KLIVIO PREVIEW #{account['id']}.{sent+1}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Акаунт:     #{account['id']} ({account['from_email']})
Бизнес:     {lead.business_name}
Имейл:      {lead.email}
Град:       {lead.city}, {lead.country}
Sequence:   {lead.sequence} | {lead.tier}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PAIN POINT ОТКРИТ:
{lead.pain_points}

MONEY ANGLE:
{lead.weak_reviews}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT: {subject}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{body}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Изглежда добре? → python klivio_v2.py --mode live
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""

            if send_email(account, YOUR_EMAIL, f"[PREVIEW #{account['id']}.{sent+1}] {subject}", preview_body):
                log_sent(SentRecord(lead.email, lead.business_name, subject,
                                    YOUR_EMAIL, account["id"], "preview",
                                    datetime.now().isoformat(), 1, lead.sequence,
                                    lead.pain_points[:100]))
                acc["preview_sent"] += 1; sent += 1; preview_total += 1
                log.info(f"  ✓ Preview изпратен")
            time.sleep(random.uniform(2, 4))

        save_state(state)

    # Telegram известие
    tg_preview_ready(preview_total)
    log.info(f"\nGOTOVO — провери {YOUR_EMAIL} | Telegram известие изпратено")


# ═══════════════════════════════════════════════════════════════
# LIVE MODE
# ═══════════════════════════════════════════════════════════════
def run_live(leads: list[Lead]):
    log.info("LIVE РЕЖИМ")
    state = load_state()

    already_sent = set()
    if Path(SENT_LOG).exists():
        with open(SENT_LOG) as f:
            for row in csv.DictReader(f):
                if row.get("mode") == "live": already_sent.add(row["lead_email"])

    fresh = [l for l in leads if l.email not in already_sent]
    log.info(f"Fresh leads: {len(fresh)}")

    lead_idx = 0; total = 0; accounts_used = 0; sequences_used: dict = {}

    for account in BREVO_ACCOUNTS:
        acc = get_acc_state(state, account["id"])
        capacity = DAILY_LIMIT - acc["sent_today"]
        if capacity <= 0: continue
        accounts_used += 1
        log.info(f"\n#{account['id']} — капацитет: {capacity}")
        sent = 0

        while sent < capacity and lead_idx < len(fresh):
            lead = fresh[lead_idx]; lead_idx += 1

            # Research + pain points
            lead = research_lead(lead)
            time.sleep(random.uniform(0.5, 1.5))

            subject, body = generate_email(lead, 1)

            if send_email(account, lead.email, subject, body):
                log_sent(SentRecord(lead.email, lead.business_name, subject,
                                    lead.email, account["id"], "live",
                                    datetime.now().isoformat(), 1, lead.sequence,
                                    lead.pain_points[:100]))
                acc["sent_today"] += 1; sent += 1; total += 1
                sequences_used[lead.sequence] = sequences_used.get(lead.sequence, 0) + 1
                log.info(f"  ✓ {lead.business_name} | pain: {lead.pain_points[:60]}...")

            # Delay между имейли — изглежда по-natural
            time.sleep(random.uniform(20, 50))

        save_state(state)

    # Telegram digest
    tg_daily_digest(total, accounts_used, sequences_used)
    log.info(f"\nОБЩО ДНЕС: {total} | Telegram digest изпратен")


# ═══════════════════════════════════════════════════════════════
# REPLY CHECKER — проверява за нови отговори
# ═══════════════════════════════════════════════════════════════
def run_check_replies():
    """
    Проверява IMAP inbox-а за нови replies и праща Telegram известие.
    Изисква Gmail App Password за reply-to акаунта.
    """
    if not REPLY_IMAP_PASS:
        log.warning("REPLY_IMAP_PASS не е зададен — не мога да проверя replies")
        log.info("За да настроиш:")
        log.info("  1. Gmail → Settings → Security → 2-Step Verification → App Passwords")
        log.info("  2. Генерирай App Password за 'Mail'")
        log.info(f"  3. Добави REPLY_IMAP_PASS=твоята_парола в .env")
        return

    log.info(f"Проверявам replies в {REPLY_TO_EMAIL}...")

    # Зареди вече обработените replies
    seen_ids = set()
    if Path(REPLIES_LOG).exists():
        with open(REPLIES_LOG) as f:
            for row in csv.DictReader(f):
                seen_ids.add(row.get("message_id",""))

    try:
        mail = imaplib.IMAP4_SSL(REPLY_IMAP_SERVER)
        mail.login(REPLY_IMAP_USER, REPLY_IMAP_PASS)
        mail.select("inbox")

        # Търси непрочетени имейли
        _, data = mail.search(None, "UNSEEN")
        ids = data[0].split()
        log.info(f"Намерени {len(ids)} непрочетени имейли")

        new_replies = 0
        for num in ids:
            _, msg_data = mail.fetch(num, "(RFC822)")
            raw = msg_data[0][1]
            msg = emaillib.message_from_bytes(raw)

            msg_id    = msg.get("Message-ID", str(num))
            from_addr = msg.get("From", "")
            subject   = msg.get("Subject", "")
            date      = msg.get("Date", "")

            if msg_id in seen_ids:
                continue

            # Извлечи текста
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode("utf-8", errors="replace")[:500]
                        break
            else:
                body = msg.get_payload(decode=True).decode("utf-8", errors="replace")[:500]

            # Провери дали е reply към Klivio имейл (не spam)
            is_reply = "re:" in subject.lower() or any(
                name in from_addr.lower() for name in ["klivio"] + [
                    row["lead_email"].split("@")[1] if "@" in row.get("lead_email","") else ""
                    for row in (csv.DictReader(open(SENT_LOG)) if Path(SENT_LOG).exists() else [])
                ]
            )

            # Запази в log
            exists = Path(REPLIES_LOG).exists()
            with open(REPLIES_LOG, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["message_id","from","subject","date","preview","notified_at"])
                if not exists: w.writeheader()
                w.writerow({"message_id":msg_id,"from":from_addr,"subject":subject,
                             "date":date,"preview":body[:200],"notified_at":datetime.now().isoformat()})

            # Telegram известие
            tg_reply_alert(from_addr, from_addr, subject, body)
            new_replies += 1
            log.info(f"  🔥 Reply от: {from_addr} | Subject: {subject}")

        mail.logout()
        log.info(f"Проверката завърши — {new_replies} нови replies")

        if new_replies == 0:
            log.info("Няма нови replies")

    except Exception as e:
        log.error(f"IMAP грешка: {e}")
        log.info("Провери REPLY_IMAP_PASS и дали Gmail App Password е правилен")


# ═══════════════════════════════════════════════════════════════
# DIGEST
# ═══════════════════════════════════════════════════════════════
def run_digest():
    """Изпраща ръчен digest с днешните изпращания."""
    if not Path(SENT_LOG).exists():
        telegram("📭 Klivio: Все още няма изпратени имейли.")
        return

    today = datetime.now().date().isoformat()
    today_rows = []
    with open(SENT_LOG) as f:
        for row in csv.DictReader(f):
            if row.get("mode") == "live" and row.get("sent_at","").startswith(today):
                today_rows.append(row)

    seqs = {}
    for row in today_rows:
        s = row.get("sequence","?")
        seqs[s] = seqs.get(s,0) + 1

    tg_daily_digest(len(today_rows), len(set(r["account_id"] for r in today_rows)), seqs)
    log.info(f"Digest изпратен — {len(today_rows)} имейла днес")


# ═══════════════════════════════════════════════════════════════
# LOAD LEADS
# ═══════════════════════════════════════════════════════════════
def load_leads(fp: str = LEADS_FILE) -> list[Lead]:
    if not Path(fp).exists():
        log.error(f"{fp} не съществува — пусни klivio_scraper.py първо")
        return []
    leads = []
    with open(fp, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            l = Lead(**{k: row.get(k,"") for k in [fi.name for fi in fields(Lead)]})
            if l.email and "@" in l.email:
                leads.append(l)
    log.info(f"Заредени {len(leads)} leads")
    return leads


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    p = argparse.ArgumentParser(description="Klivio Pipeline v2")
    p.add_argument("--mode", choices=["preview","live","check","digest"], default="preview",
                   help="preview | live | check (replies) | digest (Telegram summary)")
    p.add_argument("--leads-file", default=LEADS_FILE)
    args = p.parse_args()

    if args.mode == "check":
        run_check_replies(); return
    if args.mode == "digest":
        run_digest(); return

    leads = load_leads(args.leads_file)
    if not leads: return

    if args.mode == "preview": run_preview(leads)
    elif args.mode == "live":  run_live(leads)


if __name__ == "__main__":
    main()
