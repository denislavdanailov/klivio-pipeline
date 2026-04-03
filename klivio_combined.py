#!/usr/bin/env python3
"""
Klivio — Всичко в един файл
1. Scraping (Common Crawl)
2. AI имейли (Groq)  
3. Изпращане (Brevo)
4. Telegram известие
"""

import csv, json, logging, os, random, re, smtplib, time
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("klivio")

# ── КОНФИГ ────────────────────────────────────────────────────────
YOUR_EMAIL       = os.getenv("YOUR_EMAIL", "danailovd48@gmail.com")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SENDER_NAME      = "James"
SENDER_COMPANY   = "Klivio"
BREVO_HOST       = "smtp-relay.brevo.com"
BREVO_PORT       = 587
DAILY_LIMIT      = 50
LEADS_FILE       = "leads.csv"
SENT_LOG         = "sent_log.csv"

BREVO_ACCOUNTS = [
    {"id":  1, "from_email": "james@klivio.bond",    "smtp_user": "a48308001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_1","")},
    {"id":  2, "from_email": "oliver@klivio.bond",   "smtp_user": "a4a8e6001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_2","")},
    {"id":  3, "from_email": "harry@klivio.bond",    "smtp_user": "a4f3a6001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_3","")},
    {"id":  4, "from_email": "george@klivio.bond",   "smtp_user": "a4f936001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_4","")},
    {"id":  5, "from_email": "emma@klivio.bond",     "smtp_user": "a482e2001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_5","")},
    {"id":  6, "from_email": "jack@klivio.bond",     "smtp_user": "a5f48a001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_6","")},
    {"id":  7, "from_email": "samuel@klivio.bond",   "smtp_user": "a5f4e5001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_7","")},
    {"id":  8, "from_email": "jessica@klivio.bond",  "smtp_user": "a5f520001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_8","")},
    {"id":  9, "from_email": "emily@klivio.bond",    "smtp_user": "a5f582001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_9","")},
    {"id": 10, "from_email": "sarah@klivio.bond",    "smtp_user": "a5f6ab001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_10","")},
    {"id": 11, "from_email": "william@klivio.site",  "smtp_user": "a5bc51001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_11","")},
    {"id": 12, "from_email": "thomas@klivio.site",   "smtp_user": "a5d742001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_12","")},
    {"id": 13, "from_email": "michael@klivio.site",  "smtp_user": "a5d898001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_13","")},
    {"id": 14, "from_email": "amy@klivio.site",      "smtp_user": "a5f997001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_14","")},
    {"id": 15, "from_email": "laura@klivio.site",    "smtp_user": "a5f9f8001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_15","")},
    {"id": 16, "from_email": "benjamin@klivio.site", "smtp_user": "a5fa73001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_16","")},
    {"id": 17, "from_email": "edward@klivio.site",   "smtp_user": "a611ce001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_17","")},
    {"id": 18, "from_email": "joseph@klivio.site",   "smtp_user": "a6128f001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_18","")},
    {"id": 19, "from_email": "charles@klivio.site",  "smtp_user": "a61985001@smtp-brevo.com",  "smtp_pass": os.getenv("BREVO_PASS_19","")},
]

HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/122.0.0.0 Safari/537.36", "Accept-Language": "en-GB,en;q=0.9"}

# ── DATA ──────────────────────────────────────────────────────────
@dataclass
class Lead:
    business_name: str = ""; first_name: str = ""; email: str = ""
    phone: str = ""; website: str = ""; city: str = ""; country: str = ""
    industry: str = ""; source: str = ""; tier: str = "Tier1"; sequence: str = "B"
    scraped_at: str = ""; pain_points: str = ""

CSV_FIELDS = [f.name for f in fields(Lead)]

# ── TELEGRAM ──────────────────────────────────────────────────────
def tg(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except: pass

# ── SCRAPING ──────────────────────────────────────────────────────
def safe_get(url, params=None):
    for i in range(2):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code == 200: return r
        except: pass
        time.sleep(2)
    return None

def extract_email(text):
    skip = ["noreply","no-reply","donotreply","example","test@","spam","abuse","privacy"]
    for m in re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text):
        if not any(s in m.lower() for s in skip):
            return m
    return ""

def get_from_url(url, industry, country):
    try:
        r = safe_get(url)
        if not r: return None
        soup = BeautifulSoup(r.text, "html.parser")
        name = ""
        t = soup.find("title")
        if t: name = re.sub(r"\s*[-|:—]\s*.*$", "", t.get_text(strip=True)).strip()[:70]
        if not name: return None
        email = ""
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                c = a["href"].replace("mailto:","").split("?")[0].strip()
                if c and "@" in c and not any(s in c.lower() for s in ["noreply","no-reply","example"]):
                    email = c; break
        if not email: email = extract_email(soup.get_text())
        if not email: return None
        parsed = urlparse(url)
        return Lead(business_name=name, email=email, website=f"{parsed.scheme}://{parsed.netloc}",
                    country=country, industry=industry, source="common_crawl",
                    scraped_at=datetime.now().isoformat())
    except: return None

def scrape(industry, country, limit=60):
    leads = []
    tld = {"UK":".co.uk","Ireland":".ie","Australia":".com.au","Canada":".ca","UAE":".ae"}.get(country,".com")
    kws = {"estate agent":["estateagent","propertysales","lettings"],
           "marketing agency":["marketingagency","digitalagency"],
           "accountant":["accountingfirm","taxservices"],
           "mortgage broker":["mortgagebroker","mortgageadvice"],
           "software company":["techcompany","saasplatform"],
           "digital agency":["digitalagency","webagency"]}.get(industry,[industry.replace(" ","")])
    
    cdx = "https://index.commoncrawl.org/CC-MAIN-2024-51-index"
    for kw in kws[:2]:
        q = f"*{tld}/{kw}*" if country != "Global" else f"*.com/{kw}*"
        try:
            r = requests.get(cdx, params={"url":q,"output":"json","limit":40,"fl":"url","filter":"status:200"}, timeout=25)
            if not r.ok: continue
            urls = []
            for line in r.text.strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    u = obj.get("url","")
                    if u and not any(s in u for s in ["wikipedia","facebook","twitter","instagram","linkedin"]):
                        urls.append(u)
                except: continue
            log.info(f"  CDX '{kw}': {len(urls)} URLs")
            for url in urls[:20]:
                if len(leads) >= limit: break
                lead = get_from_url(url, industry, country)
                if lead: leads.append(lead)
                time.sleep(random.uniform(0.5,1.5))
        except Exception as e:
            log.warning(f"CDX error: {e}")
    return leads

# ── AI EMAIL ──────────────────────────────────────────────────────
def ai_email(lead, num=1):
    instructions = {
        1: f"Cold email #1. Mention {lead.city or lead.country} and {lead.business_name}. Ask for 15-min call. Max 100 words.",
        2: f"Follow-up email. Reference previous email. One question about client acquisition. Max 80 words.",
        3: "Final break-up email. Very short. Leave door open. Max 60 words."
    }
    prompt = f"""Cold email copywriter for Klivio (B2B cold email automation service).
Lead: {lead.business_name} | {lead.industry} | {lead.city}, {lead.country}
Sender: {SENDER_NAME} from {SENDER_COMPANY}
Task: {instructions.get(num, instructions[1])}
Rules: No links, no bullets, ONE CTA, conversational, no "I hope this finds you well".
Sign off: "{SENDER_NAME} | {SENDER_COMPANY}"
Reply ONLY with JSON: {{"subject":"...","body":"..."}}"""
    
    if not GROQ_API_KEY:
        return fallback_email(lead, num)
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model":"llama3-8b-8192","messages":[{"role":"user","content":prompt}],
                  "temperature":0.75,"max_tokens":400}, timeout=20)
        r.raise_for_status()
        content = re.sub(r"```json|```","",r.json()["choices"][0]["message"]["content"]).strip()
        d = json.loads(content)
        return d["subject"], d["body"]
    except Exception as e:
        log.warning(f"Groq: {e}")
        return fallback_email(lead, num)

def fallback_email(lead, n):
    name = lead.first_name or "there"
    t = {
        1: (f"Quick question, {name}",
            f"Hi {name},\n\nI help {lead.industry} businesses in {lead.country} get more clients through automated outreach — no ads needed.\n\nWorth a 15-min call to see if it fits {lead.business_name}?\n\n-- {SENDER_NAME} | {SENDER_COMPANY}"),
        2: (f"Re: Quick question, {name}",
            f"Hi {name},\n\nFollowing up — how are you currently finding new clients for {lead.business_name}?\n\nIf it's mainly referrals, that's exactly what we fix.\n\n-- {SENDER_NAME}"),
        3: ("Closing the loop",
            f"Hi {name},\n\nLast one from me. If outbound ever becomes a priority, just reply.\n\n-- {SENDER_NAME} | {SENDER_COMPANY}")
    }
    return t.get(n, t[1])

# ── SEND ──────────────────────────────────────────────────────────
def send(account, to_email, subject, body):
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{SENDER_NAME} from {SENDER_COMPANY} <{account['from_email']}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg["Reply-To"] = account["from_email"]
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT) as s:
            s.starttls()
            s.login(account["smtp_user"], account["smtp_pass"])
            s.sendmail(account["from_email"], to_email, msg.as_string())
        return True
    except Exception as e:
        log.error(f"SMTP #{account['id']}: {e}")
        return False

def log_sent(email, name, subject, account_id):
    exists = Path(SENT_LOG).exists()
    with open(SENT_LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["email","business","subject","account_id","sent_at"])
        if not exists: w.writeheader()
        w.writerow({"email":email,"business":name,"subject":subject,
                    "account_id":account_id,"sent_at":datetime.now().isoformat()})

# ── PREVIEW ──────────────────────────────────────────────────────
def send_preview(account, lead, subject, body, num):
    preview = f"""━━━━━━━━━━━━━━━━━━━━━━━
KLIVIO PREVIEW #{account['id']}.{num}
Акаунт: {account['from_email']}
Бизнес: {lead.business_name}
Имейл:  {lead.email}
Държава: {lead.country}
━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT: {subject}
━━━━━━━━━━━━━━━━━━━━━━━

{body}"""
    return send(account, YOUR_EMAIL, f"[PREVIEW #{account['id']}.{num}] {subject}", preview)

# ── MAIN ─────────────────────────────────────────────────────────
CAMPAIGNS = [
    {"industry":"estate agent",      "country":"UK"},
    {"industry":"marketing agency",  "country":"UK"},
    {"industry":"accountant",        "country":"UK"},
    {"industry":"mortgage broker",   "country":"Ireland"},
    {"industry":"estate agent",      "country":"Australia"},
    {"industry":"accountant",        "country":"Canada"},
    {"industry":"real estate",       "country":"UAE"},
    {"industry":"marketing agency",  "country":"Global"},
    {"industry":"software company",  "country":"Global"},
    {"industry":"digital agency",    "country":"Global"},
]

def main():
    preview_mode = os.getenv("PREVIEW_MODE", "false").lower() == "true"
    
    log.info("="*50)
    log.info(f"Klivio {'PREVIEW' if preview_mode else 'LIVE'} — старт")
    log.info("="*50)

    # 1. SCRAPING
    log.info("Стъпка 1: Scraping...")
    existing_emails = set()
    if Path(SENT_LOG).exists():
        with open(SENT_LOG) as f:
            for row in csv.DictReader(f):
                existing_emails.add(row.get("email",""))

    all_leads = []
    for c in CAMPAIGNS:
        log.info(f"  Scraping: {c['industry']} / {c['country']}")
        leads = scrape(c["industry"], c["country"])
        new = [l for l in leads if l.email and l.email not in existing_emails]
        all_leads.extend(new)
        for l in new: existing_emails.add(l.email)
        log.info(f"  ✓ {len(new)} нови leads")

    log.info(f"Общо нови leads: {len(all_leads)}")

    if not all_leads:
        msg = "⚠️ Klivio: Няма нови leads днес. Scraper-ът не намери нови имейли."
        log.warning(msg)
        tg(msg)
        return

    # 2. ИЗПРАЩАНЕ
    log.info("Стъпка 2: Генериране и изпращане...")
    lead_idx = 0
    total_sent = 0
    preview_sent = 0

    for account in BREVO_ACCOUNTS:
        if not account["smtp_pass"]:
            log.warning(f"#{account['id']}: няма парола — пропускам")
            continue
        if lead_idx >= len(all_leads):
            break

        if preview_mode:
            # Preview — само 2 имейла на акаунт → към теб
            for i in range(2):
                if lead_idx >= len(all_leads): break
                lead = all_leads[lead_idx]; lead_idx += 1
                subject, body = ai_email(lead, 1)
                if send_preview(account, lead, subject, body, i+1):
                    preview_sent += 1
                    log.info(f"  ✓ Preview #{account['id']}.{i+1} → {YOUR_EMAIL}")
                time.sleep(random.uniform(2,4))
        else:
            # Live — до DAILY_LIMIT имейла на акаунт
            sent = 0
            while sent < DAILY_LIMIT and lead_idx < len(all_leads):
                lead = all_leads[lead_idx]; lead_idx += 1
                subject, body = ai_email(lead, 1)
                if send(account, lead.email, subject, body):
                    log_sent(lead.email, lead.business_name, subject, account["id"])
                    sent += 1; total_sent += 1
                    log.info(f"  ✓ {lead.business_name} ({lead.email})")
                time.sleep(random.uniform(15,40))

    # 3. TELEGRAM
    if preview_mode:
        tg(f"👀 <b>Klivio Preview</b>\n\n{preview_sent} имейла пратени на {YOUR_EMAIL}\n\nАко изглеждат ОК → смени PREVIEW_MODE на false в Secrets и пусни отново.")
        log.info(f"Preview готов — {preview_sent} имейла → {YOUR_EMAIL}")
    else:
        tg(f"✅ <b>Klivio Daily</b>\n\n📤 Изпратени: {total_sent}\n📋 Leads намерени: {len(all_leads)}\n📅 {datetime.now().strftime('%d %b %Y %H:%M')}")
        log.info(f"ГОТОВО — изпратени {total_sent} имейла")

if __name__ == "__main__":
    main()
