#!/usr/bin/env python3
"""
Klivio Scraper v2 — Common Crawl + Clutch + Apollo Public
100% автоматично от GitHub Actions, без блокиране
"""

import csv, json, logging, os, random, re, time
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler()])
log = logging.getLogger("klivio")

OUTPUT_FILE = "leads.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.9",
}

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
    tier: str = "Tier1"
    sequence: str = "A"
    scraped_at: str = ""
    pain_points: str = ""
    website_summary: str = ""
    weak_reviews: str = ""
    opportunity: str = ""

CSV_FIELDS = [f.name for f in fields(Lead)]

def save_leads(leads):
    if not leads: return
    exists = Path(OUTPUT_FILE).exists()
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not exists: w.writeheader()
        for lead in leads:
            lead.scraped_at = datetime.now().isoformat()
            w.writerow(asdict(lead))
    log.info(f"Saved {len(leads)} leads → {OUTPUT_FILE}")

def extract_email(text):
    matches = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    skip = ["noreply","no-reply","donotreply","example","test","spam","abuse","privacy","support@support"]
    for m in matches:
        if not any(s in m.lower() for s in skip):
            return m
    return ""

def safe_get(url, params=None, timeout=15):
    for i in range(2):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 200: return r
        except: pass
        time.sleep(2)
    return None

def assign_sequence(industry):
    i = industry.lower()
    if any(k in i for k in ["estate","property","realtor","letting","real estate"]): return "Tier1","A"
    if any(k in i for k in ["marketing","agency","digital","seo","web","it ","software","tech","saas"]): return "Tier1","B"
    if any(k in i for k in ["account","finance","mortgage","legal","solicitor","insurance","tax"]): return "Tier1","C"
    if any(k in i for k in ["dental","health","physio","hvac","plumb","electric","build","trade"]): return "Tier2","D"
    return "Tier1","B"

# ─── SCRAPER 1: COMMON CRAWL CDX API ─────────────────────────────
def scrape_common_crawl(industry: str, country: str, max_results: int = 80) -> list[Lead]:
    """Common Crawl Index API — безплатно, без блокиране, работи от GitHub."""
    leads = []
    log.info(f"Common Crawl: '{industry}' / {country}")

    tld_map = {"UK":".co.uk","Ireland":".ie","Australia":".com.au","Canada":".ca","UAE":".ae","Global":".com"}
    tld = tld_map.get(country, ".com")

    # Keyword patterns за индустрията
    kw_map = {
        "estate agent":     ["estateagent","propertysales","lettingsagent","realestate"],
        "marketing agency": ["marketingagency","digitalagency","creativeagency","seoagency"],
        "accountant":       ["accountants","accounting","bookkeeping","taxadvisors"],
        "mortgage broker":  ["mortgagebroker","mortgageadvice","homeloans"],
        "digital agency":   ["digitalagency","webagency","onlinemarketing"],
        "real estate":      ["realestate","propertyagency","propertydevelopers"],
        "software company": ["software","techcompany","saas","appdev"],
        "accountant":       ["accountingfirm","taxservices","cpa"],
        "recruitment":      ["recruitment","staffingagency","hiring"],
        "dental":           ["dentalclinic","dentist","dentalcare"],
    }

    keywords = []
    for key, vals in kw_map.items():
        if any(k in industry.lower() for k in key.split()):
            keywords = vals
            break
    if not keywords:
        keywords = [industry.lower().replace(" ","")]

    # Latest Common Crawl index
    index_url = "https://index.commoncrawl.org/CC-MAIN-2024-51-index"

    for kw in keywords[:2]:
        query = f"*{tld}/{kw}*" if country != "Global" else f"*.com/{kw}*"
        try:
            r = requests.get(index_url, params={"url": query, "output": "json", "limit": 40,
                                                  "fl": "url", "filter": "status:200"}, timeout=25)
            if not r.ok:
                # Try alternative query format
                query2 = f"*{kw}*{tld}*"
                r = requests.get(index_url, params={"url": query2, "output": "json", "limit": 40,
                                                     "fl": "url", "filter": "status:200"}, timeout=25)
                if not r.ok: continue

            urls = []
            for line in r.text.strip().split("\n"):
                if not line: continue
                try:
                    obj = json.loads(line)
                    u = obj.get("url","")
                    if u and not any(skip in u for skip in ["wikipedia","facebook","linkedin","twitter","instagram"]):
                        urls.append(u)
                except: continue

            log.info(f"  CDX: {len(urls)} URLs за '{kw}'")

            for url in urls[:20]:
                if len(leads) >= max_results: break
                lead = extract_from_url(url, industry, country)
                if lead and lead.email and lead.business_name:
                    tier, seq = assign_sequence(industry)
                    lead.tier, lead.sequence = tier, seq
                    leads.append(lead)
                time.sleep(random.uniform(0.8, 1.8))

        except Exception as e:
            log.warning(f"CDX error for '{kw}': {e}")

    log.info(f"Common Crawl total: {len(leads)} leads с имейл")
    return leads


def extract_from_url(url: str, industry: str, country: str) -> Lead:
    try:
        r = safe_get(url)
        if not r: return None
        soup = BeautifulSoup(r.text, "html.parser")

        # Название
        name = ""
        title = soup.find("title")
        if title:
            name = re.sub(r"\s*[-|:—]\s*.*$", "", title.get_text(strip=True)).strip()[:70]
        if not name:
            og = soup.find("meta", property="og:site_name")
            if og: name = og.get("content","").strip()[:70]

        # Имейл
        email = ""
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                candidate = a["href"].replace("mailto:","").split("?")[0].strip()
                skip = ["noreply","no-reply","example","test","spam","abuse"]
                if candidate and not any(s in candidate.lower() for s in skip):
                    email = candidate
                    break
        if not email:
            email = extract_email(soup.get_text())

        if not email or not name: return None

        # Телефон
        phone = ""
        phone_match = re.search(r"(\+[\d\s\-\(\)]{9,18}|0[\d\s\-]{9,14})", soup.get_text())
        if phone_match: phone = phone_match.group(0).strip()

        parsed = urlparse(url)
        website = f"{parsed.scheme}://{parsed.netloc}"

        # Град от meta или contact page
        city = ""
        loc = soup.find("meta", attrs={"name": re.compile("geo.placename|location", re.I)})
        if loc: city = loc.get("content","").strip()

        return Lead(business_name=name, email=email, phone=phone, website=website,
                    city=city, country=country, industry=industry, source="common_crawl")
    except Exception as e:
        log.debug(f"Extract error {url}: {e}")
        return None


# ─── SCRAPER 2: CLUTCH.CO ─────────────────────────────────────────
def scrape_clutch(service: str, max_pages: int = 4) -> list[Lead]:
    """Clutch.co — работи от GitHub без блокиране."""
    leads = []
    slug = service.lower().replace(" ", "-")

    for page in range(1, max_pages + 1):
        url = f"https://clutch.co/agencies/{slug}"
        params = {"page": page} if page > 1 else {}
        r = safe_get(url, params=params)
        if not r: break

        soup = BeautifulSoup(r.text, "html.parser")
        companies = soup.find_all("li", class_=re.compile("provider-row|providers__item"))
        if not companies: break

        for co in companies:
            name_tag = co.find(class_=re.compile("company_info|provider__title|sg-provider-name"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            website = ""
            for a in co.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "clutch.co" not in href and "linkedin" not in href:
                    website = href
                    break

            loc_tag = co.find(class_=re.compile("locality|location|sg-provider-location"))
            city = loc_tag.get_text(strip=True) if loc_tag else ""

            email = ""
            if website:
                r2 = safe_get(website)
                if r2:
                    soup2 = BeautifulSoup(r2.text, "html.parser")
                    for a in soup2.find_all("a", href=True):
                        if "mailto:" in a["href"]:
                            email = a["href"].replace("mailto:","").split("?")[0].strip()
                            break
                    if not email: email = extract_email(soup2.get_text())
                time.sleep(random.uniform(1, 2))

            if name and email:
                leads.append(Lead(business_name=name, email=email, website=website or "",
                                  city=city, country="Global", industry=service,
                                  source="clutch.co", tier="Tier1", sequence="G2"))
        time.sleep(random.uniform(2, 3))

    log.info(f"Clutch: {len(leads)} leads за '{service}'")
    return leads


# ─── SCRAPER 3: APOLLO PUBLIC ─────────────────────────────────────
def scrape_apollo_public(industry: str, country: str) -> list[Lead]:
    """Apollo.io публични данни."""
    leads = []
    country_map = {"UK":"United Kingdom","Ireland":"Ireland","Australia":"Australia",
                   "Canada":"Canada","UAE":"United Arab Emirates"}
    country_name = country_map.get(country, country)

    try:
        r = requests.post("https://api.apollo.io/api/v1/mixed_companies/search",
            json={"q_organization_keyword_tags":[industry],
                  "organization_locations":[country_name], "page":1, "per_page":25},
            headers={**HEADERS,"Content-Type":"application/json"}, timeout=15)
        if not r.ok: return leads

        for c in r.json().get("organizations", []):
            name = c.get("name","")
            website = c.get("website_url","")
            city = c.get("city","")
            email = ""
            if website:
                r2 = safe_get(website)
                if r2:
                    soup = BeautifulSoup(r2.text, "html.parser")
                    for a in soup.find_all("a", href=True):
                        if "mailto:" in a["href"]:
                            email = a["href"].replace("mailto:","").split("?")[0].strip()
                            break
                    if not email: email = extract_email(soup.get_text())
                time.sleep(random.uniform(1, 2))

            if name and email:
                tier, seq = assign_sequence(industry)
                leads.append(Lead(business_name=name, email=email, website=website,
                                  city=city, country=country, industry=industry,
                                  source="apollo_public", tier=tier, sequence=seq))
    except Exception as e:
        log.warning(f"Apollo error: {e}")

    log.info(f"Apollo: {len(leads)} leads за '{industry}'")
    return leads


# ─── CAMPAIGNS ────────────────────────────────────────────────────
CAMPAIGNS = [
    {"scraper":"common_crawl", "industry":"estate agent",      "country":"UK"},
    {"scraper":"common_crawl", "industry":"marketing agency",  "country":"UK"},
    {"scraper":"common_crawl", "industry":"accountant",        "country":"UK"},
    {"scraper":"common_crawl", "industry":"mortgage broker",   "country":"Ireland"},
    {"scraper":"common_crawl", "industry":"estate agent",      "country":"Australia"},
    {"scraper":"common_crawl", "industry":"digital agency",    "country":"Australia"},
    {"scraper":"common_crawl", "industry":"accountant",        "country":"Canada"},
    {"scraper":"common_crawl", "industry":"real estate",       "country":"UAE"},
    {"scraper":"common_crawl", "industry":"marketing agency",  "country":"Global"},
    {"scraper":"common_crawl", "industry":"software company",  "country":"Global"},
    {"scraper":"clutch",       "industry":"digital-marketing", "country":"Global"},
    {"scraper":"clutch",       "industry":"seo",               "country":"Global"},
    {"scraper":"clutch",       "industry":"web-development",   "country":"Global"},
    {"scraper":"apollo",       "industry":"estate agent",      "country":"UK"},
    {"scraper":"apollo",       "industry":"marketing agency",  "country":"Australia"},
]

def main():
    log.info("="*55)
    log.info("Klivio Scraper v2 — Common Crawl + Clutch + Apollo")
    log.info(f"Campaigns: {len(CAMPAIGNS)}")
    log.info("="*55)

    existing = set()
    if Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing.add(row.get("email",""))
        log.info(f"Вече имаме {len(existing)} leads")

    total = 0
    for i, c in enumerate(CAMPAIGNS, 1):
        log.info(f"\n[{i}/{len(CAMPAIGNS)}] {c['scraper'].upper()} — {c.get('industry','')} / {c.get('country','')}")
        try:
            if c["scraper"] == "common_crawl":
                leads = scrape_common_crawl(c["industry"], c["country"])
            elif c["scraper"] == "clutch":
                leads = scrape_clutch(c["industry"])
            elif c["scraper"] == "apollo":
                leads = scrape_apollo_public(c["industry"], c["country"])
            else:
                leads = []

            new = [l for l in leads if l.email and l.email not in existing]
            if new:
                save_leads(new)
                for l in new: existing.add(l.email)
                total += len(new)
            log.info(f"✓ {len(new)} нови leads")
        except Exception as e:
            log.error(f"Campaign {i} грешка: {e}")

    log.info(f"\nГОТОВО — {total} нови leads в {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
