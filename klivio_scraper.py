#!/usr/bin/env python3
"""
Klivio Lead Scraper — Master Script
Поддържа: Google Maps, Yell.com, Yellow Pages AU/CA, Clutch.co,
          ProductHunt, LinkedIn (public), Indie Hackers
Изход: leads.csv с всички полета за klivio_ai_system.py
"""

import asyncio
import csv
import json
import os
import random
import re
import time
import logging
from dataclasses import dataclass, fields, asdict
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode, quote_plus

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")]
)
log = logging.getLogger("klivio")

# ─── КОНФИГУРАЦИЯ ────────────────────────────────────────────────
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")   # за Google Maps API
OUTPUT_FILE = "leads.csv"
DELAY_MIN = 1.5    # секунди между заявки
DELAY_MAX = 3.5
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── DATA MODEL ─────────────────────────────────────────────────
@dataclass
class Lead:
    business_name: str = ""
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    phone: str = ""
    website: str = ""
    city: str = ""
    country: str = ""
    industry: str = ""
    source: str = ""
    google_rating: str = ""
    google_reviews: str = ""
    linkedin_url: str = ""
    tier: str = "Tier3"           # Tier1 / Tier2 / Tier3
    sequence: str = "E"           # A B C D E F G1 G2 G3
    scraped_at: str = ""
    notes: str = ""

CSV_FIELDS = [f.name for f in fields(Lead)]

def save_leads(leads: list[Lead], filepath: str = OUTPUT_FILE):
    """Запазва leads в CSV — appends ако файлът съществува."""
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for lead in leads:
            lead.scraped_at = datetime.now().isoformat()
            writer.writerow(asdict(lead))
    log.info(f"Saved {len(leads)} leads → {filepath}")


def random_delay():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def safe_get(url: str, params: dict = None, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            time.sleep(2 ** attempt)
    return None


def extract_email_from_text(text: str) -> str:
    """Извлича първия имейл от произволен текст."""
    match = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    return match.group(0) if match else ""


def extract_email_from_website(url: str) -> str:
    """Посещава website и търси имейл адрес."""
    if not url:
        return ""
    try:
        r = safe_get(url)
        if not r:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        # Търси mailto: линкове
        for a in soup.find_all("a", href=True):
            if "mailto:" in a["href"]:
                return a["href"].replace("mailto:", "").split("?")[0].strip()
        # Търси в текста
        return extract_email_from_text(soup.get_text())
    except Exception as e:
        log.debug(f"Email extract failed for {url}: {e}")
        return ""


def assign_tier_and_sequence(industry: str, source: str) -> tuple[str, str]:
    """Определя Tier и Sequence по индустрия."""
    industry_lower = industry.lower()
    source_lower = source.lower()

    # Online бизнеси → Tier 1
    if any(k in source_lower for k in ["clutch", "producthunt", "indie", "g2", "linkedin"]):
        if any(k in industry_lower for k in ["saas", "software", "tech", "dev", "app"]):
            return "Tier1", "G1"
        if any(k in industry_lower for k in ["agency", "seo", "content", "marketing", "design"]):
            return "Tier1", "G2"
        if any(k in industry_lower for k in ["coach", "consult", "training"]):
            return "Tier1", "G3"
        return "Tier1", "G2"  # default за online

    # Offline Tier 1
    if any(k in industry_lower for k in ["estate", "real estate", "property", "letting", "realtor"]):
        return "Tier1", "A"
    if any(k in industry_lower for k in ["marketing", "digital", "it ", "msp", "managed service", "web design"]):
        return "Tier1", "B"
    if any(k in industry_lower for k in ["account", "mortgage", "financial", "solicitor", "legal", "insurance"]):
        return "Tier1", "C"

    # Tier 2
    if any(k in industry_lower for k in ["dental", "physio", "health", "clinic", "hvac", "plumb", "electric", "builder", "construct"]):
        return "Tier2", "D"

    return "Tier3", "E"


# ─── SCRAPER 1: GOOGLE MAPS API ──────────────────────────────────
def scrape_google_maps(keyword: str, location: str, country: str, max_results: int = 60) -> list[Lead]:
    """
    Scrape Google Maps Places API.
    Изисква GOOGLE_MAPS_API_KEY в .env
    """
    if not GOOGLE_MAPS_API_KEY:
        log.warning("GOOGLE_MAPS_API_KEY not set — skipping Google Maps scraper")
        return []

    leads = []
    query = f"{keyword} in {location}"
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": GOOGLE_MAPS_API_KEY, "language": "en"}

    log.info(f"Google Maps: '{query}'")

    while len(leads) < max_results:
        r = safe_get(url, params=params)
        if not r:
            break
        data = r.json()
        results = data.get("results", [])
        if not results:
            break

        for place in results:
            name = place.get("name", "")
            address = place.get("formatted_address", "")
            rating = str(place.get("rating", ""))
            reviews = str(place.get("user_ratings_total", ""))
            place_id = place.get("place_id", "")

            # Details call за website + phone
            website, phone = "", ""
            if place_id:
                det_url = "https://maps.googleapis.com/maps/api/place/details/json"
                det_params = {"place_id": place_id, "fields": "website,formatted_phone_number", "key": GOOGLE_MAPS_API_KEY}
                det_r = safe_get(det_url, params=det_params)
                if det_r:
                    det = det_r.json().get("result", {})
                    website = det.get("website", "")
                    phone = det.get("formatted_phone_number", "")
                random_delay()

            email = extract_email_from_website(website)
            tier, seq = assign_tier_and_sequence(keyword, "google_maps")

            leads.append(Lead(
                business_name=name,
                email=email,
                phone=phone,
                website=website,
                city=location,
                country=country,
                industry=keyword,
                source="google_maps",
                google_rating=rating,
                google_reviews=reviews,
                tier=tier,
                sequence=seq,
            ))
            if len(leads) >= max_results:
                break

        next_token = data.get("next_page_token")
        if not next_token:
            break
        params = {"pagetoken": next_token, "key": GOOGLE_MAPS_API_KEY}
        time.sleep(2)  # Google изисква delay преди next_page_token

    log.info(f"Google Maps: {len(leads)} leads за '{query}'")
    return leads


# ─── SCRAPER 2: YELL.COM (UK) ────────────────────────────────────
def scrape_yell(keyword: str, location: str, max_pages: int = 5) -> list[Lead]:
    """Scrape Yell.com за UK бизнеси."""
    leads = []
    base_url = "https://www.yell.com/ucs/UcsSearchAction.do"

    for page in range(1, max_pages + 1):
        params = {
            "keywords": keyword,
            "location": location,
            "pageNum": page,
        }
        log.info(f"Yell.com: '{keyword}' в {location} — страница {page}")
        r = safe_get(base_url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all("article", class_=re.compile("businessCapsule"))

        if not listings:
            log.info(f"Yell: Няма повече резултати на страница {page}")
            break

        for listing in listings:
            name_tag = listing.find(class_=re.compile("businessCapsule--name"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            phone_tag = listing.find("span", class_=re.compile("business-phone"))
            phone = phone_tag.get_text(strip=True) if phone_tag else ""

            website_tag = listing.find("a", class_=re.compile("businessCapsule--website"))
            website = website_tag.get("href", "") if website_tag else ""
            if website.startswith("//"):
                website = "https:" + website

            email = extract_email_from_website(website) if website else ""
            tier, seq = assign_tier_and_sequence(keyword, "yell")

            leads.append(Lead(
                business_name=name,
                email=email,
                phone=phone,
                website=website,
                city=location,
                country="UK",
                industry=keyword,
                source="yell.com",
                tier=tier,
                sequence=seq,
            ))
            random_delay()

        random_delay()

    log.info(f"Yell.com: {len(leads)} leads за '{keyword}' в {location}")
    return leads


# ─── SCRAPER 3: YELLOW PAGES AUSTRALIA ───────────────────────────
def scrape_yellowpages_au(keyword: str, location: str, max_pages: int = 5) -> list[Lead]:
    """Scrape Yellow Pages Australia."""
    leads = []

    for page in range(1, max_pages + 1):
        url = f"https://www.yellowpages.com.au/search/listings?clue={quote_plus(keyword)}&locationClue={quote_plus(location)}&pageNumber={page}"
        log.info(f"YP Australia: '{keyword}' в {location} — страница {page}")
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all("div", class_=re.compile("listing-"))

        if not listings:
            break

        for listing in listings:
            name_tag = listing.find(class_=re.compile("listing-name"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            phone_tag = listing.find(class_=re.compile("phone"))
            phone = phone_tag.get_text(strip=True) if phone_tag else ""

            website_tag = listing.find("a", href=re.compile(r"^https?://"))
            website = website_tag.get("href", "") if website_tag else ""

            email = extract_email_from_website(website) if website else ""
            tier, seq = assign_tier_and_sequence(keyword, "yellowpages_au")

            if name:
                leads.append(Lead(
                    business_name=name,
                    email=email,
                    phone=phone,
                    website=website,
                    city=location,
                    country="Australia",
                    industry=keyword,
                    source="yellowpages_au",
                    tier=tier,
                    sequence=seq,
                ))
            random_delay()

        random_delay()

    log.info(f"YP AU: {len(leads)} leads")
    return leads


# ─── SCRAPER 4: YELLOW PAGES CANADA ──────────────────────────────
def scrape_yellowpages_ca(keyword: str, location: str, max_pages: int = 5) -> list[Lead]:
    """Scrape Yellow Pages Canada."""
    leads = []

    for page in range(1, max_pages + 1):
        url = f"https://www.yellowpages.ca/search/si/{page}/{quote_plus(keyword)}/{quote_plus(location)}"
        log.info(f"YP Canada: '{keyword}' в {location} — страница {page}")
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all("div", class_=re.compile("listing__content"))

        if not listings:
            break

        for listing in listings:
            name_tag = listing.find(class_=re.compile("listing__name"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            phone_tag = listing.find(class_=re.compile("listing__phone"))
            phone = phone_tag.get_text(strip=True) if phone_tag else ""

            website_tag = listing.find("a", class_=re.compile("website"))
            website = website_tag.get("href", "") if website_tag else ""

            email = extract_email_from_website(website) if website else ""
            tier, seq = assign_tier_and_sequence(keyword, "yellowpages_ca")

            if name:
                leads.append(Lead(
                    business_name=name,
                    email=email,
                    phone=phone,
                    website=website,
                    city=location,
                    country="Canada",
                    industry=keyword,
                    source="yellowpages_ca",
                    tier=tier,
                    sequence=seq,
                ))
            random_delay()

        random_delay()

    log.info(f"YP CA: {len(leads)} leads")
    return leads


# ─── SCRAPER 5: CLUTCH.CO (Online Agencies) ──────────────────────
def scrape_clutch(service: str, location: str = "", max_pages: int = 5) -> list[Lead]:
    """
    Scrape Clutch.co за digital agencies и tech companies.
    Перфектно за Sequence G2.
    """
    leads = []
    service_slug = service.lower().replace(" ", "-")

    for page in range(1, max_pages + 1):
        url = f"https://clutch.co/agencies/{service_slug}"
        params = {}
        if location:
            params["geographic_focus"] = location
        if page > 1:
            params["page"] = page

        log.info(f"Clutch: '{service}' — страница {page}")
        r = safe_get(url, params=params)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        companies = soup.find_all("li", class_=re.compile("provider-row"))

        if not companies:
            break

        for company in companies:
            name_tag = company.find(class_=re.compile("company_info"))
            if not name_tag:
                name_tag = company.find("h3")
            name = name_tag.get_text(strip=True) if name_tag else ""

            website_tag = company.find("a", class_=re.compile("website-link"))
            website = website_tag.get("href", "") if website_tag else ""

            email = extract_email_from_website(website) if website else ""

            location_tag = company.find(class_=re.compile("location"))
            city = location_tag.get_text(strip=True) if location_tag else location

            if name:
                leads.append(Lead(
                    business_name=name,
                    email=email,
                    website=website,
                    city=city,
                    country="Global",
                    industry=service,
                    source="clutch.co",
                    tier="Tier1",
                    sequence="G2",
                ))
            random_delay()

        random_delay()

    log.info(f"Clutch: {len(leads)} leads за '{service}'")
    return leads


# ─── SCRAPER 6: INDIE HACKERS ─────────────────────────────────────
def scrape_indie_hackers(max_pages: int = 3) -> list[Lead]:
    """
    Scrape Indie Hackers products за bootstrap SaaS founders.
    Sequence G1 — Tier 1.
    """
    leads = []

    for page in range(1, max_pages + 1):
        url = f"https://www.indiehackers.com/products?page={page}"
        log.info(f"Indie Hackers — страница {page}")
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        products = soup.find_all("div", class_=re.compile("product-summary"))

        if not products:
            break

        for product in products:
            name_tag = product.find(class_=re.compile("product-summary__name"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            website_tag = product.find("a", class_=re.compile("product-summary__tagline"))
            website = website_tag.get("href", "") if website_tag else ""

            founder_tag = product.find(class_=re.compile("user-link"))
            founder = founder_tag.get_text(strip=True) if founder_tag else ""

            email = extract_email_from_website(website) if website else ""

            if name:
                leads.append(Lead(
                    business_name=name,
                    first_name=founder,
                    email=email,
                    website=website,
                    city="Remote",
                    country="Global",
                    industry="SaaS / Startup",
                    source="indiehackers",
                    tier="Tier1",
                    sequence="G1",
                ))
            random_delay()

        random_delay()

    log.info(f"Indie Hackers: {len(leads)} leads")
    return leads


# ─── SCRAPER 7: GOLDEN PAGES IRELAND ────────────────────────────
def scrape_golden_pages(keyword: str, location: str, max_pages: int = 5) -> list[Lead]:
    """Scrape GoldenPages.ie за Ireland бизнеси."""
    leads = []

    for page in range(1, max_pages + 1):
        url = f"https://www.goldenpages.ie/q/business/advanced/where/{quote_plus(location)}/what/{quote_plus(keyword)}/page/{page}/"
        log.info(f"Golden Pages IE: '{keyword}' в {location} — страница {page}")
        r = safe_get(url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all("div", class_=re.compile("listing"))

        if not listings:
            break

        for listing in listings:
            name_tag = listing.find(class_=re.compile("listing-title"))
            name = name_tag.get_text(strip=True) if name_tag else ""

            phone_tag = listing.find(class_=re.compile("phone"))
            phone = phone_tag.get_text(strip=True) if phone_tag else ""

            website_tag = listing.find("a", href=re.compile(r"^https?://"))
            website = website_tag.get("href", "") if website_tag else ""

            email = extract_email_from_website(website) if website else ""
            tier, seq = assign_tier_and_sequence(keyword, "golden_pages")

            if name:
                leads.append(Lead(
                    business_name=name,
                    email=email,
                    phone=phone,
                    website=website,
                    city=location,
                    country="Ireland",
                    industry=keyword,
                    source="goldenpages.ie",
                    tier=tier,
                    sequence=seq,
                ))
            random_delay()

        random_delay()

    log.info(f"Golden Pages IE: {len(leads)} leads")
    return leads


# ─── MAIN CAMPAIGN CONFIG ────────────────────────────────────────
CAMPAIGNS = [
    # ── UK ──────────────────────────────────────────────────────
    {"scraper": "yell",        "keyword": "estate agent",         "location": "London",     "country": "UK"},
    {"scraper": "yell",        "keyword": "estate agent",         "location": "Manchester", "country": "UK"},
    {"scraper": "yell",        "keyword": "marketing agency",     "location": "London",     "country": "UK"},
    {"scraper": "yell",        "keyword": "accountant",           "location": "Birmingham", "country": "UK"},
    {"scraper": "yell",        "keyword": "mortgage broker",      "location": "London",     "country": "UK"},
    {"scraper": "google_maps", "keyword": "letting agent",        "location": "Leeds",      "country": "UK"},
    {"scraper": "google_maps", "keyword": "solicitor",            "location": "Manchester", "country": "UK"},

    # ── Ireland ─────────────────────────────────────────────────
    {"scraper": "golden_pages", "keyword": "estate agent",        "location": "Dublin",     "country": "Ireland"},
    {"scraper": "golden_pages", "keyword": "mortgage broker",     "location": "Dublin",     "country": "Ireland"},
    {"scraper": "golden_pages", "keyword": "accountant",          "location": "Cork",       "country": "Ireland"},

    # ── Australia ────────────────────────────────────────────────
    {"scraper": "yellowpages_au", "keyword": "real estate agent", "location": "Sydney",     "country": "Australia"},
    {"scraper": "yellowpages_au", "keyword": "mortgage broker",   "location": "Melbourne",  "country": "Australia"},
    {"scraper": "yellowpages_au", "keyword": "digital agency",    "location": "Brisbane",   "country": "Australia"},
    {"scraper": "google_maps",    "keyword": "dental clinic",     "location": "Perth",      "country": "Australia"},

    # ── Canada ───────────────────────────────────────────────────
    {"scraper": "yellowpages_ca", "keyword": "realtor",           "location": "Toronto",    "country": "Canada"},
    {"scraper": "yellowpages_ca", "keyword": "mortgage broker",   "location": "Vancouver",  "country": "Canada"},
    {"scraper": "yellowpages_ca", "keyword": "IT company",        "location": "Toronto",    "country": "Canada"},

    # ── UAE ──────────────────────────────────────────────────────
    {"scraper": "google_maps", "keyword": "real estate agency",   "location": "Dubai",      "country": "UAE"},
    {"scraper": "google_maps", "keyword": "property developer",   "location": "Abu Dhabi",  "country": "UAE"},
    {"scraper": "google_maps", "keyword": "financial consultant",  "location": "Dubai",     "country": "UAE"},

    # ── Online бизнеси (Global) ──────────────────────────────────
    {"scraper": "clutch",      "keyword": "digital-marketing",    "location": "",           "country": "Global"},
    {"scraper": "clutch",      "keyword": "seo",                  "location": "",           "country": "Global"},
    {"scraper": "clutch",      "keyword": "web-development",      "location": "",           "country": "Global"},
    {"scraper": "indie_hackers", "keyword": "",                   "location": "",           "country": "Global"},
]


def run_campaign(campaign: dict) -> list[Lead]:
    scraper = campaign["scraper"]
    keyword = campaign.get("keyword", "")
    location = campaign.get("location", "")
    country = campaign.get("country", "Global")

    if scraper == "yell":
        return scrape_yell(keyword, location)
    elif scraper == "yellowpages_au":
        return scrape_yellowpages_au(keyword, location)
    elif scraper == "yellowpages_ca":
        return scrape_yellowpages_ca(keyword, location)
    elif scraper == "golden_pages":
        return scrape_golden_pages(keyword, location)
    elif scraper == "google_maps":
        return scrape_google_maps(keyword, location, country)
    elif scraper == "clutch":
        return scrape_clutch(keyword, location)
    elif scraper == "indie_hackers":
        return scrape_indie_hackers()
    else:
        log.warning(f"Непознат scraper: {scraper}")
        return []


def main():
    log.info("=" * 60)
    log.info("Klivio Lead Scraper — старт")
    log.info(f"Брой campaigns: {len(CAMPAIGNS)}")
    log.info("=" * 60)

    total_leads = 0

    for i, campaign in enumerate(CAMPAIGNS, 1):
        log.info(f"\n[{i}/{len(CAMPAIGNS)}] {campaign['scraper'].upper()} — {campaign.get('keyword','')} {campaign.get('location','')}")
        try:
            leads = run_campaign(campaign)
            # Филтрирай само leads с имейл за по-добро качество
            leads_with_email = [l for l in leads if l.email]
            leads_without = [l for l in leads if not l.email]

            if leads_with_email:
                save_leads(leads_with_email, OUTPUT_FILE)
            if leads_without:
                save_leads(leads_without, "leads_no_email.csv")

            total_leads += len(leads)
            log.info(f"✓ {len(leads_with_email)} с имейл | {len(leads_without)} без имейл")

        except KeyboardInterrupt:
            log.info("Прекъснато от потребителя.")
            break
        except Exception as e:
            log.error(f"Грешка в campaign {i}: {e}", exc_info=True)
            continue

    log.info("\n" + "=" * 60)
    log.info(f"ГОТОВО — Общо leads: {total_leads}")
    log.info(f"Файл с имейли: {OUTPUT_FILE}")
    log.info(f"Файл без имейли: leads_no_email.csv")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
