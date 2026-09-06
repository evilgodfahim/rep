#!/usr/bin/env python3
"""
BD Economics & Finance RSS Feed Processor

Fetches all feeds, deduplicates by link, sends titles to Gemini.
Gemini filters for broad Bangladesh economics and finance news only.
Both Bangla and English titles are evaluated.

Output:  econ_feed.xml
Stats:   econ_stats.json

Changes from previous version:
- Two-stage XML recovery: parse as-is → sanitize → fresh (never silently loses items)
- _sanitize_xml_bytes(): strips forbidden control chars + fixes bare & in URLs
- _safe_text(): escapes & < > in plain text nodes (<link>, <guid>) on write
- errors="replace" on file open guards against encoding corruption
- googlenewsdecoder (PyPI) replaces hand-rolled base64 decoder; retry wrapper
  handles 429s with exponential backoff.
- fetch_all_feeds() now collects all items across all feeds first, then sorts
  globally by parsed pubDate descending before applying age cutoff and cap.
  This ensures Gemini always sees the newest articles regardless of feed order.
- Switched classifier from Mistral to Gemini 2.5 Flash; prompt rewritten for
  higher recall (defaults to inclusion on ambiguity).
- Switched from deprecated google.generativeai to google.genai (google-genai).
- _set_gha_output(): writes has_signal=true/false to $GITHUB_OUTPUT so the
  Actions workflow can skip the commit/push step when AI selects 0 articles.
"""

import feedparser
from googlenewsdecoder import new_decoderv1 as _gnews_decoderv1
from google import genai
import html as _html_mod
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
import requests

try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# -- FEEDS ---------------------------------------------------------------------

FEED_URLS = [
    # 1. World's Leading Organizations
    "https://news.google.com/rss/search?q=%22United+Nations%22+OR+NATO+OR+IMF+OR+%22World+Bank%22+OR+G20+OR+G7+OR+WTO+OR+OPEC+OR+WHO&hl=en-US&gl=US&ceid=US:en",

    # 2. Biggest News (Top Global Headlines)
    "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en",

    # 3. World Scale Macroeconomy
    "https://news.google.com/rss/search?q=global+economy+OR+macroeconomics+OR+inflation+OR+recession+OR+%22central+bank%22+OR+%22interest+rates%22+OR+%22debt+crisis%22+OR+%22fiscal+policy%22+OR+%22trade+war%22&hl=en-US&gl=US&ceid=US:en",

    # 4. Geopolitical Analysis & Events
    "https://news.google.com/rss/search?q=geopolitics+OR+geopolitical+OR+%22foreign+policy%22+OR+%22international+relations%22+OR+%22strategic+rivalry%22+OR+%22sphere+of+influence%22+OR+diplomacy&hl=en-US&gl=US&ceid=US:en",

    # 5. Groundbreaking Tech (non-mainstream)
    "https://news.google.com/rss/search?q=%22quantum+computing%22+OR+%22nuclear+fusion%22+OR+CRISPR+OR+%22synthetic+biology%22+OR+AGI+OR+%22brain+computer+interface%22+OR+neuromorphic+OR+%22gene+editing%22+OR+%22dark+matter%22+OR+%22fusion+energy%22&hl=en-US&gl=US&ceid=US:en",

    # 6. Intelligence News
    "https://news.google.com/rss/search?q=espionage+OR+%22intelligence+agency%22+OR+CIA+OR+MI6+OR+Mossad+OR+FSB+OR+cyberespionage+OR+%22signals+intelligence%22+OR+%22covert+operation%22+OR+SIGINT&hl=en-US&gl=US&ceid=US:en",

    # 7. Military & Conflict
    "https://news.google.com/rss/search?q=%22military+operation%22+OR+%22armed+conflict%22+OR+%22defense+contract%22+OR+%22weapons+system%22+OR+%22troop+deployment%22+OR+%22air+strike%22+OR+%22naval+exercise%22+OR+warfare+OR+%22theater+command%22&hl=en-US&gl=US&ceid=US:en",

    # 8. Energy & Critical Resources
    "https://news.google.com/rss/search?q=%22rare+earth%22+OR+%22critical+minerals%22+OR+%22energy+security%22+OR+%22oil+supply%22+OR+%22LNG%22+OR+%22pipeline%22+OR+%22food+security%22+OR+%22water+crisis%22+OR+%22resource+competition%22+OR+%22commodity+shock%22&hl=en-US&gl=US&ceid=US:en",

    # 9. Cyber & Information Warfare
    "https://news.google.com/rss/search?q=%22cyberattack%22+OR+%22ransomware%22+OR+%22critical+infrastructure+attack%22+OR+%22disinformation+campaign%22+OR+%22information+warfare%22+OR+%22election+interference%22+OR+%22state+sponsored+hacking%22+OR+%22zero+day%22&hl=en-US&gl=US&ceid=US:en",

    # 10. Global South & Emerging Markets
    "https://news.google.com/rss/search?q=%22Global+South%22+OR+%22emerging+markets%22+OR+ASEAN+OR+%22African+Union%22+OR+%22Belt+and+Road%22+OR+BRICS+OR+%22Latin+America+politics%22+OR+%22Southeast+Asia%22+OR+%22Sub-Saharan+Africa%22&hl=en-US&gl=US&ceid=US:en",

    # 11. Space (Strategic & Military)
    "https://news.google.com/rss/search?q=%22space+warfare%22+OR+%22satellite+weapon%22+OR+%22anti+satellite%22+OR+%22space+force%22+OR+%22commercial+space%22+OR+%22lunar+race%22+OR+%22space+debris%22+OR+%22space+domain%22&hl=en-US&gl=US&ceid=US:en",

    # 12. Biosecurity
    "https://news.google.com/rss/search?q=biosecurity+OR+bioweapon+OR+%22pandemic+preparedness%22+OR+%22gain+of+function%22+OR+%22lab+leak%22+OR+pathogen+OR+%22biological+threat%22+OR+%22WHO+emergency%22+OR+%22epidemic+outbreak%22&hl=en-US&gl=US&ceid=US:en",

    # 13. Climate & Environment
    "https://news.google.com/rss/search?q=%22climate+crisis%22+OR+%22extreme+weather%22+OR+%22sea+level+rise%22+OR+%22arctic+ice%22+OR+%22carbon+emissions%22+OR+%22COP%22+OR+%22climate+tipping+point%22+OR+%22environmental+collapse%22+OR+%22deforestation%22+OR+%22glacier+retreat%22&hl=en-US&gl=US&ceid=US:en",
]

# Google News feed URL prefix — used to identify which links need decoding
_GNEWS_PREFIXES = (
    "https://news.google.com/rss/articles/",
    "https://news.google.com/read/",
)

KL_API_FEEDS = set()

# -- CONFIG --------------------------------------------------------------------

GEMINI_MODEL          = "gemini-3.6-flash"

SEEN_FILE             = "seen.json"
SELECTED_FILE         = "econ_selected_articles.json"
OUTPUT_XML            = "econ_feed.xml"
EXCLUDED_XML          = "econ_ex.xml"
STATS_FILE            = "econ_stats.json"
MAX_ARTICLES_PER_FEED = 100
MAX_AGE_HOURS         = 25
ALLOW_MISSING_DATES   = True
ALLOW_OLDER           = False
MAX_FEED_ITEMS        = 500

# -- PROMPT --------------------------------------------------------------------

PROMPT = """ROLE: News classifier for a global intelligence feed covering 13 domains. Input is a numbered list of article titles. Output must be valid JSON only — no markdown, no explanation.

TASK: Return the 0-based indices of every SIGNAL article.

SIGNAL — include if the article reports a genuinely significant development in ANY of these domains:

1. WORLD ORGANIZATIONS — UN, NATO, IMF, World Bank, G7/G20, WTO, OPEC, WHO: major decisions, resolutions, sanctions, leadership changes, structural shifts, funding actions
2. BREAKING GLOBAL NEWS — events with clear world-scale consequence: wars declared, governments fallen, catastrophic disasters, assassination of major figures, historic agreements
3. MACROECONOMY — global inflation trajectory, recession signals, central bank pivots, sovereign debt crises, major currency shocks, trade war escalations, BIS/IMF/World Bank outlooks
4. GEOPOLITICS — shifts in alliance architecture, territorial disputes, major diplomatic ruptures or breakthroughs, great-power competition moves, strategic doctrine changes
5. GROUNDBREAKING TECH — verified breakthroughs in quantum, fusion, CRISPR, AGI, BCI, neuromorphic computing, synthetic biology — peer-reviewed, institutional, or credible first-reports only; not product launches or incremental updates
6. INTELLIGENCE — confirmed espionage operations, intelligence agency actions, SIGINT/HUMINT revelations, covert programs exposed, spy scandals with state-level actors
7. MILITARY & CONFLICT — active combat operations, verified troop movements, defense treaty activations, major weapons system deployments, strategic strikes, theater-level command changes
8. ENERGY & CRITICAL RESOURCES — oil/gas supply disruptions, rare earth export controls, pipeline sabotage or deals, food/water security crises at national or regional scale, commodity shocks with macroeconomic spillover
9. CYBER & INFORMATION WARFARE — confirmed state-sponsored cyberattacks on critical infrastructure, major zero-day exploitation in the wild, large-scale disinformation ops with attribution, election interference with evidence
10. GLOBAL SOUTH & EMERGING MARKETS — political realignment, debt restructuring, BRI developments, BRICS expansion moves, ASEAN/AU/CELAC significant decisions, democratic backsliding or breakthroughs
11. SPACE — anti-satellite weapons tests, military space doctrine, strategic satellite launches, major commercial space milestones (lunar landing, crewed mission), debris events threatening infrastructure
12. BIOSECURITY — outbreak with pandemic potential, gain-of-function research news, biosafety-level incidents, bioweapon allegations with evidence, WHO emergency declarations, pathogen containment failures
13. CLIMATE & ENVIRONMENT — tipping point science, extreme weather events causing major humanitarian or economic impact, COP agreements or failures, Arctic/Antarctic threshold crossings, deforestation at nation-state scale

NOISE — exclude if the article is:
- A keyword false positive: "pipeline" (software), "fusion" (music/food), "intelligence" (business analytics), "space" (office/retail), "zero day" (sales event)
- Celebrity, sports, lifestyle, entertainment with no strategic angle
- A minor company's routine business dressed in strategic language
- Speculative opinion with no factual news peg
- A rehash or wire-service reprint of a story already clearly covered (only exclude if obviously redundant, not merely similar)
- Purely local/national news with no discernible world-scale consequence

DEFAULT RULE: When in doubt, classify as SIGNAL. These feeds are pre-filtered for global significance — err heavily toward inclusion. Only exclude what is clearly noise or a keyword accident.

OUTPUT FORMAT — respond with ONLY this JSON object, nothing else:
{"signal": [list of 0-based indices]}

Article titles:
{titles}"""

# -- CONSTANTS -----------------------------------------------------------------

MEDIA_NS = "http://search.yahoo.com/mrss/"
MEDIA_TAG = "{%s}" % MEDIA_NS
ET.register_namespace("media", MEDIA_NS)

BD_TZ = timezone(timedelta(hours=6))

STATS = {
    "per_feed":            {},
    "per_method":          {"KL": 0, "DIRECT": 0},
    "total_fetched":       0,
    "total_passed_age":    0,
    "total_new":           0,
    "total_signal_gemini": 0,
    "total_signal":        0,
    "timestamp":            None,
}

# -- GITHUB ACTIONS OUTPUT -----------------------------------------------------

def _set_gha_output(name: str, value: str) -> None:
    """
    Write name=value to $GITHUB_OUTPUT when running inside GitHub Actions.
    No-op in local environments where the variable is absent.
    """
    output_file = os.environ.get("GITHUB_OUTPUT")
    if not output_file:
        return
    with open(output_file, "a", encoding="utf-8") as fh:
        fh.write(f"{name}={value}\n")

# -- GOOGLE NEWS URL DECODING --------------------------------------------------

def is_google_news_url(url: str) -> bool:
    return any(url.startswith(p) for p in _GNEWS_PREFIXES)


def decode_google_news_url(gnews_url: str, _retries: int = 3) -> str:
    """
    Decode a Google News redirect URL to the real article URL using the
    `googlenewsdecoder` PyPI package (new_decoderv1).

    Retries up to _retries times with exponential backoff on 429 / failure.
    Returns the original gnews_url if all attempts fail so no article is lost.
    """
    if not gnews_url or not is_google_news_url(gnews_url):
        return gnews_url

    delay = 1.0
    for attempt in range(_retries):
        try:
            result = _gnews_decoderv1(gnews_url, interval=None)
            if result.get("status") and result.get("decoded_url"):
                decoded = result["decoded_url"]
                if decoded.startswith("http"):
                    return decoded
            if attempt < _retries - 1:
                time.sleep(delay)
                delay *= 2
        except Exception as e:
            if attempt < _retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                print(f"[WARN] gnews decode failed for {gnews_url}: {e}")

    return gnews_url

# -- XML SANITIZATION ----------------------------------------------------------

_CTRL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')


def _sanitize_xml_bytes(raw: str) -> str:
    raw = _CTRL_RE.sub("", raw)
    raw = re.sub(
        r'&(?!(?:[a-zA-Z][a-zA-Z0-9]*|#[0-9]+|#x[0-9a-fA-F]+);)',
        '&amp;',
        raw,
    )
    return raw


def _safe_text(value: str) -> str:
    if not value:
        return value
    return _html_mod.escape(value, quote=False)

# -- I/O -----------------------------------------------------------------------

def load_seen_links():
    if Path(SEEN_FILE).exists():
        try:
            with open(SEEN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return set(data.get("links", []))
        except Exception:
            pass
    return set()


def save_seen_links(seen_links):
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"links": sorted(seen_links)}, f, indent=2, ensure_ascii=False)


def save_selected_articles(articles):
    existing = []
    if Path(SELECTED_FILE).exists():
        try:
            with open(SELECTED_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing_links = {a.get("link") for a in existing}
    merged = existing + [a for a in articles if a.get("link") not in existing_links]
    with open(SELECTED_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)


def save_stats():
    STATS["timestamp"] = datetime.utcnow().isoformat()
    existing = {}
    if Path(STATS_FILE).exists():
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass
    existing.update(STATS)
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

# -- UTILITIES -----------------------------------------------------------------

def normalize_link(link, base=None):
    if not link:
        return ""
    link = link.strip()
    if link.startswith("//"):
        link = "https:" + link
    if base and not urlparse(link).netloc:
        link = urljoin(base, link)
    link = re.sub(r"([?&])utm_[^=]+=[^&]+", r"\1", link)
    link = re.sub(r"([?&])fbclid=[^&]+",    r"\1", link)
    link = re.sub(r"[?&]$", "", link)
    return link.split("#")[0]


def parse_date(entry):
    for key in ("published_parsed", "updated_parsed", "created_parsed", "issued_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime.fromtimestamp(time.mktime(st), tz=timezone.utc), False
            except Exception:
                pass
    for key in ("published", "updated", "created", "dc_date", "issued"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            try:
                dt = parsedate_to_datetime(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc), False
            except Exception:
                pass
            if dateutil_parser:
                try:
                    dt = dateutil_parser.parse(val)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.astimezone(timezone.utc), False
                except Exception:
                    pass
    if ALLOW_MISSING_DATES:
        return datetime.now(timezone.utc), True
    return None, False


IMG_SRC_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)


def find_image_in_html(html_text, base=None):
    if not html_text:
        return None
    m = IMG_SRC_RE.search(html_text)
    if not m:
        return None
    return normalize_link(m.group(1).strip(), base=base)


def get_mime_for_url(url):
    if not url:
        return "image/jpeg"
    path = urlparse(url).path.lower()
    if path.endswith(".png"):  return "image/png"
    if path.endswith(".gif"):  return "image/gif"
    if path.endswith(".webp"): return "image/webp"
    if path.endswith(".svg"):  return "image/svg+xml"
    return "image/jpeg"


def extract_image_url(entry, base_link=None):
    mt = entry.get("media_thumbnail")
    if mt:
        if isinstance(mt, list) and mt[0].get("url"):
            return normalize_link(mt[0]["url"], base=base_link)
        if isinstance(mt, dict) and mt.get("url"):
            return normalize_link(mt["url"], base=base_link)

    mc = entry.get("media_content")
    if mc:
        if isinstance(mc, list) and mc[0].get("url"):
            return normalize_link(mc[0]["url"], base=base_link)
        if isinstance(mc, dict) and mc.get("url"):
            return normalize_link(mc["url"], base=base_link)

    enc = entry.get("enclosures")
    if enc and isinstance(enc, list):
        for e in enc:
            href = e.get("href") or e.get("url") or e.get("link")
            typ  = e.get("type", "")
            if href and (typ.startswith("image/") or re.search(r'\.(jpg|jpeg|png|gif|webp|svg)$', href, re.I)):
                return normalize_link(href, base=base_link)

    links = entry.get("links")
    if links and isinstance(links, list):
        for lnk in links:
            if lnk.get("rel") == "enclosure":
                href = lnk.get("href")
                if href:
                    return normalize_link(href, base=base_link)

    content = entry.get("content")
    if content:
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("value"):
                    found = find_image_in_html(c.get("value"), base=base_link)
                    if found:
                        return found
        elif isinstance(content, str):
            found = find_image_in_html(content, base=base_link)
            if found:
                return found

    for key in ("summary", "description", "summary_detail", "description_detail"):
        val = entry.get(key)
        if isinstance(val, dict):
            val = val.get("value")
        if isinstance(val, str) and val:
            found = find_image_in_html(val, base=base_link)
            if found:
                return found
    return None

# -- FETCHING ------------------------------------------------------------------

def fetch_via_kl(kl_endpoint, target_feed_url, timeout=20):
    if not kl_endpoint:
        return None
    headers = {"Content-Type": "application/json", "Accept": "application/xml, text/xml, */*"}
    payload = {"url": target_feed_url}
    try:
        resp = requests.post(kl_endpoint, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    try:
        resp = requests.get(kl_endpoint, params={"url": target_feed_url}, headers=headers, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return feedparser.parse(resp.text)
    except Exception:
        pass
    return None


def fetch_feed(url):
    url_norm    = url.strip()
    method_used = "DIRECT"

    if url_norm in KL_API_FEEDS:
        kl_endpoint = os.environ.get("KL")
        feed        = None
        if kl_endpoint:
            feed = fetch_via_kl(kl_endpoint, url_norm)
            if feed:
                method_used = "KL"
        if not feed:
            feed = feedparser.parse(url_norm)
    else:
        feed = feedparser.parse(url_norm)

    entries_count = len(getattr(feed, "entries", []))
    STATS["per_feed"].setdefault(url_norm, {"fetched": 0, "passed_age": 0, "capped": 0})
    STATS["per_feed"][url_norm]["fetched"] += entries_count
    STATS["per_method"].setdefault(method_used, 0)
    STATS["per_method"][method_used] += entries_count
    STATS["total_fetched"]            += entries_count

    return feed


def fetch_all_feeds():
    now        = datetime.now(timezone.utc)
    cutoff     = now - timedelta(hours=MAX_AGE_HOURS)
    bd_now     = datetime.now(BD_TZ)
    bd_now_str = bd_now.strftime("%a, %d %b %Y %H:%M:%S +0600")

    raw_items = []

    for url in FEED_URLS:
        feed       = fetch_feed(url)
        feed_items = []

        for e in feed.entries:
            dt, inferred = parse_date(e)
            if not dt:
                continue
            if (not ALLOW_OLDER) and dt < cutoff:
                continue

            desc = ""
            if e.get("summary"):
                desc = e.get("summary")
            elif e.get("description"):
                desc = e.get("description")
            elif e.get("content") and isinstance(e.get("content"), list):
                desc = "\n".join([c.get("value", "") for c in e.get("content") if isinstance(c, dict)])
            else:
                det = e.get("summary_detail") or e.get("description_detail")
                if isinstance(det, dict):
                    desc = det.get("value", "") or ""

            raw_link = normalize_link(e.get("link") or "")

            if is_google_news_url(raw_link):
                real_link = decode_google_news_url(raw_link)
            else:
                real_link = raw_link

            article_id = e.get("id") or real_link or raw_link or ""
            image_url  = extract_image_url(e, base_link=real_link)

            article = {
                "id":          str(article_id),
                "title":       e.get("title", "") or "",
                "link":        real_link,
                "description": desc or "",
                "published":   bd_now_str,
                "source":      url,
                "_dt":         dt,
            }
            if inferred:
                article["published_inferred"] = True
            if image_url:
                article["thumbnail"]      = image_url
                article["thumbnail_type"] = get_mime_for_url(image_url)

            feed_items.append(article)

        passed = len(feed_items)
        STATS["per_feed"][url]["passed_age"] = passed
        STATS["total_passed_age"]           += passed
        raw_items.extend(feed_items)

    raw_items.sort(key=lambda a: a["_dt"], reverse=True)

    pre_gemini_cap = MAX_ARTICLES_PER_FEED * len(FEED_URLS)
    all_articles   = raw_items[:pre_gemini_cap]

    included_sources: dict[str, int] = {}
    for a in all_articles:
        included_sources[a["source"]] = included_sources.get(a["source"], 0) + 1
    for url in FEED_URLS:
        STATS["per_feed"][url]["capped"] = included_sources.get(url, 0)

    return all_articles


def get_new_articles(all_articles, seen_links):
    new = []
    for a in all_articles:
        link = a.get("link")
        if link and link not in seen_links:
            new.append(a)
    return new


def dedup_by_link(articles):
    seen    = set()
    deduped = []
    for a in articles:
        link = a.get("link", "")
        if link and link not in seen:
            seen.add(link)
            deduped.append(a)
        elif not link:
            deduped.append(a)
    dropped = len(articles) - len(deduped)
    if dropped:
        print(f"Link dedup: removed {dropped} duplicate link(s).")
    return deduped

# -- CLASSIFICATION ------------------------------------------------------------

def extract_signal_indices(text):
    text = text.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group(0))
            if isinstance(obj, dict):
                return [i for i in obj.get("signal", []) if isinstance(i, int)]
        except Exception:
            pass
    m = re.search(r'"signal"\s*:\s*(\[.*?\])', text, flags=re.DOTALL)
    if m:
        try:
            return [i for i in json.loads(m.group(1)) if isinstance(i, int)]
        except Exception:
            pass
    return []


def send_to_gemini(articles):
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key or not articles:
        return []

    try:
        client = genai.Client(api_key=api_key)

        titles_text = "\n".join([f"{i}. {a.get('title', '')}" for i, a in enumerate(articles)])
        prompt = PROMPT.replace("{titles}", titles_text)

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                response_mime_type="application/json",
            ),
        )

        text = response.text or ""
        return extract_signal_indices(text)

    except Exception as e:
        print(f"Gemini classification error: {e}")
        return []

# -- XML -----------------------------------------------------------------------

def _fresh_channel(root, feed_title, feed_description):
    channel = ET.SubElement(root, "channel")
    ET.SubElement(channel, "title").text       = feed_title
    ET.SubElement(channel, "link").text        = "https://evilgodfahim.github.io/"
    ET.SubElement(channel, "description").text = feed_description
    return channel


def _load_or_create(output_file, feed_title, feed_description):
    ET.register_namespace("media", MEDIA_NS)

    if Path(output_file).exists():
        try:
            tree    = ET.parse(output_file)
            root    = tree.getroot()
            channel = root.find("channel")
            if channel is not None:
                return tree, root, channel
            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel

        except ET.ParseError as e:
            print(f"[WARN] XML parse failed on first attempt ({output_file}): {e}")
            print("[INFO] Retrying with sanitized content…")

        try:
            with open(output_file, "r", encoding="utf-8", errors="replace") as fh:
                raw = fh.read()

            clean   = _sanitize_xml_bytes(raw)
            root    = ET.fromstring(clean)
            tree    = ET.ElementTree(root)
            channel = root.find("channel")

            if channel is not None:
                recovered = len(channel.findall("item"))
                print(f"[INFO] Sanitization succeeded — recovered {recovered} existing item(s).")
                return tree, root, channel

            channel = _fresh_channel(root, feed_title, feed_description)
            return tree, root, channel

        except ET.ParseError as e:
            print(f"[WARN] XML still unparseable after sanitization ({output_file}): {e}")
            print("[WARN] Starting a fresh feed — existing items in this file cannot be recovered.")

    root    = ET.Element("rss", {"version": "2.0"})
    tree    = ET.ElementTree(root)
    channel = _fresh_channel(root, feed_title, feed_description)
    return tree, root, channel


def generate_xml_feed(articles, output_file, feed_title=None, feed_description=None):
    feed_title       = feed_title       or "BD Economics & Finance"
    feed_description = feed_description or "AI-curated Bangladesh economics and finance news"

    tree, root, channel = _load_or_create(output_file, feed_title, feed_description)

    existing_links: set[str] = set()
    for item in channel.findall("item"):
        link_el = item.find("link")
        if link_el is not None and link_el.text:
            existing_links.add(link_el.text.strip())

    added = 0
    for a in articles:
        link = (a.get("link") or "").strip()
        if not link or link in existing_links:
            continue

        item = ET.SubElement(channel, "item")

        ET.SubElement(item, "title").text       = a.get("title", "") or ""
        ET.SubElement(item, "description").text = a.get("description", "") or ""

        ET.SubElement(item, "link").text = _safe_text(link)

        guid_val     = a.get("id") or link
        is_permalink = "true" if guid_val.startswith("http") else "false"
        ET.SubElement(item, "guid", {"isPermaLink": is_permalink}).text = _safe_text(guid_val)

        if a.get("published"):
            ET.SubElement(item, "pubDate").text = a["published"]

        thumb = a.get("thumbnail")
        if thumb:
            ET.SubElement(item, MEDIA_TAG + "thumbnail", {"url": thumb})
            mime = a.get("thumbnail_type") or get_mime_for_url(thumb)
            ET.SubElement(item, "enclosure", {"url": thumb, "type": mime, "length": "0"})

        existing_links.add(link)
        added += 1

    all_items = channel.findall("item")
    overflow  = len(all_items) - MAX_FEED_ITEMS
    if overflow > 0:
        for old_item in all_items[:overflow]:
            channel.remove(old_item)

    now_text   = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
    last_build = channel.find("lastBuildDate")
    if last_build is None:
        ET.SubElement(channel, "lastBuildDate").text = now_text
    else:
        last_build.text = now_text

    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(output_file, encoding="unicode", xml_declaration=False)
    with open(output_file, "r+", encoding="utf-8") as fh:
        body = fh.read()
        fh.seek(0)
        fh.write('<?xml version="1.0" encoding="UTF-8"?>\n' + body)
        fh.truncate()

    print(f"  → {added} new item(s) written to {output_file}  "
          f"(total in feed: {len(channel.findall('item'))})")
    return added

# -- STATS ---------------------------------------------------------------------

def print_stats():
    print("\nFetch statistics:")
    print(f"  Timestamp:       {STATS.get('timestamp')}")
    print(f"  Total fetched:   {STATS['total_fetched']}")
    print(f"  Passed age cut:  {STATS['total_passed_age']}  (within {MAX_AGE_HOURS}h)")
    print(f"  New (unseen):    {STATS['total_new']}")
    print(f"  Signal (Gemini): {STATS['total_signal_gemini']}  -> {OUTPUT_XML}")
    print("  Per-method:")
    for method, cnt in STATS["per_method"].items():
        print(f"    {method}: {cnt}")
    print("  Per-feed:")
    for feed, d in STATS["per_feed"].items():
        print(f"    {feed}")
        print(f"      fetched={d.get('fetched',0)}  passed_age={d.get('passed_age',0)}  capped={d.get('capped',0)}")
    print("")

# -- MAIN ----------------------------------------------------------------------

def main():
    seen_links   = load_seen_links()
    all_articles = fetch_all_feeds()
    new_articles = get_new_articles(all_articles, seen_links)

    new_articles = dedup_by_link(new_articles)

    for a in new_articles:
        a.pop("_dt", None)

    STATS["total_new"] = len(new_articles)
    print(f"Sending {len(new_articles)} article(s) to Gemini for BD economics/finance filtering…")

    gemini_indices = send_to_gemini(new_articles)
    gemini_indices = [i for i in gemini_indices if 0 <= i < len(new_articles)]

    STATS["total_signal_gemini"] = len(gemini_indices)
    STATS["total_signal"]        = len(gemini_indices)

    for a in new_articles:
        link = a.get("link")
        if link:
            seen_links.add(link)
    save_seen_links(seen_links)

    if not gemini_indices:
        print("Gemini returned no signal indices. Skipping XML writes.")
        _set_gha_output("has_signal", "false")
        print_stats()
        return

    signal_articles   = [new_articles[i] for i in gemini_indices]
    excluded_articles = [
        new_articles[i]
        for i in range(len(new_articles))
        if i not in set(gemini_indices)
    ]

    generate_xml_feed(
        signal_articles,
        output_file=OUTPUT_XML,
        feed_title="BD Economics & Finance",
        feed_description="AI-curated Bangladesh national economics and finance news",
    )

    generate_xml_feed(
        excluded_articles,
        output_file=EXCLUDED_XML,
        feed_title="Excluded (BD Economics Filter)",
        feed_description="Articles excluded by BD economics and finance filter",
    )

    save_selected_articles(signal_articles)

    _set_gha_output("has_signal", "true")

    STATS["timestamp"] = datetime.utcnow().isoformat()
    save_stats()
    print_stats()


if __name__ == "__main__":
    main()
