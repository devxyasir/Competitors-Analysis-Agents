"""
brave_search.py  —  Credit-efficient Brave Search client
=========================================================
Reads the official docs so you don't burn credits:

  Rate limits  : 1 req/second (sliding window), 429 on breach
  Free tier    : $5 credit/month  ≈  1,000 free searches
  Billing rule : only SUCCESSFUL responses are counted
  Max results  : 20 per call  (use count=20 to get most out of 1 credit)
  Pagination   : check more_results_available before spending another credit

All credit-safety knobs live in the CONFIG block below.
Change them there — nothing else needs touching.
"""

import asyncio
import json
import time
import httpx
from urllib.parse import urlparse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG  ←  edit here, nothing else needs changing
# ──────────────────────────────────────────────────────────────────────────────

API_KEY = "BSAsEzeWt5k-kQ-ZO_opXn6k5QNz5vQ"   # paste your key

# Credit guards
MONTHLY_CREDIT_LIMIT  = 1000   # stop when this many calls used this month
                                # free tier = 1 000 calls, paid = however many you want
MONTHLY_WARN_THRESHOLD = 800   # print a warning when you hit this
PER_RUN_CALL_LIMIT     = 30    # hard cap per single script run (safety net)

# Per-call settings  (count=20 = max results per credit = most efficient)
RESULTS_PER_CALL = 20          # max allowed by API; costs 1 credit regardless
REQUEST_TIMEOUT  = 30          # seconds before we give up (no credit charged)

# Rate-limit behaviour
RATE_LIMIT_RETRY_MAX    = 3    # how many times to retry a 429
RATE_LIMIT_BASE_WAIT    = 1.1  # seconds to wait after 429 (slightly > 1s window)

# Cache  (avoids re-spending credits on identical queries in the same run)
ENABLE_CACHE = True            # set False to always hit the API
CACHE_FILE   = "brave_cache.json"

# Competitor list — edit this
COMPETITORS = [
    {"name": "Microsoft Azure", "domain": "azure.microsoft.com"},
    {"name": "AWS",             "domain": "aws.amazon.com"},
    {"name": "Digital Ocean",   "domain": "digitalocean.com"},
    # add more:
    # {"name": "Vultr",         "domain": "vultr.com"},
    # {"name": "Linode",        "domain": "linode.com"},
]

# ──────────────────────────────────────────────────────────────────────────────
# INTERNAL — no need to edit below this line
# ──────────────────────────────────────────────────────────────────────────────

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
SourceType = Literal["directory", "comparison", "review", "alternative", "media", "unknown"]


@dataclass
class RateLimitState:
    """Tracks headers from the last response so we never overshoot."""
    per_second_remaining: int = 1
    monthly_remaining:    int = MONTHLY_CREDIT_LIMIT
    reset_in_seconds:     float = 1.0
    calls_this_run:       int = 0
    _last_call_ts:        float = field(default_factory=time.time, repr=False)

    def update_from_headers(self, headers: dict):
        remaining = headers.get("X-RateLimit-Remaining", "")
        reset     = headers.get("X-RateLimit-Reset", "")
        if remaining:
            parts = [p.strip() for p in remaining.split(",")]
            if len(parts) >= 1:
                try:
                    self.per_second_remaining = int(parts[0])
                except ValueError:
                    pass
            if len(parts) >= 2:
                try:
                    self.monthly_remaining = int(parts[1])
                except ValueError:
                    pass
        if reset:
            parts = [p.strip() for p in reset.split(",")]
            if parts:
                try:
                    self.reset_in_seconds = float(parts[0])
                except ValueError:
                    pass

    def should_stop(self) -> tuple[bool, str]:
        if self.calls_this_run >= PER_RUN_CALL_LIMIT:
            return True, f"per-run call cap reached ({PER_RUN_CALL_LIMIT})"
        if self.monthly_remaining <= 0:
            return True, "monthly credit limit exhausted"
        if self.monthly_remaining <= (MONTHLY_CREDIT_LIMIT - MONTHLY_WARN_THRESHOLD):
            print(f"  [WARN] Only {self.monthly_remaining} monthly credits left!")
        return False, ""

    async def throttle(self):
        """Enforce 1 req/second using both the header data and a local timer."""
        elapsed = time.time() - self._last_call_ts
        if elapsed < 1.05:                   # 1.05s gives a small buffer
            await asyncio.sleep(1.05 - elapsed)
        self._last_call_ts = time.time()


# Single shared state instance for the whole run
_state = RateLimitState()


# ──────────────────────────────────────────────────────────────────────────────
# Simple disk cache  (avoids charging the same query twice in one session)
# ──────────────────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if ENABLE_CACHE and Path(CACHE_FILE).exists():
        try:
            return json.loads(Path(CACHE_FILE).read_text())
        except Exception:
            pass
    return {}

def _save_cache(cache: dict):
    if ENABLE_CACHE:
        Path(CACHE_FILE).write_text(json.dumps(cache, indent=2))

_cache: dict = _load_cache()


# ──────────────────────────────────────────────────────────────────────────────
# Core search call  (1 credit per call regardless of count)
# ──────────────────────────────────────────────────────────────────────────────

async def brave_search(
    query: str,
    count: int = RESULTS_PER_CALL,
    freshness: str | None = None,         # "pd"=24h  "pw"=week  "pm"=month
    extra_snippets: bool = False,         # 5 extra snippets per result, same credit
) -> list[dict]:
    """
    One Brave API call = one credit.
    Returns up to `count` results (max 20).
    Returns [] on error — no credit charged for failed calls.
    """
    # Cache check — free
    cache_key = f"{query}|{count}|{freshness}|{extra_snippets}"
    if ENABLE_CACHE and cache_key in _cache:
        print(f"  [CACHE] {query[:60]}")
        return _cache[cache_key]

    # Credit guards
    stop, reason = _state.should_stop()
    if stop:
        print(f"  [STOP] {reason}")
        return []

    # Enforce 1 req/second
    await _state.throttle()

    params: dict = {
        "q":            query,
        "count":        min(count, 20),   # API hard max is 20
        "safesearch":   "off",
    }
    if freshness:
        params["freshness"] = freshness
    if extra_snippets:
        params["extra_snippets"] = "true"

    headers = {
        "Accept":               "application/json",
        "Accept-Encoding":      "gzip",
        "X-Subscription-Token": API_KEY,
    }

    for attempt in range(RATE_LIMIT_RETRY_MAX + 1):
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(BRAVE_ENDPOINT, headers=headers, params=params)

            # Always update our rate-limit picture from headers
            _state.update_from_headers(dict(resp.headers))

            if resp.status_code == 429:
                # Not billed — but we must wait
                wait = (RATE_LIMIT_BASE_WAIT * (2 ** attempt))
                print(f"  [429] Rate limited. Waiting {wait:.1f}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
                continue

            if resp.status_code != 200:
                # Non-200 errors are NOT billed
                print(f"  [ERR] HTTP {resp.status_code} for: {query[:50]}")
                return []

            # Success — count the credit
            _state.calls_this_run += 1
            data    = resp.json()
            results = data.get("web", {}).get("results", [])

            # Warn if no more pages exist (saves caller from trying offset)
            more = data.get("query", {}).get("more_results_available", False)
            if not more and len(results) == count:
                pass  # all results fit in one call — ideal

            print(
                f"  [OK]  {query[:55]:<55} "
                f"→ {len(results)} results  "
                f"| monthly left: {_state.monthly_remaining}"
            )

            if ENABLE_CACHE:
                _cache[cache_key] = results
                _save_cache(_cache)

            return results

        except httpx.TimeoutException:
            # Timeout = no credit charged
            print(f"  [TIMEOUT] {query[:50]}")
            return []
        except Exception as e:
            print(f"  [ERR] {e}")
            return []

    return []


# ──────────────────────────────────────────────────────────────────────────────
# URL classifier
# ──────────────────────────────────────────────────────────────────────────────

DIRECTORY_DOMAINS   = {"g2.com","capterra.com","clutch.co","getapp.com","softwareadvice.com","gartner.com"}
REVIEW_DOMAINS      = {"trustpilot.com","sitejabber.com","reddit.com","producthunt.com","g2crowd.com"}
ALTERNATIVE_DOMAINS = {"alternativeto.net","slant.co","saashub.com","stackshare.io"}
MEDIA_DOMAINS       = {"techcrunch.com","forbes.com","wired.com","venturebeat.com","zdnet.com","theregister.com"}
COMPARISON_KW       = {" vs ", "versus", "alternative", "compare", "comparison", "best "}

def classify_url(url: str, snippet: str = "") -> SourceType:
    domain = urlparse(url).netloc.replace("www.", "")
    if domain in DIRECTORY_DOMAINS:   return "directory"
    if domain in REVIEW_DOMAINS:      return "review"
    if domain in ALTERNATIVE_DOMAINS: return "alternative"
    if domain in MEDIA_DOMAINS:       return "media"
    combined = (url + " " + snippet).lower()
    if any(kw in combined for kw in COMPARISON_KW):
        return "comparison"
    return "unknown"


# ──────────────────────────────────────────────────────────────────────────────
# Query builder  —  5 categories, 1 call each = 5 credits per competitor
# We use count=20 every time so each credit gives maximum return
# ──────────────────────────────────────────────────────────────────────────────

def source_queries(name: str, domain: str) -> dict[str, str]:
    """Returns one search query per source category."""
    return {
        "directory":   f'"{name}" site:g2.com OR site:capterra.com OR site:clutch.co',
        "comparison":  f'"{name}" vs OR alternative OR compare -site:{domain}',
        "alternative": f'"{name}" site:alternativeto.net OR site:saashub.com OR site:stackshare.io',
        "review":      f'"{name}" reviews site:trustpilot.com OR site:reddit.com OR site:producthunt.com',
        "media":       f'"{name}" press release OR funding OR launch -site:{domain}',
    }

def field_queries(name: str, domain: str) -> dict[str, str]:
    """Returns one search query per data field."""
    return {
        "pricing":  f'{name} pricing plans tiers cost site:{domain}',
        "products": f'{name} products services features overview',
        "website_notes": f'{name} website overview mission statement site:{domain}',
        "sw_stack": f'{name} technology stack built with infrastructure',
        "exposure": f'"{name}" news coverage mentions 2024 2025',
    }
    # NOTE: 4 field queries + 5 source queries = 9 credits per competitor
    # For 3 competitors = 27 credits total — well within 1,000/month free tier


# ──────────────────────────────────────────────────────────────────────────────
# Competitor analyser  —  runs field + source queries sequentially
# Sequential (not concurrent) to respect 1 req/second rate limit
# ──────────────────────────────────────────────────────────────────────────────

from models import CompetitorProfile, SourceRecord

def _snippets_to_text(results: list[dict]) -> str:
    """Joins descriptions from search results into a readable summary."""
    parts = [r.get("description", "") for r in results if r.get("description")]
    return "  |  ".join(parts[:3])   # top 3 snippets, joined

def get_credit_cost(name: str, domain: str) -> int:
    """Approximate credits spent building this profile."""
    return len(field_queries(name, domain)) + len(source_queries(name, domain))


def _snippets_to_list(results: list[dict]) -> list[str]:
    parts = [r.get("description", "") for r in results if r.get("description")]
    return parts[:3]


async def analyse_competitor(comp: dict) -> CompetitorProfile:
    name   = comp["name"]
    domain = comp["domain"]
    profile = CompetitorProfile(name=name, domain=domain)

    print(f"\n── {name} ──────────────────────────────────")

    # 1. Field queries (4 credits)
    fq = field_queries(name, domain)
    for field_name, query in fq.items():
        results = await brave_search(query)
        if results:
            if field_name in ["products", "sw_stack"]:
                setattr(profile, field_name, _snippets_to_list(results))
            else:
                setattr(profile, field_name, _snippets_to_text(results))

    # 2. Source queries (5 credits)
    seen_domains: set[str] = set()
    sq = source_queries(name, domain)
    for source_type, query in sq.items():
        results = await brave_search(query, extra_snippets=False)
        for r in results:
            url     = r.get("url", "")
            d       = urlparse(url).netloc.replace("www.", "")
            if d in seen_domains:
                continue
            seen_domains.add(d)
            snippet = r.get("description", "")
            profile.useful_sources.append(SourceRecord(
                url=url,
                domain=d,
                source_type=classify_url(url, snippet),
                snippet=snippet[:300],
                title=r.get("title", ""),
            ))

    # Sort: directories first
    TYPE_ORDER = {"directory":0,"comparison":1,"review":2,"alternative":3,"media":4,"unknown":5}
    profile.useful_sources.sort(key=lambda s: TYPE_ORDER[s.source_type])

    print(f"  → {len(profile.useful_sources)} sources found  |  ~{get_credit_cost(name, domain)} credits used")
    return profile


# ──────────────────────────────────────────────────────────────────────────────
# Output
# ──────────────────────────────────────────────────────────────────────────────

def save_json(profiles: list[CompetitorProfile], path: str = "results.json"):
    import json
    data = []
    for p in profiles:
        data.append({
            "name":    p.name,
            "domain":  p.domain,
            "pricing": p.pricing,
            "products":p.products,
            "sw_stack":p.sw_stack,
            "exposure":p.exposure,
            "useful_sources": [
                {
                    "url":   s.url,
                    "domain": s.domain,
                    "source_type": s.source_type,
                    "title": s.title,
                    "snippet": s.snippet,
                }
                for s in p.useful_sources
            ],
        })
    Path(path).write_text(json.dumps(data, indent=2))
    print(f"\n[OK] JSON saved → {path}")


def save_excel(profiles: list[CompetitorProfile], path: str = "results.xlsx"):
    try:
        import pandas as pd
    except ImportError:
        print("[SKIP] pandas not installed — skipping Excel export")
        return
    rows = []
    for p in profiles:
        src = p.sources_by_type()
        rows.append({
            "Competitor":   p.name,
            "Domain":       p.domain,
            "Pricing":      p.pricing[:400],
            "Products":     p.products[:400],
            "S/W Stack":    p.sw_stack[:400],
            "Exposure":     p.exposure[:400],
            "Directories":  "\n".join(src.get("directory", [])[:5]),
            "Comparisons":  "\n".join(src.get("comparison", [])[:5]),
            "Reviews":      "\n".join(src.get("review", [])[:5]),
            "Alternatives": "\n".join(src.get("alternative", [])[:5]),
            "Media":        "\n".join(src.get("media", [])[:5]),
        })
    pd.DataFrame(rows).to_excel(path, index=False)
    print(f"[OK] Excel saved → {path}")


def print_summary(profiles: list[CompetitorProfile]):
    total_credits = sum(p.credit_cost() for p in profiles)
    print("\n" + "="*60)
    print(f"  SUMMARY  —  {len(profiles)} competitors  |  ~{total_credits} credits used this run")
    print("="*60)
    for p in profiles:
        src = p.sources_by_type()
        print(f"\n  {p.name}")
        print(f"    Total sources : {len(p.useful_sources)}")
        for stype in ["directory","comparison","review","alternative","media","unknown"]:
            urls = src.get(stype, [])
            if urls:
                print(f"    {stype:<14}: {len(urls)}")
    print(f"\n  Credits remaining this month : {_state.monthly_remaining}")
    print(f"  Calls this run               : {_state.calls_this_run}")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

async def main():
    print("="*60)
    print("  Competitor Analysis  —  Brave Search API")
    print(f"  Competitors : {len(COMPETITORS)}")
    print(f"  Max calls   : {PER_RUN_CALL_LIMIT}  (edit PER_RUN_CALL_LIMIT)")
    print(f"  Cache       : {'ON  → ' + CACHE_FILE if ENABLE_CACHE else 'OFF'}")
    print("="*60)

    profiles = []
    for comp in COMPETITORS:
        profile = await analyse_competitor(comp)
        profiles.append(profile)

    print_summary(profiles)
    save_json(profiles)
    save_excel(profiles)


if __name__ == "__main__":
    asyncio.run(main())