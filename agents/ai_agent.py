import os
import json
import httpx
import asyncio
from bs4 import BeautifulSoup
from openai import AsyncOpenAI
from models import CompetitorProfile

import config

# Use centralized keys from config.py
LONGCAT_API_KEY = config.LONGCAT_API_KEY
FMXDNS_API_KEY = config.FMXDNS_API_KEY
FMXDNS_BASE_URL = "https://openai.fmxdns.com/v1"

def get_client(provider: str):
    if provider == "fmxdns":
        return AsyncOpenAI(api_key=FMXDNS_API_KEY, base_url=FMXDNS_BASE_URL), "minimax-reasoner"
    else:
        # Default to LongCat-Flash-Lite as requested
        return AsyncOpenAI(api_key=LONGCAT_API_KEY, base_url="https://api.longcat.chat/openai/v1"), "LongCat-Flash-Lite"

async def _fetch_page(client_session: httpx.AsyncClient, url: str) -> str:
    """Fetch and extract visible text from a URL."""
    try:
        response = await client_session.get(url, timeout=10.0, follow_redirects=True)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            # Remove scripts and styles
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()
            return soup.get_text(separator=" ", strip=True)
    except Exception as e:
        print(f"      [WARN] Failed to read {url}: {e}")
    return ""

async def deep_crawl(domain: str) -> str:
    """Visits the primary pages of a competitor to build a raw text corpus."""
    print(f"    [OK] Deep crawling {domain}...")
    urls = [
        f"https://{domain}",
        f"https://{domain}/pricing",
        f"https://{domain}/features",
        f"https://{domain}/about"
    ]
    
    corpus_parts = []
    # Fetch concurrently
    async with httpx.AsyncClient(verify=False) as session:
        tasks = [_fetch_page(session, url) for url in urls]
        results = await asyncio.gather(*tasks)
        for i, text in enumerate(results):
            if text:
                print(f"      [OK] Extracted {len(text)} chars from {urls[i]}")
                corpus_parts.append(f"--- PAGE: {urls[i]} ---\n{text}")
                
    full_text = "\n\n".join(corpus_parts)
    if not full_text.strip():
        print(f"      [WARN] No text could be crawled from {domain}. LLM may hallucinate or rely on pre-training.")
    else:
        print(f"    [OK] Total assembled context: {len(full_text)} characters.")
        
    return full_text

async def extract_profile(name: str, domain: str, raw_text: str, provider: str = "longcat") -> CompetitorProfile:
    """Uses the specified AI Provider to extract the exact schema from the raw website text."""
    client, model_name = get_client(provider)
    
    # Check for missing API key for LongCat
    if provider == "longcat" and (not LONGCAT_API_KEY or LONGCAT_API_KEY == "missing_key"):
        print("    [ERROR] LONGCAT_API_KEY is missing from environment. Using mock data.")
        return CompetitorProfile(name=name, domain=domain, pricing="Missing API Key", products=["API Key required"], website_notes="Please add LONGCAT_API_KEY to .env", sw_stack=[], exposure="")

    print(f"    [OK] Sending corpus to {provider} ({model_name}) for Semantic Extraction...")
    
    prompt = f"""
    You are an expert market intelligence analyst. 
    Analyze the following raw website text scraped from {name} ({domain}).
    
    Extract the following details and return ONLY a strict JSON object (do not include markdown wrapping like ```json).
    Use this exact schema:
    {{
        "name": "{name}",
        "domain": "{domain}",
        "pricing": "Summary of their pricing model, tiers, and base costs.",
        "products": ["List of core products or services"],
        "website_notes": "Summary of their target audience and main value proposition.",
        "sw_stack": ["Any mentioned software stack components or integrations"],
        "exposure": "Any highly visible partnerships, risks, or market focus areas"
    }}
    
    If any field is completely missing from the text, use your pre-trained knowledge to fill in the most accurate guess, but mark it with '(Estimated)' or 'Unknown'.
    
    RAW WEBSITE TEXT:
    {raw_text[:200000]}
    """

    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        content = response.choices[0].message.content.strip()
        # Clean potential markdown wrapping just in case
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
            
        data = json.loads(content)
        
        # Build Profile (Useful sources will be populated by Brave later)
        profile = CompetitorProfile(
            name=data.get("name", name),
            domain=data.get("domain", domain),
            pricing=data.get("pricing", ""),
            products=data.get("products", []),
            website_notes=data.get("website_notes", ""),
            sw_stack=data.get("sw_stack", []),
            exposure=data.get("exposure", ""),
            useful_sources=[] 
        )
        print(f"    [OK] LLM Extraction Successful for {name}.")
        return profile
        
    except Exception as e:
        print(f"    [ERROR] {provider} LLM Error: {str(e)}")
        # Return fallback
        return CompetitorProfile(name=name, domain=domain, pricing=f"Error: {e}")

async def synthesize_comparison(profiles: list[CompetitorProfile], provider: str = "longcat") -> str:
    """Takes all structured profiles and asks the specified AI Provider to synthesize a comparative insights report."""
    client, model_name = get_client(provider)
    
    if not profiles or len(profiles) < 2:
        return "Insufficient data for cross-competitor synthesis. At least 2 competitors required."
        
    if provider == "longcat" and (not LONGCAT_API_KEY or LONGCAT_API_KEY == "missing_key"):
        return "Synthesis unavailable: LONGCAT_API_KEY missing."

    print(f"\n[OK] Synthesizing Cross-Competitor Intelligence with {provider}...")
    
    # Dump profiles into a clean string format
    profiles_data = []
    for p in profiles:
        profiles_data.append(f"""
Competitor: {p.name}
Pricing: {p.pricing}
Products: {', '.join(p.products)}
Notes: {p.website_notes}
Stack: {', '.join(p.sw_stack)}
Exposure: {p.exposure}
""")
        
    prompt = f"""
You are an elite market intelligence strategist. 
Review the following extracted competitor profiles and write a concise 1-2 paragraph executive summary comparing them.
Identify specific feature parity, key pricing differences, and market positioning gaps. 

Competitor Data:
{'------'.join(profiles_data)}

Return ONLY the paragraphs of text, no markdown headers or chatty intros. Be professional, punchy, and highly analytical.
"""
    try:
        # Use specific synthesis models if needed; LongCat-Flash-Lite is fine for synthesis too
        synth_model = "minimax-reasoner" if provider == "fmxdns" else "LongCat-Flash-Lite"
        response = await client.chat.completions.create(
            model=synth_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        synthesis = response.choices[0].message.content.strip()
        print(f"[OK] Synthesis Complete via {provider}.")
        return synthesis
    except Exception as e:
        print(f"[ERROR] Synthesis failed: {e}")
        return "Error completing AI synthesis."

async def validate_competitors_via_llm(competitors: list[dict], provider: str = "longcat") -> tuple[list[dict], list[dict]]:
    """Validates the overarching industry of the competitors to filter out outliers."""
    client, model_name = get_client(provider)
    
    if (provider == "longcat" and (not LONGCAT_API_KEY or LONGCAT_API_KEY == "missing_key")) or len(competitors) == 0:
        return competitors, []

    print(f"\n[OK] Validating {len(competitors)} competitors via {provider} LLM...")
    
    comp_list_str = "\\n".join([f"- {c['name']} ({c['domain']})" for c in competitors])
    
    prompt = f"""
    You are a data validation agent parsing a user-provided list of competitors.
    The user has provided the following companies:
    {comp_list_str}
    
    Determine the dominant overarching industry/field across these companies. 
    Identify any obvious outliers that are completely unrelated to the main cluster (e.g., if there's Stripe and PayPal, but also McDonald's, McDonald's is an outlier).
    
    Return ONLY a strict JSON object with two arrays of domain names (strings). Do not include markdown wraps like ```json.
    {{
        "valid": ["domain1.com", "domain2.com"],
        "disqualified": ["outlier.com"]
    }}
    """
    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```json"): content = content[7:]
        if content.startswith("```"): content = content[3:]
        if content.endswith("```"): content = content[:-3]
            
        data = json.loads(content)
        valid_domains = set(data.get("valid", [c["domain"] for c in competitors]))
        
        valid_comps = [c for c in competitors if c["domain"] in valid_domains]
        disq_comps = [c for c in competitors if c["domain"] not in valid_domains]
        
        if disq_comps:
            print(f"    [WARN] {provider} Disqualified {len(disq_comps)} outliers: {', '.join([c['name'] for c in disq_comps])}")
        
        return valid_comps, disq_comps
    except Exception as e:
        print(f"    [ERROR] Validation failed ({e}). Proceeding to filter all.")
        return competitors, []

async def filter_sources_via_llm(main_competitor_name: str, source_snippets: list[dict], provider: str = "longcat") -> list[dict]:
    """Filters out competitor-owned blogs masking as neutral directories."""
    client, model_name = get_client(provider)
    
    if (provider == "longcat" and (not LONGCAT_API_KEY or LONGCAT_API_KEY == "missing_key")) or len(source_snippets) == 0:
        return source_snippets

    # We only process the top 15 sources to save tokens
    sources_to_check = source_snippets[:15]
    
    snippets_str = []
    for i, s in enumerate(sources_to_check):
        snippets_str.append(f"[{i}] URL: {s.get('url')} | Title: {s.get('title')}")
        
    prompt = f"""
    You are a data cleanliness filter. We are building a neutral directory of 3rd-party reviews/comparisons for '{main_competitor_name}'.
    
    Below is a list of URLs and titles found in search results.
    Filter out any URLs that are obviously published by direct competitors themselves trying to steal traffic (e.g. if the main competitor is Azure, block DigitalOcean blogs like digitalocean.com/resources/azure-alternatives).
    Keep only neutral 3rd-party review directories, news media, generic tech forums, or official partner domains (like G2, Capterra, TechCrunch, Reddit).
    
    Return ONLY a strict JSON array containing the integer indices of the ALLOWED sources. Exclude the disqualified indices.
    Example output format: [0, 1, 4, 5]
    
    --- Sources ---
    {"\\n".join(snippets_str)}
    """
    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```json"): content = content[7:]
        if content.startswith("```"): content = content[3:]
        if content.endswith("```"): content = content[:-3]
            
        allowed_indices = json.loads(content)
        filtered_sources = [s for i, s in enumerate(sources_to_check) if i in allowed_indices]
        
        diff = len(sources_to_check) - len(filtered_sources)
        if diff > 0:
            print(f"      [FILTER] {provider} ejected {diff} competitor-published blogs for {main_competitor_name}.")
            
        # Append the rest of the unprocessed ones if we sliced them
        filtered_sources.extend(source_snippets[15:])
        return filtered_sources
    except Exception as e:
        print(f"      [ERROR] {provider} LLM Filter failed ({e}). Returning all sources.")
        return source_snippets

async def discover_competitors(target_name: str, target_domain: str, provider: str = "longcat") -> list[dict]:
    """Discovers competitors of a target using AI and search."""
    client, model_name = get_client(provider)
    
    print(f"\n[DISCOVERY] Finding competitors of {target_name}...")
    
    # Search for competitors
    search_queries = [
        f"{target_name} competitors alternatives",
        f"best alternatives to {target_name}",
        f"{target_name} vs competitors comparison",
        f"top {target_name} competitors 2024"
    ]
    
    all_sources = []
    from agents.brave_search import brave_search
    for query in search_queries:
        results = await brave_search(query, extra_snippets=True)
        if results:
            all_sources.extend(results)
    
    print(f"  [OK] Found {len(all_sources)} raw sources for competitor discovery.")
    
    # Use AI to extract competitor list from sources
    sources_text = "\n".join([f"- {s.get('title', '')}: {s.get('description', '')}" for s in all_sources[:20]])
    
    prompt = f"""
    You are a market research expert. Given the search results about {target_name} ({target_domain}), identify ALL direct competitors.
    
    Return ONLY a strict JSON array of competitors. Each competitor must have:
    - name: Company/product name
    - domain: Their main website domain (e.g., "aws.amazon.com")
    - category: Type of competitor (direct, indirect, alternative)
    
    Filter OUT:
    - The target itself ({target_name})
    - Articles about the target (not actual competitors)
    - Blogs comparing the target (not competitors themselves)
    - Irrelevant companies
    
    Example output:
    [
      {{"name": "Amazon Web Services", "domain": "aws.amazon.com", "category": "direct"}},
      {{"name": "Google Cloud Platform", "domain": "cloud.google.com", "category": "direct"}}
    ]
    
    Search Results:
    {sources_text}
    """
    
    try:
        response = await client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        content = response.choices[0].message.content.strip()
        if content.startswith("```json"): content = content[7:]
        if content.startswith("```"): content = content[3:]
        if content.endswith("```"): content = content[:-3]
        
        competitors = json.loads(content)
        
        # Remove the target itself if present
        competitors = [c for c in competitors if target_name.lower() not in c['name'].lower()]
        
        # Remove duplicates by domain
        seen_domains = set()
        unique_competitors = []
        for c in competitors:
            domain = c['domain'].replace('www.', '')
            if domain not in seen_domains:
                seen_domains.add(domain)
                unique_competitors.append(c)
        
        print(f"  [OK] Discovered {len(unique_competitors)} unique competitors:")
        for c in unique_competitors:
            print(f"    - {c['name']} ({c['domain']}) [{c.get('category', 'unknown')}]")
        
        return unique_competitors
        
    except Exception as e:
        print(f"  [ERROR] Competitor discovery failed: {e}")
        return []


# LLM Provider helper functions for Prefect workflows (longcat & fmxdns only)
async def _call_llm(prompt: str, provider: str = "longcat") -> str:
    """Call LLM API (longcat or fmxdns) and return completion."""
    client = get_client(provider)
    model = "gpt-4o" if provider == "longcat" else "llama-3.1-8b"
    
    response = await client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    return response.choices[0].message.content.strip()
