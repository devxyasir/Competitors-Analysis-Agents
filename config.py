from dotenv import load_dotenv
import os

load_dotenv()

# API Keys
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
LONGCAT_API_KEY = os.getenv("LONGCAT_API_KEY")
FMXDNS_API_KEY = os.getenv("FMXDNS_API_KEY")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# Competitors - edit this list
COMPETITORS = [
    {"name": "Microsoft Azure", "domain": "azure.microsoft.com"},
    {"name": "AWS",             "domain": "aws.amazon.com"},
    {"name": "Digital Ocean",   "domain": "digitalocean.com"},
    # Add more competitors here
]

# Caps (from spreadsheet)
MAX_RESULTS = 100
TIMEOUT_SEC = 60
MAX_PER_QUERY = 5

# Fields to collect
FIELDS = ["pricing", "products", "website", "sw_stack", "exposure"]

# Source types to discover
SOURCE_TYPES = [
    "directory_listings",
    "comparison_websites",
    "alternative_websites",
    "review_websites",
    "exposure",
    "media",
]
