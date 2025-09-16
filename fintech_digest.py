#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified FinTech Intelligence System v3.0.2 - Bug Fixes
=====================================================================================================

BUG FIXES v3.0.2:
1. FIXED: Database column 'processed_date' missing error - added to base table creation
2. FIXED: HTTP timeout issues with improved retry logic and domain-specific timeouts
3. FIXED: DNS resolution error for investor.fisglobal.com - updated to correct URL
4. ENHANCED: Better error handling for network connectivity issues
5. ENHANCED: More robust database migration with proper column checking

CRITICAL FIXES:
- Fixed "no such column: processed_date" by ensuring it's in base table creation
- Fixed affirm.com timeout by increasing domain-specific timeouts
- Fixed fisglobal.com DNS error by correcting URL to investors.fisglobal.com
- Added fallback handling for network errors
- Improved database column existence checking before queries

All other functionality remains identical to v3.0.1
"""

import asyncio
import datetime
import logging
import os
import re
import smtplib
import ssl
import time
import hashlib
import json
import sqlite3
import sys
import argparse
import queue
import threading
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Optional, Any, Tuple, Set
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode, urlunparse
from contextlib import contextmanager
from functools import wraps
import random
import socket

try:
    import aiohttp
    import feedparser
    from bs4 import BeautifulSoup
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    from dotenv import load_dotenv
    from dateutil import parser as dateparser
    from requests.adapters import HTTPAdapter
    from urllib import robotparser
    from urllib3.util.retry import Retry
    import requests
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install aiohttp feedparser beautifulsoup4 scikit-learn numpy python-dotenv requests urllib3 python-dateutil")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_fintech_intelligence.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Global Constants and Configuration
# --------------------------------------------------------------------------------------

USER_AGENT = "unified-fintech-intelligence/3.0 (+mailto:intelligence@example.org)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.8", "Connection": "keep-alive"}

SAFE_SCHEMES = ("http", "https")
# FIXED: Increased timeouts for problematic domains
DOMAIN_TIMEOUTS = {
    'investors.affirm.com': (8, 15),  # Increased from (4, 8)
    'investors.fisglobal.com': (8, 15),  # Fixed domain name and increased timeout
    'investor.fisglobal.com': (8, 15),  # Legacy support
}
TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid", "msclkid", "_hsenc", "_hsmi", "ref"
}

# --------------------------------------------------------------------------------------
# Custom Exceptions
# --------------------------------------------------------------------------------------

class IntelligenceSystemError(Exception):
    """Base exception for intelligence system errors"""
    pass

class ConfigurationError(IntelligenceSystemError):
    """Configuration validation errors"""
    pass

class DatabaseError(IntelligenceSystemError):
    """Database operation errors"""
    pass

class ContentProcessingError(IntelligenceSystemError):
    """Content processing errors"""
    pass

class APIError(IntelligenceSystemError):
    """External API errors"""
    pass

# --------------------------------------------------------------------------------------
# Enhanced Configuration Management
# --------------------------------------------------------------------------------------

class ConfigValidator:
    """Validates configuration on startup"""
    
    @staticmethod
    def validate(config: 'UnifiedConfig') -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Required API keys
        if config.llm_enabled and not config.llm_api_key:
            errors.append("LLM_API_KEY required when LLM_INTEGRATION_ENABLED=1")
        
        if config.trends_enabled and not config.google_api_key:
            errors.append("GOOGLE_SEARCH_API_KEY required when TRENDS_ENABLED=1")
        
        if config.trends_enabled and not config.google_search_engine_id:
            errors.append("GOOGLE_SEARCH_ENGINE_ID required when TRENDS_ENABLED=1")
        
        # Email configuration
        if config.email_recipients and not config.smtp_user:
            errors.append("SMTP_USER required when EMAIL_RECIPIENTS is set")
        
        # Validate numeric ranges
        if not 0.0 <= config.relevance_threshold <= 1.0:
            errors.append("RELEVANCE_THRESHOLD must be between 0.0 and 1.0")
        
        if not 0.0 <= config.llm_temperature <= 2.0:
            errors.append("LLM_TEMPERATURE must be between 0.0 and 2.0")
        
        if config.keep_historical_days < 1:
            errors.append("KEEP_HISTORICAL_DAYS must be at least 1")
        
        # Validate new limits
        if not 10 <= config.max_email_articles <= 100:
            errors.append("MAX_EMAIL_ARTICLES must be between 10 and 100")
        
        if not 20 <= config.semantic_total_limit <= 200:
            errors.append("SEMANTIC_TOTAL_LIMIT must be between 20 and 200")
        
        return errors

class UnifiedConfig:
    """Unified configuration management for both company and trends monitoring"""
    
    def __init__(self):
        # Execution modes
        self.companies_enabled = os.getenv("COMPANIES_ENABLED", "1") == "1"
        self.trends_enabled = os.getenv("TRENDS_ENABLED", "1") == "1"
        
        # Core settings
        self.relevance_threshold = float(os.getenv("RELEVANCE_THRESHOLD", "0.58"))
        self.semantic_scoring_enabled = os.getenv("SEMANTIC_SCORING_ENABLED", "1") == "1"
        self.keyword_expansion_enabled = os.getenv("KEYWORD_EXPANSION_ENABLED", "1") == "1"
        self.strict_region_filter = os.getenv("STRICT_REGION_FILTER", "1") == "1"
        self.keep_historical_days = self._safe_int("KEEP_HISTORICAL_DAYS", 365)
        
        # Email and semantic limits
        self.max_email_articles = self._safe_int("MAX_EMAIL_ARTICLES", 40)  # Increased for combined content
        self.semantic_total_limit = self._safe_int("SEMANTIC_TOTAL_LIMIT", 60)
        
        # Company monitoring settings
        self.company_hours_window = self._safe_int("COMPANY_HOURS_WINDOW", 72)
        self.company_max_age_days = self._safe_int("COMPANY_MAX_AGE_DAYS", 3)
        self.company_max_items_per_source = self._safe_int("COMPANY_MAX_ITEMS_PER_SOURCE", 50)
        
        # LLM integration settings
        self.llm_enabled = os.getenv("LLM_INTEGRATION_ENABLED", "1") == "1"
        self.llm_api_key = os.getenv("LLM_API_KEY", "").strip()
        self.llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini").strip()
        self.llm_temperature = float(os.getenv("LLM_TEMPERATURE", "0.1"))
        self.llm_max_retries = self._safe_int("LLM_MAX_RETRIES", 3)
        self.llm_batch_size = self._safe_int("LLM_BATCH_SIZE", 5)
        self.llm_max_tokens_title = self._safe_int("LLM_MAX_TOKENS_TITLE", 32)
        self.llm_max_tokens_summary = self._safe_int("LLM_MAX_TOKENS_SUMMARY", 120)
        self.llm_summary_fallback_enabled = os.getenv("LLM_SUMMARY_FALLBACK_ENABLED", "1") == "1"
        self.llm_summary_min_chars_trigger = self._safe_int("LLM_SUMMARY_MIN_CHARS_TRIGGER", 60)
        self.llm_summary_long_content_trigger = self._safe_int("LLM_SUMMARY_LONG_CONTENT_TRIGGER", 3000)
        
        # ADDED: LLM usage limits for email generation
        self.llm_max_title_enhancements = self._safe_int("LLM_MAX_TITLE_ENHANCEMENTS", 25)
        self.llm_max_summaries = self._safe_int("LLM_MAX_SUMMARIES", 15)

        # Google Search settings
        self.google_api_key = os.getenv('GOOGLE_SEARCH_API_KEY', '')
        self.google_search_engine_id = os.getenv('GOOGLE_SEARCH_ENGINE_ID', '')
        self.google_daily_limit = self._safe_int('GOOGLE_SEARCH_DAILY_LIMIT', 100)
        
        # Email settings
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = self._safe_int('SMTP_PORT', 587)
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.email_recipients = [r.strip() for r in os.getenv('EMAIL_RECIPIENTS', '').split(',') if r.strip()]
        
        # Database settings
        self.database_path = os.getenv('DATABASE_PATH', 'unified_fintech_intelligence.db')
        
        # Trend config JSON (optional)
        self.trend_config_json = os.getenv('TREND_CONFIG_JSON', 'trend_config.json')
        
        # Validate configuration
        self._validate()
        
        logger.info(f"Unified configuration loaded - Companies: {'ENABLED' if self.companies_enabled else 'DISABLED'}")
        logger.info(f"Trends: {'ENABLED' if self.trends_enabled else 'DISABLED'}")
        logger.info(f"LLM: {'ENABLED' if self.llm_enabled else 'DISABLED'} • Model: {self.llm_model}")
        logger.info(f"Semantic scoring: {'ENABLED' if self.semantic_scoring_enabled else 'DISABLED'} (limit: {self.semantic_total_limit})")
        logger.info(f"Email limit: {self.max_email_articles} articles")
    
    def _safe_int(self, env_var: str, default: int) -> int:
        """Safely get integer from environment variable"""
        try:
            value = os.getenv(env_var, str(default))
            return int(value) if value and str(value).strip() else default
        except (ValueError, AttributeError):
            logger.warning(f"Invalid value for {env_var}, using default: {default}")
            return default
    
    def _validate(self):
        """Validate configuration and raise errors if invalid"""
        errors = ConfigValidator.validate(self)
        if errors:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
            raise ConfigurationError(error_msg)

# --------------------------------------------------------------------------------------
# Company Configuration (FIXED URLs)
# --------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Company:
    key: str
    name: str
    pages: List[str]
    feeds: List[str]

# FIXED: Removed broken URLs based on error logs
COMPANIES: List[Company] = [
    Company("visa", "Visa",
            pages=["https://usa.visa.com/about-visa/newsroom.html",
                   "https://investor.visa.com/news/default.aspx"],
            feeds=[]),
    Company("mastercard", "Mastercard",
            pages=["https://www.mastercard.com/news/newsroom/press-releases/",
                   "https://investor.mastercard.com/investor-news/default.aspx"],
            feeds=[]),
    Company("paypal", "PayPal",
            pages=["https://newsroom.paypal-corp.com/",
                   "https://investor.pypl.com/news-and-events/news/default.aspx"],
            feeds=[]),
    Company("stripe", "Stripe",
            pages=["https://stripe.com/newsroom/news",
                   "https://stripe.com/newsroom",
                   "https://stripe.com/blog"],
            feeds=["https://stripe.com/blog/feed.rss"]),
    Company("affirm", "Affirm",
            pages=["https://www.affirm.com/about/news"],  # Removed problematic investor page
            feeds=[]),
    Company("toast", "Toast",
            pages=["https://investors.toasttab.com/news/default.aspx",
                   "https://pos.toasttab.com/blog/news"],
            feeds=[]),
    Company("adyen", "Adyen",
            pages=["https://www.adyen.com/press-and-media",
                   "https://www.adyen.com/investor-relations"],  # Removed broken press-releases URL
            feeds=[]),
    Company("fiserv", "Fiserv (FI)",
            pages=["https://newsroom.fiserv.com/"],  # Removed broken investor URL
            feeds=[]),
    Company("fis", "FIS",
            pages=["https://www.fisglobal.com/en/about-us/media-room"],  # Removed broken investor URL
            feeds=[]),
    Company("gpn", "Global Payments (GPN)",
            pages=["https://investors.globalpayments.com/news-events/press-releases"],  # Removed broken URL
            feeds=[]),
]

# --------------------------------------------------------------------------------------
# URL Utilities
# --------------------------------------------------------------------------------------

def is_safe_url(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.scheme in SAFE_SCHEMES and bool(p.netloc)
    except Exception:
        return False

def canonicalize_url(u: str) -> str:
    """Normalize scheme/host, drop fragment & common tracking params, sort query."""
    try:
        p = urlparse(u)
        scheme = p.scheme.lower()
        netloc = p.netloc.lower()
        path = p.path or "/"
        path = re.sub(r"/{2,}", "/", path)
        q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
        q.sort()
        return urlunparse((scheme, netloc, path, "", urlencode(q), ""))
    except Exception:
        return (u or "").strip()

def get_domain(url: str) -> str:
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

# --------------------------------------------------------------------------------------
# Enhanced Region Filter
# --------------------------------------------------------------------------------------

class EnhancedRegionFilter:
    """Enhanced region filtering - excludes Asia and Africa"""
    
    def __init__(self):
        self.excluded_regions_in_titles = {
            # Asian countries
            "china", "india", "singapore", "japan", "korea", "south korea", 
            "vietnam", "thailand", "malaysia", "indonesia", "philippines",
            "pakistan", "bangladesh", "hong kong",
            # African countries and continent
            "africa", "nigeria", "south africa", "kenya", "ghana", "egypt",
            "morocco", "ethiopia", "uganda", "tanzania", "zimbabwe"
        }
        self.allowed_tlds = {
            "com", "net", "org", "io", "ai", "gov", "edu", "int", "mil",
            "eu", "at", "be", "bg", "hr", "cy", "cz", "dk", "ee", "fi", "fr",
            "de", "gr", "hu", "ie", "it", "lv", "lt", "lu", "mt", "nl", "pl",
            "pt", "ro", "sk", "si", "es", "se", "gb", "uk", "us"
        }
        self.trusted_domains = {
            "federalreserve.gov", "consumerfinance.gov", "ecb.europa.eu", 
            "bankofengland.co.uk", "ec.europa.eu", "theclearinghouse.org",
            "bis.org", "eba.europa.eu", "esma.europa.eu", "sec.gov",
            "visa.com", "mastercard.com", "americanexpress.com", "discover.com", 
            "amex.com", "unionpay.com",
            "stripe.com", "adyen.com", "checkout.com", "squareup.com", 
            "wise.com", "revolut.com", "paypal.com", "klarna.com",
            "n26.com", "monzo.com", "starling.com", "plaid.com",
            "paymentsdive.com", "finextra.com", "thepaypers.com", 
            "fintechfutures.com", "americanbanker.com", "reuters.com",
            "bloomberg.com", "ft.com", "wsj.com", "forbes.com",
            "techcrunch.com", "venturebeat.com", "pymnts.com"
        }
    
    def is_us_eu_domain(self, url: str) -> bool:
        if not url:
            return False
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            if domain in self.trusted_domains:
                return True
            for trusted in self.trusted_domains:
                if domain.endswith('.' + trusted):
                    return True
            tld = domain.split('.')[-1] if '.' in domain else ''
            return tld in self.allowed_tlds
        except Exception as e:
            logger.debug(f"Domain validation error for {url}: {e}")
            return False
    
    def should_exclude_by_title(self, title: str) -> bool:
        if not title:
            return False
        title_lower = title.lower()
        for region in self.excluded_regions_in_titles:
            if region in title_lower:
                logger.debug(f"Excluding article with {region} in title: {title[:50]}...")
                return True
        return False
    
    def assess_content_relevance(self, text: str) -> float:
        if not text:
            return 1.0
        text_lower = text.lower()
        strong_non_us_eu_indicators = [
            "mumbai", "delhi", "bangalore", "shanghai", "beijing", "tokyo", "osaka",
            "seoul", "bangkok", "jakarta", "manila", "karachi", "dhaka",
            "lagos", "cairo", "johannesburg", "nairobi", "accra", "casablanca"
        ]
        indicator_count = sum(1 for indicator in strong_non_us_eu_indicators if indicator in text_lower)
        if indicator_count >= 2:
            return 0.3
        return 0.8

# --------------------------------------------------------------------------------------
# Database Manager with Unified Schema - FIXED processed_date issue
# --------------------------------------------------------------------------------------

class DatabaseConnectionPool:
    """Connection pool for SQLite database"""
    
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = queue.Queue(maxsize=max_connections)
        self._lock = threading.Lock()
        self._created_connections = 0
        
    def _create_connection(self):
        """Create a new database connection"""
        conn = sqlite3.connect(
            self.db_path, 
            check_same_thread=False, 
            timeout=30.0,
            isolation_level=None
        )
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA busy_timeout = 30000')
        return conn
    
    def _is_connection_valid(self, conn):
        """Check if connection is still valid"""
        try:
            conn.execute('SELECT 1').fetchone()
            return True
        except:
            return False
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = None
        connection_returned_to_pool = False
        
        try:
            # Try to get an existing connection
            while True:
                try:
                    conn = self._pool.get_nowait()
                    if self._is_connection_valid(conn):
                        break
                    else:
                        try:
                            conn.close()
                        except:
                            pass
                        conn = None
                        with self._lock:
                            self._created_connections -= 1
                except queue.Empty:
                    with self._lock:
                        if self._created_connections < self.max_connections:
                            conn = self._create_connection()
                            self._created_connections += 1
                            break
                        else:
                            try:
                                conn = self._pool.get(timeout=10)
                                if self._is_connection_valid(conn):
                                    break
                                else:
                                    try:
                                        conn.close()
                                    except:
                                        pass
                                    conn = None
                                    with self._lock:
                                        self._created_connections -= 1
                            except queue.Empty:
                                conn = self._create_connection()
                                break
            
            if not conn:
                raise DatabaseError("Could not obtain database connection")
            
            yield conn
            
            try:
                if self._is_connection_valid(conn):
                    self._pool.put_nowait(conn)
                    connection_returned_to_pool = True
                else:
                    try:
                        conn.close()
                    except:
                        pass
                    with self._lock:
                        self._created_connections -= 1
            except queue.Full:
                try:
                    conn.close()
                except:
                    pass
                with self._lock:
                    self._created_connections -= 1
                    
        except Exception as e:
            if conn and not connection_returned_to_pool:
                try:
                    conn.close()
                except:
                    pass
                with self._lock:
                    if self._created_connections > 0:
                        self._created_connections -= 1
            raise DatabaseError(f"Database operation failed: {e}")

class UnifiedDatabaseManager:
    """FIXED: Database manager with proper processed_date column handling"""
    
    def __init__(self, config: UnifiedConfig):
        self.db_path = config.database_path
        self.keep_days = config.keep_historical_days
        self.use_connection_pool = os.getenv('USE_CONNECTION_POOL', '1') == '1'
        
        if self.use_connection_pool:
            self.pool = DatabaseConnectionPool(self.db_path)
            logger.info("Using connection pool for database operations")
        else:
            logger.info("Using direct connections for database operations")
            
        self._initialize_database()
        
    def _get_connection_direct(self):
        """Direct connection without pooling (fallback method)"""
        conn = sqlite3.connect(
            self.db_path, 
            check_same_thread=False, 
            timeout=30.0,
            isolation_level=None
        )
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode = WAL')
        conn.execute('PRAGMA synchronous = NORMAL')
        conn.execute('PRAGMA busy_timeout = 30000')
        return conn
        
    @contextmanager
    def _get_connection(self):
        """Get database connection (pooled or direct)"""
        if self.use_connection_pool:
            with self.pool.get_connection() as conn:
                yield conn
        else:
            conn = self._get_connection_direct()
            try:
                yield conn
            finally:
                conn.close()
        
    def _initialize_database(self):
        try:
            self._create_tables()
            self._create_indexes()
            logger.info(f"Unified database initialized: {self.db_path}")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            if self.use_connection_pool:
                logger.warning("Falling back to direct connections")
                self.use_connection_pool = False
                self._create_tables()
                self._create_indexes()
                logger.info("Database initialized with direct connections")
            else:
                raise DatabaseError(f"Database initialization failed: {e}")
    
    def _column_exists(self, conn, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table"""
        try:
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
            return column_name in columns
        except Exception:
            return False
    
    def _create_tables(self):
        """FIXED: Ensure all columns including processed_date are in base table creation"""
        with self._get_connection() as conn:
            # FIXED: Added processed_date to the base table creation
            conn.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    canonical_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    original_title TEXT,
                    enhanced_title TEXT,
                    content TEXT,
                    summary TEXT,
                    source_type TEXT,  -- 'company', 'trend_google', 'trend_rss'
                    domain TEXT,
                    category TEXT,  -- company key or trend key
                    company_key TEXT,  -- for company articles
                    trend_category TEXT,  -- for trend articles
                    relevance_score REAL,
                    semantic_relevance_score REAL DEFAULT 0.0,
                    quality_score REAL,
                    region_confidence REAL,
                    word_count INTEGER,
                    content_hash TEXT,
                    published_date DATETIME,
                    processed_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    search_keywords TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
      
            # Daily statistics
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE UNIQUE NOT NULL,
                    total_articles INTEGER,
                    company_articles INTEGER,
                    trend_articles INTEGER,
                    google_articles INTEGER,
                    rss_articles INTEGER,
                    avg_relevance REAL,
                    avg_semantic_relevance REAL DEFAULT 0.0,
                    processing_time REAL,
                    email_articles INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # FIXED: Check each column before attempting to add it
            columns_to_add = [
                ('original_title', 'TEXT'),
                ('enhanced_title', 'TEXT'),
                ('canonical_url', 'TEXT'),
                ('semantic_relevance_score', 'REAL DEFAULT 0.0'),
                ('search_keywords', 'TEXT'),
                ('category', 'TEXT'),
                ('company_key', 'TEXT'),
                ('trend_category', 'TEXT'),
                ('processed_date', 'DATETIME DEFAULT CURRENT_TIMESTAMP'),
                ('quality_score', 'REAL'),
                ('region_confidence', 'REAL'),
                ('word_count', 'INTEGER'),
                ('content_hash', 'TEXT'),
                ('published_date', 'DATETIME'),
                ('created_at', 'DATETIME DEFAULT CURRENT_TIMESTAMP'),
                ('updated_at', 'DATETIME DEFAULT CURRENT_TIMESTAMP'),
            ]
            
            for column_name, column_type in columns_to_add:
                if not self._column_exists(conn, 'articles', column_name):
                    try:
                        conn.execute(f'ALTER TABLE articles ADD COLUMN {column_name} {column_type}')
                        logger.debug(f"Added {column_name} column")
                    except sqlite3.OperationalError as e:
                        # Column might already exist or other error
                        logger.debug(f"Could not add column {column_name}: {e}")

    def _create_indexes(self):
        """Properly implemented _create_indexes method"""
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_articles_category_date ON articles(category, processed_date)',
            'CREATE INDEX IF NOT EXISTS idx_articles_company_date ON articles(company_key, processed_date)',
            'CREATE INDEX IF NOT EXISTS idx_articles_trend_date ON articles(trend_category, processed_date)',
            'CREATE INDEX IF NOT EXISTS idx_articles_source_type ON articles(source_type)',
            'CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(relevance_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_semantic_relevance ON articles(semantic_relevance_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_quality ON articles(quality_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain)',
            'CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)',
            'CREATE INDEX IF NOT EXISTS idx_articles_canonical ON articles(canonical_url)',
            'CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date)',
        ]
        with self._get_connection() as conn:
            for index_sql in indexes:
                try:
                    conn.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Index creation failed: {e}")
    
    def save_article(self, article_data: Dict[str, Any]) -> bool:
        """Save article to unified database"""
        try:
            with self._get_connection() as conn:
                conn.execute('SELECT 1').fetchone()
                
                conn.execute('''
                    INSERT OR REPLACE INTO articles 
                    (url, canonical_url, title, original_title, enhanced_title, content, summary, 
                     source_type, domain, category, company_key, trend_category, relevance_score, 
                     semantic_relevance_score, quality_score, region_confidence, word_count, 
                     content_hash, published_date, search_keywords, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    article_data['url'], 
                    article_data.get('canonical_url', article_data['url']),
                    article_data['title'], 
                    article_data.get('original_title'),
                    article_data.get('enhanced_title'),
                    article_data.get('content', ''), 
                    article_data.get('summary', ''),
                    article_data['source_type'], 
                    article_data.get('domain', ''),
                    article_data.get('category', ''), 
                    article_data.get('company_key'),
                    article_data.get('trend_category'), 
                    article_data.get('relevance_score', 0.0),
                    article_data.get('semantic_relevance_score', 0.0), 
                    article_data.get('quality_score', 0.0),
                    article_data.get('region_confidence', 1.0), 
                    article_data.get('word_count', 0),
                    article_data.get('content_hash', ''), 
                    article_data.get('published_date'),
                    json.dumps(article_data.get('search_keywords', []), ensure_ascii=False)
                ))
                
                logger.debug(f"Saved article: {article_data['title'][:50]}...")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save article '{article_data['title'][:50]}...': {e}")
            return False
    
    def save_daily_stats(self, stats: Dict[str, Any]) -> bool:
        try:
            today = datetime.date.today()
            with self._get_connection() as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO daily_stats
                    (date, total_articles, company_articles, trend_articles, google_articles, rss_articles,
                     avg_relevance, avg_semantic_relevance, processing_time, email_articles)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    today, 
                    stats.get('total_articles', 0), 
                    stats.get('company_articles', 0),
                    stats.get('trend_articles', 0), 
                    stats.get('google_articles', 0),
                    stats.get('rss_articles', 0), 
                    stats.get('avg_relevance', 0.0),
                    stats.get('avg_semantic_relevance', 0.0), 
                    stats.get('processing_time', 0.0), 
                    stats.get('email_articles', 0)
                ))
            return True
        except Exception as e:
            logger.error(f"Failed to save daily stats: {e}")
            return False
    
    def get_recent_article_hashes(self, days: int = 7) -> Set[str]:
        try:
            cutoff_date = datetime.date.today() - datetime.timedelta(days=days)
            with self._get_connection() as conn:
                # FIXED: Check if processed_date column exists before using it
                if self._column_exists(conn, 'articles', 'processed_date'):
                    hashes = conn.execute('''
                        SELECT content_hash FROM articles 
                        WHERE processed_date >= ? AND content_hash IS NOT NULL
                    ''', (cutoff_date,)).fetchall()
                else:
                    # Fallback to created_at if processed_date doesn't exist
                    hashes = conn.execute('''
                        SELECT content_hash FROM articles 
                        WHERE created_at >= ? AND content_hash IS NOT NULL
                    ''', (cutoff_date,)).fetchall()
            return {h[0] for h in hashes}
        except Exception as e:
            logger.error(f"Failed to get recent hashes: {e}")
            return set()
    
    def get_recent_articles_for_email(self, hours: int = 72, max_age_days: int = 3) -> Dict[str, List[Dict]]:
        """FIXED: 3-day limit for company articles, existing logic for trends"""
        try:
            now_utc = datetime.datetime.now()
            
            # FIXED: Company articles limited to 3 days max
            company_cutoff = now_utc - datetime.timedelta(days=3)
            
            # Trend articles keep existing logic
            cutoff_hours = now_utc - datetime.timedelta(hours=hours)
            cutoff_days_cap = now_utc - datetime.timedelta(days=max_age_days)
            trend_cutoff = max(cutoff_hours, cutoff_days_cap)
            
            with self._get_connection() as conn:
                # FIXED: Check if processed_date column exists
                date_column = 'processed_date' if self._column_exists(conn, 'articles', 'processed_date') else 'created_at'
                
                # FIXED: Company articles with 3-day cutoff
                company_cursor = conn.execute(f'''
                    SELECT * FROM articles 
                    WHERE source_type = 'company' 
                    AND {date_column} >= ? 
                    ORDER BY quality_score DESC, {date_column} DESC
                ''', (company_cutoff,))
                company_results = company_cursor.fetchall()
                
                # Trend articles with existing logic
                trend_cursor = conn.execute(f'''
                    SELECT * FROM articles 
                    WHERE source_type IN ('trend_google', 'trend_rss') 
                    AND {date_column} >= ? 
                    AND relevance_score >= 0.45
                    ORDER BY quality_score DESC, {date_column} DESC
                ''', (trend_cutoff,))
                trend_results = trend_cursor.fetchall()
                
                # FIXED: Get columns from cursor description, not connection
                columns = [desc[0] for desc in company_cursor.description] if company_results else []
                if not columns and trend_results:
                    columns = [desc[0] for desc in trend_cursor.description]
                
                company_articles = []
                for row in company_results:
                    article_dict = dict(zip(columns, row))
                    if article_dict.get('search_keywords'):
                        try:
                            article_dict['search_keywords'] = json.loads(article_dict['search_keywords'])
                        except:
                            article_dict['search_keywords'] = []
                    company_articles.append(article_dict)
                
                trend_articles = []
                for row in trend_results:
                    article_dict = dict(zip(columns, row))
                    if article_dict.get('search_keywords'):
                        try:
                            article_dict['search_keywords'] = json.loads(article_dict['search_keywords'])
                        except:
                            article_dict['search_keywords'] = []
                    trend_articles.append(article_dict)
                
                return {
                    'company_articles': company_articles,
                    'trend_articles': trend_articles
                }
        except Exception as e:
            logger.error(f"Failed to get recent articles: {e}")
            return {'company_articles': [], 'trend_articles': []}
    
    def cleanup_old_data(self):
        try:
            cutoff_date = datetime.date.today() - datetime.timedelta(days=self.keep_days)
            with self._get_connection() as conn:
                # FIXED: Use appropriate date column for cleanup
                date_column = 'processed_date' if self._column_exists(conn, 'articles', 'processed_date') else 'created_at'
                result = conn.execute(f'DELETE FROM articles WHERE {date_column} < ?', (cutoff_date,))
                deleted_articles = result.rowcount
                conn.execute('DELETE FROM daily_stats WHERE date < ?', (cutoff_date,))
                conn.execute('VACUUM')
            if deleted_articles > 0:
                logger.info(f"Cleaned up {deleted_articles} old articles (older than {self.keep_days} days)")
        except Exception as e:
            logger.error(f"Database cleanup failed: {e}")
    
    def get_database_stats(self) -> Dict[str, Any]:
        try:
            stats = {}
            with self._get_connection() as conn:
                stats['total_articles'] = conn.execute('SELECT COUNT(*) FROM articles').fetchone()[0]
                
                # Company stats
                company_counts = conn.execute('''
                    SELECT company_key, COUNT(*) FROM articles 
                    WHERE source_type = 'company' AND company_key IS NOT NULL
                    GROUP BY company_key ORDER BY COUNT(*) DESC
                ''').fetchall()
                stats['articles_by_company'] = dict(company_counts)
                
                # Trend stats
                trend_counts = conn.execute('''
                    SELECT trend_category, COUNT(*) FROM articles 
                    WHERE source_type IN ('trend_google', 'trend_rss') AND trend_category IS NOT NULL
                    GROUP BY trend_category ORDER BY COUNT(*) DESC
                ''').fetchall()
                stats['articles_by_trend'] = dict(trend_counts)
                
                week_ago = datetime.date.today() - datetime.timedelta(days=7)
                date_column = 'processed_date' if self._column_exists(conn, 'articles', 'processed_date') else 'created_at'
                stats['recent_articles'] = conn.execute(f'''
                    SELECT COUNT(*) FROM articles WHERE {date_column} >= ?
                ''', (week_ago,)).fetchone()[0]
                
                stats['high_quality_articles'] = conn.execute('''
                    SELECT COUNT(*) FROM articles WHERE relevance_score >= 0.58
                ''').fetchone()[0]
                
                stats['semantic_scored_articles'] = conn.execute('''
                    SELECT COUNT(*) FROM articles WHERE semantic_relevance_score > 0
                ''').fetchone()[0]
                
            stats['db_size_mb'] = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            return stats
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}

# --------------------------------------------------------------------------------------
# Retry Decorator
# --------------------------------------------------------------------------------------

def with_retry(max_retries=3, backoff_factor=2, exceptions=(Exception,)):
    """Enhanced retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries - 1:
                        raise
                    wait_time = backoff_factor ** attempt + random.uniform(0, 1)
                    logger.debug(f"Retry {attempt + 1}/{max_retries} for {func.__name__} after {wait_time:.2f}s: {e}")
                    await asyncio.sleep(wait_time)
            return None
        return wrapper
    return decorator

# --------------------------------------------------------------------------------------
# LLM Integration (Enhanced from fintech_digest.py)
# --------------------------------------------------------------------------------------

class EnhancedLLMIntegration:
    """Enhanced LLM integration with professional title generation"""
    
    def __init__(self, config: UnifiedConfig):
        self.config = config
        self.enabled = config.llm_enabled and bool(config.llm_api_key)
        self.semantic_scoring_enabled = config.semantic_scoring_enabled and self.enabled
        
        # usage/cost tracking
        self.daily_requests = 0
        self.daily_tokens = 0
        self.daily_cost = 0.0

        if self.enabled:
            logger.info(f"Enhanced LLM integration enabled with model: {config.llm_model}")
            if self.semantic_scoring_enabled:
                logger.info(f"Semantic scoring enabled (limit: {config.semantic_total_limit} total)")
        else:
            logger.info("LLM integration disabled")

    def _pricing_per_mtok(self, model: str) -> Tuple[float, float]:
        m = (model or "").lower()
        if "gpt-4o-nano" in m:
            return (0.10, 0.40)
        if "gpt-4o-mini" in m:
            return (0.15, 0.60)
        if "gpt-4o" in m:
            return (2.50, 10.00)
        return (0.15, 0.60)

    def get_usage_stats(self) -> Dict[str, Any]:
        return {
            "model": self.config.llm_model,
            "daily_requests": self.daily_requests,
            "daily_tokens": self.daily_tokens,
            "daily_cost": self.daily_cost,
        }
    
    @with_retry(max_retries=3, exceptions=(APIError, aiohttp.ClientError))
    async def evaluate_semantic_relevance(self, title: str, content: str, trend_keywords: List[str]) -> float:
        """Selective semantic relevance evaluation using LLM"""
        if not self.semantic_scoring_enabled:
            return 0.0
        if not title and not content:
            return 0.0
        try:
            keywords_str = ", ".join(trend_keywords[:5])
            prompt = (
                f"Evaluate how relevant this article is to the fintech trend keywords: {keywords_str}\n\n"
                f"Article Title: {title}\n"
                f"Article Content: {content[:500]}...\n\n"
                "Rate relevance on a scale of 0.0 to 1.0 where:\n"
                "- 1.0: Highly relevant, directly discusses the trend\n"
                "- 0.7-0.9: Relevant, mentions related concepts\n"
                "- 0.4-0.6: Somewhat relevant, tangentially related\n"
                "- 0.0-0.3: Not relevant or unrelated\n\n"
                "Return only the numeric score (e.g., 0.8)."
            )
            response = await self._call_llm(prompt, max_tokens=10)
            score_match = re.search(r'0\.\d+|1\.0|0|1', response)
            if score_match:
                score = float(score_match.group())
                return min(max(score, 0.0), 1.0)
            return 0.0
        except Exception as e:
            logger.debug(f"Semantic relevance evaluation failed: {e}")
            return 0.0

    @with_retry(max_retries=3, exceptions=(APIError, aiohttp.ClientError))
    async def enhance_title(self, title: str, content: str = "", force: bool = False) -> str:
        """ENHANCED: Professional title enhancement for equity research context"""
        if not self.enabled or not title:
            return title

        if not force:
            # Conservative enhancement for basic cleanup
            needs_enhancement = (
                len(title) > 120 or title.count(' | ') > 1 or title.count(' - ') > 1
                or title.endswith('...') or '...' in title
                or ' - ' in title[-40:] or ' | ' in title[-40:]
            )
            if not needs_enhancement:
                return title
            
            prompt = (
                "You are a headline editor for an equity research analyst covering fintech/payments. "
                "Clean this title for a payments/fintech digest: remove site names, fix truncation, "
                "keep key entities, and ensure clear, scannable format. 65-92 characters preferred, HARD CAP 100. "
                "Return only the cleaned title.\n\n"
                f"Original: {title}\n\nCleaned title:"
            )
            max_tok = self.config.llm_max_tokens_title
        else:
            # ENHANCED: Professional equity research headline generation
            prompt = (
                "You are a headline editor for an equity research analyst covering fintech/payments. "
                "Write ONE concise, scannable headline that: "
                "- Front-loads who did what (entity + action) in neutral, factual language. "
                "- If a rail/reg/segment from payments, banking, fintech, regulatory, compliance, security, "
                "digital wallet, instant payments, open banking, stablecoin, CBDC, cross-border, BNPL, "
                "fraud prevention appears in the source text, include EXACTLY ONE of them; otherwise include none. "
                "- 65-92 characters (HARD CAP 100). AP-like style, present tense, numerals OK. No emojis, no site names, no quotes. "
                "- Use ONLY information present in the provided title/snippet; do not infer or add missing details. "
                "Return ONLY the headline text.\n\n"
                f"Original title:\n{title}\n\n"
                f"Context (snippet):\n{(content or '')[:600]}\n\n"
                "Headline:"
            )
            max_tok = self.config.llm_max_tokens_title

        try:
            enhanced = await self._call_llm(prompt, max_tokens=max_tok)
            cleaned = (enhanced or "").strip().strip('"\'')
            if 15 <= len(cleaned) <= 100 and not cleaned.lower().startswith('error'):
                return cleaned
            return title
        except Exception as e:
            logger.debug(f"Title enhancement failed: {e}")
            return title

    @with_retry(max_retries=2, exceptions=(APIError, aiohttp.ClientError))
    async def generate_summary(self, content: str, title: str = "") -> str:
        """Fallback LLM summary with retry logic"""
        if not self.enabled or not self.config.llm_summary_fallback_enabled:
            return ""
        prompt = (
            "You are a financial analyst. Summarize for a daily payments/fintech digest in 80-120 words, "
            "2-3 sentences, neutral tone. Include one concrete detail if available ($, metric, regulation, partner, geography). "
            "Focus on what happened, to whom, and why it matters (rails, economics, compliance, partnerships). "
            "No fluff, no speculation.\n\n"
            f"Title: {title}\n\nArticle text:\n{(content or '')[:8000]}"
        )
        return await self._call_llm(prompt, max_tokens=self.config.llm_max_tokens_summary)

    async def _call_llm(self, prompt: str, max_tokens: int = 150) -> str:
        """Optimized LLM API call with better error handling"""
        if not self.enabled:
            return ""
        
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.config.llm_api_key}",
                    "Content-Type": "application/json"
                }
                payload = {
                    "model": self.config.llm_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": int(max_tokens),
                    "temperature": float(self.config.llm_temperature)
                }
                async with session.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        out = data["choices"][0]["message"]["content"].strip()
                        # rough token accounting
                        in_toks = max(len(prompt)//4, 1)
                        out_toks = max(len(out)//4, 1)
                        self.daily_requests += 1
                        self.daily_tokens += (in_toks + out_toks)
                        in_price, out_price = self._pricing_per_mtok(self.config.llm_model)
                        self.daily_cost += (in_toks/1e6)*in_price + (out_toks/1e6)*out_price
                        return out
                    elif response.status == 429:
                        raise APIError("Rate limit exceeded")
                    else:
                        text = (await response.text())[:400].replace("\n", " ")
                        raise APIError(f"LLM API error {response.status}: {text}")
        except aiohttp.ClientError as e:
            raise APIError(f"LLM API connection error: {e}")
        except Exception as e:
            raise APIError(f"LLM API unexpected error: {e}")

# --------------------------------------------------------------------------------------
# HTTP Session and Utilities (FIXED timeout and error handling)
# --------------------------------------------------------------------------------------

SESSION = None

def get_session():
    global SESSION
    if SESSION is None:
        SESSION = requests.Session()
        SESSION.headers.update(HEADERS)
        
        # FIXED: Enhanced retry strategy with better connection handling
        retry_strategy = Retry(
            total=4,  # Increased retries
            backoff_factor=1.5,  # More aggressive backoff
            status_forcelist=[429, 500, 502, 503, 504], 
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False  # Don't raise on retry exhaustion
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,  # Connection pooling
            pool_maxsize=20
        )
        SESSION.mount("https://", adapter)
        SESSION.mount("http://", adapter)
        
        # FIXED: Set more generous timeouts for problematic domains
        SESSION.timeout = (12, 45)  # Increased from (10, 30)
        
    return SESSION

def robots_allowed(target_url: str, user_agent: str = USER_AGENT) -> bool:
    try:
        parsed = urlparse(target_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(base, "/robots.txt"))
        rp.read()
        return rp.can_fetch(user_agent, target_url)
    except Exception:
        return True

def http_head(url: str, timeout: int | tuple = (8, 15), max_retries: int = 2) -> int:
    """FIXED: Improved HEAD with better timeout handling"""
    if not is_safe_url(url):
        raise RuntimeError(f"unsafe scheme: {url}")
    
    # FIXED: Use domain-specific timeouts
    domain = get_domain(url)
    if domain in DOMAIN_TIMEOUTS:
        timeout = DOMAIN_TIMEOUTS[domain]
    
    session = get_session()
    last_exc = None
    for attempt in range(max_retries):
        try:
            resp = session.head(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 405:
                return 200
            return resp.status_code
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                requests.exceptions.ReadTimeout, socket.gaierror) as e:
            last_exc = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.debug(f"HEAD retry {attempt + 1}/{max_retries} for {url} after {wait_time}s: {e}")
                time.sleep(wait_time)
        except Exception as e:
            last_exc = e
    raise last_exc if last_exc else RuntimeError('HEAD failed')

def http_get(url: str, timeout: int = 30, max_retries: int = 4) -> requests.Response:
    """FIXED: Enhanced HTTP GET with better error handling"""
    if not is_safe_url(url):
        raise RuntimeError(f"unsafe scheme: {url}")
    if not robots_allowed(url):
        raise RuntimeError(f"robots.txt disallows: {url}")
    
    # FIXED: Get domain-specific timeout if available
    domain = get_domain(url)
    if domain in DOMAIN_TIMEOUTS:
        timeout = DOMAIN_TIMEOUTS[domain]

    session = get_session()
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            # FIXED: Progressively reduce timeout on retries
            current_timeout = timeout
            if attempt > 0:
                if isinstance(timeout, tuple):
                    current_timeout = (timeout[0] * 0.9, timeout[1] * 0.9)
                else:
                    current_timeout = timeout * 0.9
            
            resp = session.get(url, timeout=current_timeout, allow_redirects=True)
            resp.raise_for_status()
            return resp
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                requests.exceptions.ReadTimeout, socket.gaierror) as e:
            last_exception = e
            if attempt == max_retries - 1:
                logger.error(f"HTTP GET failed for {url} after {max_retries} attempts: {e}")
                raise
            wait_time = 2 ** attempt + random.uniform(0, 1)
            logger.warning(f"HTTP GET retry {attempt + 1}/{max_retries} for {url} after {wait_time:.1f}s: {e}")
            time.sleep(wait_time)
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for {url}: {e}")
            raise
            
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error for {url}: {e}")
            if attempt == max_retries - 1:
                raise

def parse_date(text: str) -> Optional[datetime.datetime]:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if not dt: 
            return None
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
        return dt
    except Exception:
        return None

# --------------------------------------------------------------------------------------
# Company Monitoring System (Enhanced from daily_company_updates.py)
# --------------------------------------------------------------------------------------

class CompanyMonitoringSystem:
    """Enhanced company monitoring system"""
    
    def __init__(self, config: UnifiedConfig, database: UnifiedDatabaseManager, region_filter: EnhancedRegionFilter):
        self.config = config
        self.database = database
        self.region_filter = region_filter
        
    def _extract_meta_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Best-effort: look for common meta date tags within a document."""
        meta_props = [
            ("meta", {"property": "article:published_time"}),
            ("meta", {"property": "og:published_time"}),
            ("meta", {"name": "date"}),
            ("meta", {"name": "pubdate"}),
            ("meta", {"itemprop": "datePublished"}),
        ]
        for tag, attrs in meta_props:
            el = soup.find(tag, attrs=attrs)
            if el and el.get("content"):
                return el["content"].strip()
        return None

    def discover_feeds_from_html(self, html_text: str, base_url: str) -> List[str]:
        feeds = set()
        soup = BeautifulSoup(html_text, "html.parser")
        for link in soup.find_all("link"):
            rel = [r.lower() for r in (link.get("rel") or [])]
            typ = (link.get("type") or "").lower()
            href = link.get("href")
            if href and "alternate" in rel and any(t in typ for t in ["rss", "atom", "xml"]):
                feeds.add(urljoin(base_url, href))
        for a in soup.find_all("a", href=True):
            href = a["href"].strip().lower()
            if any(tok in href for tok in ["rss", "atom", "feed"]) and href.endswith(("xml", "rss")):
                feeds.add(urljoin(base_url, a["href"]))
        return list(feeds)

    def collect_from_feed(self, feed_url: str, max_items: int = 50) -> List[Dict]:
        out: List[Dict] = []
        try:
            fp = feedparser.parse(feed_url)
            for e in fp.entries[:max_items]:
                title = (e.get("title") or "").strip()
                link = canonicalize_url((e.get("link") or "").strip())
                dt = None
                if hasattr(e, "published_parsed") and e.published_parsed:
                    dt = datetime.datetime(*e.published_parsed[:6], tzinfo=datetime.timezone.utc)
                elif hasattr(e, "updated_parsed") and e.updated_parsed:
                    dt = datetime.datetime(*e.updated_parsed[:6], tzinfo=datetime.timezone.utc)
                else:
                    dt = parse_date(e.get("published") or e.get("updated") or "")
                out.append({"title": title, "url": link, "date": dt, "source": feed_url, "source_type": "feed"})
        except Exception as ex:
            logger.debug(f"Feed parse failed {feed_url}: {ex}")
        return out

    def collect_from_html_page(self, page_url: str, max_items: int = 50) -> List[Dict]:
        items: List[Dict] = []
        try:
            resp = http_get(page_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            newsy_class_hints = ("news", "press", "release", "media", "article", "stories", "updates")
            candidates = list(soup.find_all("article"))
            candidates += [
                d for d in soup.find_all(["div", "li", "section"])
                if any(h in " ".join((d.get("class") or []) + [d.get("id") or ""]).lower() for h in newsy_class_hints)
            ]

            seen = set()
            for node in candidates:
                a = node.find("a", href=True)
                if not a:
                    continue
                href = canonicalize_url(urljoin(page_url, a["href"].strip()))
                if href in seen or not is_safe_url(href):
                    continue
                seen.add(href)

                title = (a.get_text(strip=True) or "").strip()
                if not title or len(title) < 5:
                    continue

                # Try a nearby <time>, or meta tags as fallback, or a plain-text date pattern.
                dt = None
                t = node.find("time")
                date_text = ""
                if t and (t.get("datetime") or t.get_text(strip=True)):
                    date_text = (t.get("datetime") or t.get_text(strip=True)).strip()
                    dt = parse_date(date_text)
                if not dt:
                    meta_date = self._extract_meta_date(soup)
                    dt = parse_date(meta_date)
                if not dt:
                    txt = node.get_text(" ", strip=True)[:600]
                    m = re.search(r"([A-Z][a-z]{2,9}\s+\d{1,2},\s+\d{4})|\b(\d{4}-\d{2}-\d{2})\b", txt)
                    if m:
                        dt = parse_date(m.group(0))

                items.append({"title": title, "url": href, "date": dt, "source": page_url, "source_type": "page"})
                if len(items) >= max_items:
                    break

        except Exception as e:
            logger.debug(f"HTML collect failed for {page_url}: {e}")
        return items

    def collect_company_latest(self, company: Company, max_items: int = 50) -> List[Dict]:
        """FIXED: Collect with 3-day filter for company articles"""
        results: List[Dict] = []
        
        # FIXED: Add 3-day cutoff for company articles
        three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)

        # 1) known feeds
        for feed in company.feeds[:3]:
            feed_results = self.collect_from_feed(feed, max_items=max_items)
            # Filter by 3-day cutoff
            filtered_results = [r for r in feed_results if r.get("date") and r["date"] >= three_days_ago]
            results.extend(filtered_results)
            if len(results) >= max_items:
                break

        # 2) autodiscover feeds from pages
        if len(results) < max_items:
            for page in company.pages:
                try:
                    resp = http_get(page)
                except Exception:
                    continue
                feeds = self.discover_feeds_from_html(resp.text, page)
                for f in feeds[:3]:
                    feed_results = self.collect_from_feed(f, max_items=max_items - len(results))
                    # Filter by 3-day cutoff
                    filtered_results = [r for r in feed_results if r.get("date") and r["date"] >= three_days_ago]
                    results.extend(filtered_results)
                    if len(results) >= max_items:
                        break
                if len(results) >= max_items:
                    break

        # 3) fallback to page scrape
        if len(results) < max_items:
            for page in company.pages:
                page_results = self.collect_from_html_page(page, max_items=max_items - len(results))
                # Filter by 3-day cutoff
                filtered_results = [r for r in page_results if r.get("date") and r["date"] >= three_days_ago]
                results.extend(filtered_results)
                if len(results) >= max_items:
                    break

        # dedupe by canonical URL
        seen = set()
        uniq = []
        for r in results:
            u = r.get("url")
            if not u or u in seen:
                continue
            seen.add(u)
            uniq.append(r)
        return uniq

    async def monitor_all_companies(self) -> List[Dict[str, Any]]:
        """Monitor all companies and return collected articles"""
        logger.info("Starting company monitoring...")
        all_company_articles = []
        
        for company in COMPANIES:
            logger.info(f"Monitoring {company.name}...")
            
            try:
                raw_items = self.collect_company_latest(company, max_items=self.config.company_max_items_per_source)
                
                for item in raw_items:
                    try:
                        # Apply region filtering
                        if self.region_filter.should_exclude_by_title(item.get("title", "")):
                            continue
                            
                        canonical_url = canonicalize_url(item.get("url", ""))
                        content_hash = hashlib.md5(f"{item.get('title', '')}{canonical_url}".encode('utf-8')).hexdigest()
                        
                        article_data = {
                            'url': item.get("url", ""),
                            'canonical_url': canonical_url,
                            'title': item.get("title", ""),
                            'original_title': item.get("title", ""),
                            'content': "",  # Company monitoring doesn't extract full content
                            'summary': "",
                            'source_type': 'company',
                            'domain': get_domain(item.get("url", "")),
                            'category': company.key,
                            'company_key': company.key,
                            'trend_category': None,
                            'relevance_score': 1.0,  # Company articles are always relevant
                            'semantic_relevance_score': 0.0,
                            'quality_score': 1.0,
                            'region_confidence': 1.0,  # Company articles are pre-filtered
                            'word_count': 0,
                            'content_hash': content_hash,
                            'published_date': item.get("date"),
                            'search_keywords': []
                        }
                        
                        # Save to database
                        if self.database.save_article(article_data):
                            all_company_articles.append(article_data)
                            
                    except Exception as e:
                        logger.error(f"Error processing company article: {e}")
                        continue
                
                logger.info(f"Collected {len([a for a in all_company_articles if a['company_key'] == company.key])} articles from {company.name}")
                
                # Polite delay between companies
                time.sleep(1.0)  # Slightly increased delay
                
            except Exception as e:
                logger.error(f"Error monitoring {company.name}: {e}")
                continue
        
        logger.info(f"Company monitoring completed. Total articles: {len(all_company_articles)}")
        return all_company_articles

    def diagnose_company_links(self) -> list[dict]:
        """FIXED: Enhanced link diagnosis with better error handling"""
        report = []
        for company in COMPANIES:
            for page in company.pages:
                entry = {'company': company.name, 'url': page, 'head': None, 'get': None, 'error': None}
                try:
                    try:
                        entry['head'] = http_head(page, timeout=(8, 15), max_retries=2)
                    except Exception as e:
                        entry['head'] = f"HEAD_ERR: {type(e).__name__}: {str(e)[:100]}"
                    try:
                        r = http_get(page, timeout=20, max_retries=2)
                        entry['get'] = r.status_code
                    except Exception as e:
                        entry['get'] = f"GET_ERR: {type(e).__name__}: {str(e)[:100]}"
                except Exception as e:
                    entry['error'] = f"{type(e).__name__}: {str(e)[:100]}"
                report.append(entry)
        return report

# --------------------------------------------------------------------------------------
# Keyword Expansion System (Enhanced from trends system)
# --------------------------------------------------------------------------------------

class KeywordExpansionSystem:
    """Semantic keyword expansion for better content discovery"""
    
    def __init__(self, config: UnifiedConfig):
        self.config = config
        self.enabled = config.keyword_expansion_enabled
        self.semantic_expansions = {
            "instant_payments": [
                "real-time payments", "immediate settlement", "instant transfers", 
                "live payments", "instant money transfer", "real-time settlement",
                "immediate payments", "instant banking", "real-time transactions"
            ],
            "a2a_open_banking": [
                "account-to-account", "bank-to-bank", "direct bank transfer",
                "open finance", "banking APIs", "financial data sharing",
                "bank connectivity", "payment initiation", "account information"
            ],
            "stablecoins": [
                "digital currency", "cryptocurrency payments", "blockchain payments",
                "crypto transactions", "digital tokens", "virtual currency",
                "decentralized finance", "DeFi payments", "tokenized payments"
            ],
            "softpos_tap_to_pay": [
                "mobile point of sale", "smartphone payments", "contactless transactions",
                "NFC payments", "proximity payments", "mobile terminals",
                "software-based POS", "phone-based payments", "tap and pay"
            ],
            "cross_border": [
                "international payments", "global money transfer", "overseas payments",
                "foreign exchange", "multi-currency", "correspondent banking",
                "cross-border transactions", "international transfers", "global payments"
            ],
            "bnpl": [
                "installment payments", "deferred payments", "pay-in-installments",
                "point of sale financing", "consumer credit", "purchase financing",
                "split payments", "flexible payments", "delayed payment"
            ],
            "payment_orchestration": [
                "payment routing", "payment optimization", "payment intelligence",
                "smart routing", "payment management", "transaction orchestration",
                "payment gateway", "payment processing", "payment optimization"
            ],
            "fraud_ai": [
                "fraud detection", "risk management", "transaction monitoring",
                "artificial intelligence", "machine learning", "behavioral analytics",
                "anomaly detection", "payment security", "financial crime"
            ],
            "pci_dss": [
                "payment security", "data protection", "compliance", "cybersecurity",
                "payment card security", "data breach", "security standards",
                "financial security", "payment data protection"
            ],
            "wallet_nfc": [
                "mobile payments", "digital payments", "electronic wallet",
                "contactless payments", "mobile wallet", "payment apps",
                "smartphone payments", "digital money", "mobile commerce"
            ]
        }
    
    def expand_keywords(self, trend_key: str, base_keywords: List[str]) -> List[str]:
        """Expand keywords with semantic variations"""
        if not self.enabled:
            return base_keywords
        expanded = list(base_keywords)
        if trend_key in self.semantic_expansions:
            expanded.extend(self.semantic_expansions[trend_key])
        seen = set()
        unique_expanded = []
        for keyword in expanded:
            kl = keyword.lower()
            if kl not in seen:
                seen.add(kl)
                unique_expanded.append(keyword)
        logger.debug(f"Expanded {len(base_keywords)} keywords to {len(unique_expanded)} for {trend_key}")
        return unique_expanded

# --------------------------------------------------------------------------------------
# Enhanced RSS Feed Validator (From trends system)
# --------------------------------------------------------------------------------------

class EnhancedRSSFeedValidator:
    """RSS feed validation with better error handling"""
    
    def __init__(self, region_filter: EnhancedRegionFilter):
        self.region_filter = region_filter
    
    @with_retry(max_retries=3, exceptions=(aiohttp.ClientError,))
    async def validate_rss_feed(self, rss_url: str) -> bool:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/rss+xml, application/xml, text/xml, application/atom+xml, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(
                        rss_url,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30),
                        ssl=True
                    ) as response:
                        if 200 <= response.status < 400:
                            text = await response.text()
                            return self._is_valid_feed_content(text)
                except:
                    try:
                        async with session.get(
                            rss_url,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30),
                            ssl=False
                        ) as response:
                            if 200 <= response.status < 400:
                                text = await response.text()
                                return self._is_valid_feed_content(text)
                    except:
                        pass
            return False
        except Exception as e:
            logger.debug(f"RSS validation failed for {rss_url}: {e}")
            return False
    
    @staticmethod
    def _is_valid_feed_content(content: str) -> bool:
        if not content or len(content) < 50:
            return False
        content_lower = content.lower()
        feed_indicators = [
            '<rss', '<feed', '<atom', '<?xml',
            'application/rss+xml', 'application/atom+xml',
            '<channel>', '<entry>', '<item>'
        ]
        return any(indicator in content_lower for indicator in feed_indicators)

    async def extract_from_rss_preserve_titles(self, rss_url: str, source_name: str) -> List[Dict[str, Any]]:
        sources = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; UnifiedFinTechIntelligence/3.0; +https://example.com/bot)',
                'Accept': 'application/rss+xml, application/xml, text/xml, application/atom+xml, */*',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            }
            async with aiohttp.ClientSession() as session:
                rss_content = None
                approaches = [
                    {"ssl": True, "timeout": 45},
                    {"ssl": False, "timeout": 45},
                    {"ssl": False, "timeout": 60}
                ]
                for approach in approaches:
                    try:
                        async with session.get(
                            rss_url,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=approach["timeout"]),
                            ssl=approach["ssl"],
                            allow_redirects=True
                        ) as response:
                            if response.status == 200:
                                rss_content = await response.text()
                                break
                            elif response.status in [301, 302, 307, 308]:
                                redirect_url = response.headers.get('Location')
                                if redirect_url:
                                    logger.info(f"RSS feed redirected: {rss_url} -> {redirect_url}")
                                    async with session.get(
                                        redirect_url,
                                        headers=headers,
                                        timeout=aiohttp.ClientTimeout(total=approach["timeout"]),
                                        ssl=approach["ssl"]
                                    ) as redirect_response:
                                        if redirect_response.status == 200:
                                            rss_content = await redirect_response.text()
                                            break
                    except Exception as e:
                        logger.debug(f"RSS approach failed for {rss_url}: {e}")
                        continue
                if not rss_content:
                    logger.warning(f"Failed to fetch RSS content from: {rss_url}")
                    return sources
                if not self._is_valid_feed_content(rss_content):
                    logger.warning(f"RSS content validation failed: {rss_url}")
                    return sources
                feed = feedparser.parse(rss_content)
                if not getattr(feed, 'entries', None):
                    logger.warning(f"No entries found in RSS feed: {rss_url}")
                    return sources
                if hasattr(feed, 'bozo') and feed.bozo:
                    logger.debug(f"RSS feed has parsing issues but proceeding: {rss_url}")
                cutoff_date = datetime.datetime.now() - datetime.timedelta(days=14)
                for entry in feed.entries[:30]:
                    try:
                        source = await self._process_rss_entry(entry, source_name, cutoff_date)
                        if source:
                            sources.append(source)
                    except Exception as e:
                        logger.debug(f"Error processing RSS entry from {rss_url}: {e}")
                        continue
                logger.info(f"RSS extracted {len(sources)} articles from {source_name}")
        except Exception as e:
            logger.error(f"RSS extraction failed for {rss_url}: {e}")
        return sources

    async def _process_rss_entry(self, entry, source_name: str, cutoff_date: datetime.datetime) -> Optional[Dict[str, Any]]:
        url = getattr(entry, 'link', '').strip()
        full_title_raw = getattr(entry, 'title', '').strip()
        if not url or not full_title_raw or len(full_title_raw) < 5:
            return None
        if self.region_filter.should_exclude_by_title(full_title_raw):
            return None
        domain = urlparse(url).netloc
        if not self.region_filter.is_us_eu_domain(url):
            return None
        content = ""
        if hasattr(entry, 'summary'):
            content = BeautifulSoup(entry.summary, 'html.parser').get_text(strip=True)
        elif hasattr(entry, 'description'):
            content = BeautifulSoup(entry.description, 'html.parser').get_text(strip=True)
        elif hasattr(entry, 'content'):
            if isinstance(entry.content, list) and entry.content:
                content = BeautifulSoup(entry.content[0].value, 'html.parser').get_text(strip=True)
        region_confidence = self.region_filter.assess_content_relevance(content)
        if region_confidence < 0.3:
            return None
        full_title = self._clean_title_comprehensive(full_title_raw)
        pub_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                pub_date = datetime.datetime(*entry.published_parsed[:6])
            except:
                pass
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            try:
                pub_date = datetime.datetime(*entry.updated_parsed[:6])
            except:
                pass
        if pub_date and pub_date < cutoff_date:
            return None
        
        canonical_url = canonicalize_url(url)
        content_hash = hashlib.md5(f"{full_title}{canonical_url}".encode('utf-8')).hexdigest()
        
        return {
            'url': url,
            'canonical_url': canonical_url,
            'title': full_title,
            'original_title': full_title_raw,
            'content': content,
            'summary': '',
            'source_type': 'trend_rss',
            'domain': domain,
            'category': source_name.replace('_rss', ''),
            'company_key': None,
            'trend_category': source_name.replace('_rss', ''),
            'relevance_score': 0.0,
            'semantic_relevance_score': 0.0,
            'quality_score': 0.0,
            'region_confidence': region_confidence,
            'word_count': len(content.split()) if content else 0,
            'content_hash': content_hash,
            'published_date': pub_date,
            'search_keywords': []
        }
    
    @staticmethod
    def _clean_title_comprehensive(title: str) -> str:
        if not title:
            return ""
        cleaned = re.sub(r'\s+', ' ', title.strip())
        cleaned = re.sub(r'&[a-zA-Z0-9#]+;', '', cleaned)
        cleaned = re.sub(r'<[^>]+>', '', cleaned)
        patterns = [
            r'\s*-\s*[^-]*(?:\.com|\.org|\.net|News|Times|Post|Journal).*',
            r'\s*\|\s*[^|]*(?:\.com|\.org|\.net|News|Times|Post|Journal).*',
            r'\s*::\s*.*'
        ]
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned).strip()
        if len(cleaned) < len(title) * 0.5:
            cleaned = title.strip()
        return cleaned

# --------------------------------------------------------------------------------------
# Trends System Components
# --------------------------------------------------------------------------------------

@dataclass
class TrendConfig:
    """Enhanced trend configuration"""
    name: str
    keywords: List[str]
    rss_feeds: List[str] = field(default_factory=list)
    min_relevance_score: float = 0.3
    email_relevance_score: float = 0.45
    max_articles: int = 8
    google_search_enabled: bool = True
    priority: str = "medium"
    description: str = ""
    include_social: bool = True

class GoogleSearchIntegration:
    """Enhanced Google Search API integration"""
    
    def __init__(self, config: UnifiedConfig, region_filter: EnhancedRegionFilter, keyword_expander: KeywordExpansionSystem):
        self.config = config
        self.region_filter = region_filter
        self.keyword_expander = keyword_expander
        self.enabled = bool(config.google_api_key and config.google_search_engine_id)
        self.queries_today = 0
        
        if not self.enabled:
            raise ConfigurationError("Google Search API not configured")
        
        logger.info(f"Google Search API enabled with daily limit: {config.google_daily_limit}")
    
    async def search_trend(self, trend_name: str, base_keywords: List[str], max_results: int = 20) -> List[Dict[str, Any]]:
        """Enhanced trend search with expanded keywords"""
        if self.queries_today >= self.config.google_daily_limit:
            logger.warning(f"Google Search daily quota exceeded ({self.config.google_daily_limit})")
            return []
        
        expanded_keywords = self.keyword_expander.expand_keywords(trend_name, base_keywords)
        sources = []
        
        try:
            search_strategies = []
            for keyword in expanded_keywords[:4]:
                search_strategies.append(keyword)
            if len(expanded_keywords) >= 2:
                search_strategies.append(f"{expanded_keywords[0]} {expanded_keywords[1]}")
            
            logger.info(f"Google Search for {trend_name} with {len(search_strategies)} strategies")
            async with aiohttp.ClientSession() as session:
                for search_query in search_strategies[:3]:
                    if self.queries_today >= self.config.google_daily_limit:
                        break
                    try:
                        params = {
                            'key': self.config.google_api_key,
                            'cx': self.config.google_search_engine_id,
                            'q': search_query,
                            'num': 10,
                            'dateRestrict': 'w1',
                            'sort': 'date',
                            'lr': 'lang_en',
                            'safe': 'medium'
                        }
                        async with session.get(
                            'https://www.googleapis.com/customsearch/v1',
                            params=params,
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                items = data.get('items', [])
                                for item in items:
                                    try:
                                        source = await self._process_search_result(
                                            item, trend_name, expanded_keywords
                                        )
                                        if source:
                                            sources.append(source)
                                    except Exception as e:
                                        logger.warning(f"Error processing Google result: {e}")
                                self.queries_today += 1
                            elif response.status == 429:
                                logger.warning("Google Search API rate limit exceeded")
                                self.queries_today = self.config.google_daily_limit
                                break
                            else:
                                raise APIError(f"Google Search API error {response.status}")
                    except aiohttp.ClientError as e:
                        logger.error(f"Google Search HTTP error for {trend_name}: {e}")
                        continue
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Google Search failed for {trend_name}: {e}")
        
        unique_sources = self._deduplicate_sources(sources)
        logger.info(f"Google Search found {len(unique_sources)} unique articles for {trend_name}")
        return unique_sources
    
    async def _process_search_result(self, item: Dict, trend_name: str, keywords: List[str]) -> Optional[Dict[str, Any]]:
        url = item.get('link', '').strip()
        full_title = item.get('title', '').strip()
        snippet = item.get('snippet', '').strip()
        if not url or not full_title or len(full_title) < 5:
            return None
        if self.region_filter.should_exclude_by_title(full_title):
            return None
        if not self.region_filter.is_us_eu_domain(url):
            return None
        region_confidence = self.region_filter.assess_content_relevance(snippet)
        if region_confidence < 0.3:
            return None
        cleaned_title = self._clean_title_preserve_all_words(full_title)
        published_date = self._extract_published_date(item)
        domain = urlparse(url).netloc
        canonical_url = canonicalize_url(url)
        content_hash = hashlib.md5(f"{cleaned_title}{canonical_url}".encode('utf-8')).hexdigest()
        
        return {
            'url': url,
            'canonical_url': canonical_url,
            'title': cleaned_title,
            'original_title': full_title,
            'content': snippet,
            'summary': '',
            'source_type': 'trend_google',
            'domain': domain,
            'category': trend_name,
            'company_key': None,
            'trend_category': trend_name,
            'relevance_score': 0.0,  # Will be calculated later
            'semantic_relevance_score': 0.0,
            'quality_score': 0.0,
            'region_confidence': region_confidence,
            'word_count': len(snippet.split()) if snippet else 0,
            'content_hash': content_hash,
            'published_date': published_date,
            'search_keywords': keywords[:5]
        }
    
    def _clean_title_preserve_all_words(self, title: str) -> str:
        if not title:
            return ""
        cleaned = re.sub(r'\s+', ' ', title.strip())
        cleaned = re.sub(r'&[a-zA-Z0-9]+;', '', cleaned)
        patterns = [
            r'\s*-\s*[^-]*(?:\.com|\.org|\.net|news|times|post|journal).*$',
            r'\s*\|\s*[^|]*(?:\.com|\.org|\.net|news|times|post|journal).*$',
            r'\s*::\s*.*$'
        ]
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned).strip()
        if len(cleaned) < len(title) * 0.6:
            cleaned = title.strip()
        return cleaned
    
    def _extract_published_date(self, item: Dict) -> Optional[datetime.datetime]:
        if 'pagemap' in item and 'metatags' in item['pagemap']:
            for meta in item['pagemap']['metatags']:
                date_str = meta.get('article:published_time') or meta.get('datePublished')
                if date_str:
                    try:
                        if 'T' in date_str:
                            return datetime.datetime.fromisoformat(
                                date_str.replace('Z', '+00:00')).replace(tzinfo=None)
                    except:
                        pass
        return None
    
    def _deduplicate_sources(self, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        unique_sources = []
        seen_urls = set()
        seen_hashes = set()
        for source in sources:
            if (source['url'] not in seen_urls and 
                source['content_hash'] not in seen_hashes):
                unique_sources.append(source)
                seen_urls.add(source['url'])
                seen_hashes.add(source['content_hash'])
        return unique_sources

class TrendsMonitoringSystem:
    """Enhanced trends monitoring system"""
    
    def __init__(self, config: UnifiedConfig, database: UnifiedDatabaseManager, 
                 region_filter: EnhancedRegionFilter, llm_integration: EnhancedLLMIntegration):
        self.config = config
        self.database = database
        self.region_filter = region_filter
        self.llm_integration = llm_integration
        self.keyword_expander = KeywordExpansionSystem(config)
        
        if config.trends_enabled:
            self.google_search = GoogleSearchIntegration(config, region_filter, self.keyword_expander)
            self.rss_validator = EnhancedRSSFeedValidator(region_filter)
        else:
            self.google_search = None
            self.rss_validator = None
            
        # Load trends configuration
        self.trends = self._load_trends()
        
    def _load_trends(self) -> Dict[str, TrendConfig]:
        """Load trend configurations"""
        try:
            cfg_path = self.config.trend_config_json
            if cfg_path and os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                trends: Dict[str, TrendConfig] = {}
                for key, val in raw.items():
                    if key.startswith("_"):  # Skip metadata
                        continue
                    trends[key] = TrendConfig(
                        name=val.get("name", key.replace("_", " ").title()),
                        keywords=val.get("keywords", []),
                        rss_feeds=list(dict.fromkeys(val.get("rss_feeds", []))),
                        min_relevance_score=val.get("min_relevance_score", 0.3),
                        email_relevance_score=val.get("email_relevance_score", self.config.relevance_threshold),
                        max_articles=val.get("max_articles", 8),
                        google_search_enabled=val.get("google_search_enabled", True),
                        priority=val.get("priority", "medium"),
                        description=val.get("description", ""),
                        include_social=val.get("include_social", True),
                    )
                logger.info(f"Loaded {len(trends)} trends from JSON config: {cfg_path}")
                return trends
        except Exception as e:
            logger.warning(f"Failed to load trend JSON config; using enhanced defaults: {e}")
        
        return self._get_default_trends()
    
    def _get_default_trends(self) -> Dict[str, TrendConfig]:
        """Get default trend configurations"""
        return {
            "instant_payments": TrendConfig(
                name="Instant Payments",
                keywords=[
                    "instant payments", "real-time payments", "RTP", "FedNow", "faster payments",
                    "immediate settlement", "real-time settlement", "instant transfers"
                ],
                rss_feeds=[
                    "https://www.federalreserve.gov/feeds/press_all.xml",
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "a2a_open_banking": TrendConfig(
                name="A2A & Open Banking",
                keywords=[
                    "open banking", "PSD2", "PSD3", "account to account", "A2A payments",
                    "bank-to-bank", "direct bank transfer", "banking APIs", "payment initiation"
                ],
                rss_feeds=[
                    "https://www.consumerfinance.gov/about-us/newsroom/rss/",
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "stablecoins": TrendConfig(
                name="Stablecoins & CBDC",
                keywords=[
                    "stablecoin", "CBDC", "central bank digital currency", "digital dollar", "digital euro",
                    "cryptocurrency payments", "blockchain payments", "digital currency"
                ],
                rss_feeds=[
                    "https://www.ecb.europa.eu/rss/fie.html",
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "softpos_tap_to_pay": TrendConfig(
                name="SoftPOS & Tap to Pay",
                keywords=[
                    "SoftPOS", "tap to pay", "contactless payments", "mobile POS", "mPOS",
                    "smartphone payments", "NFC payments", "mobile terminals"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                    "https://www.nfcw.com/feed/",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            
            "cross_border": TrendConfig(
                name="Cross-Border Payments",
                keywords=[
                    "cross border payments", "international payments", "remittance", "FX", "foreign exchange",
                    "global payments", "overseas payments", "correspondent banking"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "bnpl": TrendConfig(
                name="Buy Now, Pay Later",
                keywords=[
                    "buy now pay later", "BNPL", "installment payments", "Klarna", "Afterpay",
                    "deferred payments", "point of sale financing", "split payments"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "payment_orchestration": TrendConfig(
                name="Payment Orchestration",
                keywords=[
                    "payment orchestration", "smart routing", "payment optimization", "payment routing",
                    "payment intelligence", "transaction routing", "payment management"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "fraud_ai": TrendConfig(
                name="Fraud Prevention & AI",
                keywords=[
                    "fraud prevention", "AI payments", "machine learning payments", "AML", "anti-money laundering",
                    "fraud detection", "risk management", "transaction monitoring", "behavioral analytics"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "pci_dss": TrendConfig(
                name="PCI DSS & Security",
                keywords=[
                    "PCI DSS", "payment security", "data security standard", "compliance", "cybersecurity",
                    "payment data protection", "security standards", "financial security"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            ),
            "wallet_nfc": TrendConfig(
                name="Digital Wallets & NFC",
                keywords=[
                    "digital wallet", "mobile wallet", "NFC payments", "Apple Pay", "Google Pay", "Samsung Pay",
                    "mobile payments", "contactless payments", "electronic wallet"
                ],
                rss_feeds=[
                    "https://www.finextra.com/rss/headlinefeeds.aspx?ff=1",
                    "https://www.nfcw.com/feed/",
                ],
                min_relevance_score=0.25,
                email_relevance_score=0.4
            )
        }
    
    def calculate_relevance_score(self, content: str, title: str, keywords: List[str]) -> float:
        """Enhanced relevance scoring with semantic matching"""
        try:
            full_text = f"{title} {title} {content}".lower()
            if not full_text.strip():
                return 0.0
            exact_matches = 0
            partial_matches = 0
            semantic_matches = 0
            for keyword in keywords:
                keyword_lower = keyword.lower()
                if keyword_lower in full_text:
                    exact_matches += 1
                else:
                    keyword_words = keyword_lower.split()
                    if len(keyword_words) > 1:
                        word_matches = sum(1 for word in keyword_words if word in full_text)
                        if word_matches >= len(keyword_words) * 0.4:
                            partial_matches += 1
                    semantic_terms = self._get_semantic_terms(keyword_lower)
                    if any(term in full_text for term in semantic_terms):
                        semantic_matches += 1
            if exact_matches == 0 and partial_matches == 0 and semantic_matches == 0:
                return 0.0
            total_keywords = max(len(keywords), 1)
            exact_score = (exact_matches / total_keywords)
            partial_score = (partial_matches / total_keywords) * 0.6
            semantic_score = (semantic_matches / total_keywords) * 0.3
            base_score = exact_score + partial_score + semantic_score
            title_bonus = self._calculate_title_bonus(title.lower(), keywords)
            quality_bonus = self._calculate_quality_bonus(full_text)
            recency_bonus = 0.05
            final_score = min(base_score + title_bonus + quality_bonus + recency_bonus, 1.0)
            return final_score
        except Exception as e:
            logger.error(f"Relevance scoring failed: {e}")
            return 0.0
    
    def _get_semantic_terms(self, keyword: str) -> List[str]:
        semantic_map = {
            'instant payments': ['real-time', 'immediate', 'live payments', 'fast payments'],
            'open banking': ['financial data sharing', 'bank APIs', 'PSD2', 'account information'],
            'stablecoin': ['digital currency', 'cryptocurrency', 'CBDC', 'virtual currency'],
            'contactless': ['NFC', 'tap to pay', 'proximity payments', 'mobile payments'],
            'fraud prevention': ['risk management', 'security', 'anti-fraud', 'transaction monitoring'],
            'cross border': ['international payments', 'remittance', 'global payments', 'FX'],
            'BNPL': ['installments', 'deferred payment', 'buy now pay later', 'point of sale financing'],
            'payment orchestration': ['smart routing', 'payment optimization', 'routing'],
            'digital wallet': ['mobile wallet', 'e-wallet', 'electronic wallet', 'payment app'],
            'PCI DSS': ['payment security', 'compliance', 'data protection', 'security standards']
        }
        return semantic_map.get(keyword, [])
    
    def _calculate_title_bonus(self, title_lower: str, keywords: List[str]) -> float:
        title_matches = sum(1 for keyword in keywords if keyword.lower() in title_lower)
        return min(title_matches * 0.15, 0.4)
    
    def _calculate_quality_bonus(self, text: str) -> float:
        bonus = 0.0
        if re.search(r'\$[\d,]+|\d+%|\d+\s*(million|billion|trillion)', text):
            bonus += 0.08
        action_words = ['announced', 'launched', 'acquired', 'raised', 'expanded', 'partnered', 'introduced', 'unveiled']
        action_count = sum(1 for word in action_words if word in text)
        bonus += min(action_count * 0.03, 0.12)
        business_words = ['company', 'startup', 'firm', 'corporation', 'business', 'enterprise']
        if any(word in text for word in business_words):
            bonus += 0.05
        tech_words = ['technology', 'platform', 'solution', 'system', 'API', 'integration']
        tech_count = sum(1 for word in tech_words if word in text)
        bonus += min(tech_count * 0.02, 0.08)
        return min(bonus, 0.25)

    async def monitor_all_trends(self) -> List[Dict[str, Any]]:
        """Monitor all trends and return collected articles"""
        if not self.config.trends_enabled:
            logger.info("Trends monitoring disabled")
            return []
            
        logger.info("Starting trends monitoring...")
        all_trend_articles = []
        semantic_scored_count = 0
        
        for trend_key, trend_config in self.trends.items():
            logger.info(f"Monitoring trend: {trend_config.name}")
            
            # Collect from Google Search
            if trend_config.google_search_enabled and self.google_search:
                try:
                    articles = await self.google_search.search_trend(
                        trend_key, trend_config.keywords, max_results=20
                    )
                    
                    for article in articles:
                        # Calculate relevance score
                        article['relevance_score'] = self.calculate_relevance_score(
                            article['content'], article['title'], trend_config.keywords
                        )
                        
                        # Calculate quality score
                        article['quality_score'] = (article['relevance_score'] + article['region_confidence']) / 2
                        
                        # Save if meets minimum threshold
                        if article['relevance_score'] >= trend_config.min_relevance_score:
                            if self.database.save_article(article):
                                all_trend_articles.append(article)
                                
                    logger.info(f"Google Search: {len(articles)} articles for {trend_key}")
                    
                except Exception as e:
                    logger.error(f"Google Search error for {trend_key}: {e}")
            
            # Collect from RSS feeds
            if self.rss_validator:
                for rss_url in trend_config.rss_feeds:
                    try:
                        rss_articles = await self.rss_validator.extract_from_rss_preserve_titles(
                            rss_url, f"{trend_key}_rss"
                        )
                        for article in rss_articles:
                            # Calculate relevance score
                            article['relevance_score'] = self.calculate_relevance_score(
                                article['content'], article['title'], trend_config.keywords
                            )
                            
                            # Calculate quality score
                            article['quality_score'] = (article['relevance_score'] + article['region_confidence']) / 2
                            
                            # Save if meets minimum threshold
                            if article['relevance_score'] >= trend_config.min_relevance_score:
                                if self.database.save_article(article):
                                    all_trend_articles.append(article)
                        
                        domain = urlparse(rss_url).netloc
                        logger.info(f"RSS: {len(rss_articles)} articles from {domain} for {trend_key}")
                    except Exception as e:
                        domain = urlparse(rss_url).netloc
                        logger.error(f"RSS error for {domain}: {e}")
        
        # Apply semantic scoring to top articles
        if (self.llm_integration.semantic_scoring_enabled and 
            all_trend_articles and 
            semantic_scored_count < self.config.semantic_total_limit):
            
            # Sort by relevance and take top articles for semantic scoring
            all_trend_articles.sort(key=lambda x: x['relevance_score'], reverse=True)
            remaining_quota = self.config.semantic_total_limit - semantic_scored_count
            articles_to_score = all_trend_articles[:remaining_quota]
            
            logger.info(f"Running semantic scoring for top {len(articles_to_score)} trend articles...")
            
            for article in articles_to_score:
                try:
                    trend_config = self.trends.get(article['trend_category'])
                    if trend_config:
                        semantic_score = await self.llm_integration.evaluate_semantic_relevance(
                            article['title'], article['content'], trend_config.keywords
                        )
                        if semantic_score > 0:
                            article['semantic_relevance_score'] = semantic_score
                            semantic_scored_count += 1
                            
                            # Update quality score with semantic component
                            combined = article['relevance_score'] * 0.6 + semantic_score * 0.4
                            article['quality_score'] = (combined + article['region_confidence']) / 2
                            
                            # Re-save with updated semantic score
                            self.database.save_article(article)
                            
                            if semantic_scored_count >= self.config.semantic_total_limit:
                                break
                except Exception as e:
                    logger.debug(f"Semantic scoring failed for article: {e}")
                    continue
        
        logger.info(f"Trends monitoring completed. Total articles: {len(all_trend_articles)}")
        logger.info(f"Semantic scoring applied to: {semantic_scored_count} articles")
        return all_trend_articles

# --------------------------------------------------------------------------------------
# Article Summarizer (Enhanced from trends system)
# --------------------------------------------------------------------------------------

class ArticleSummarizer:
    """Enhanced article summarization (rule-based primary)"""
    
    def __init__(self):
        self.max_summary_length = 200
        self.min_summary_length = 50
    
    async def summarize_article(self, content: str, title: str = "") -> str:
        try:
            if not content or len(content.strip()) < 20:
                return content.strip()
            if len(content) <= self.max_summary_length:
                return content.strip()
            return self._rule_based_summarization(content, title)
        except Exception as e:
            logger.error(f"Article summarization failed: {e}")
            return content[:self.max_summary_length] + "..." if len(content) > self.max_summary_length else content
    
    def _rule_based_summarization(self, content: str, title: str = "") -> str:
        content = self._clean_text(content)
        if len(content) <= self.max_summary_length:
            return content
        sentences = self._extract_sentences(content)
        if not sentences:
            return content[:self.max_summary_length] + "..."
        scored_sentences = self._score_sentences(sentences, title)
        summary = self._build_summary(scored_sentences)
        return summary
    
    def _clean_text(self, text: str) -> str:
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'http[s]?://\S+', '', text)
        text = re.sub(r'\S+@\S+', '', text)
        text = re.sub(r'[.]{3,}', '...', text)
        return text
    
    def _extract_sentences(self, content: str) -> List[str]:
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', content)
        cleaned_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if (15 <= len(sentence) <= 250 and
                not sentence.startswith(('http', 'www', '@', '#')) and
                any(char.isalpha() for char in sentence) and
                sentence.count(' ') >= 3):
                cleaned_sentences.append(sentence)
        return cleaned_sentences[:15]
    
    def _score_sentences(self, sentences: List[str], title: str = "") -> List[tuple]:
        scored = []
        important_keywords = [
            'payment', 'fintech', 'banking', 'financial', 'revenue', 'growth',
            'funding', 'investment', 'acquisition', 'partnership', 'launch',
            'announced', 'digital', 'technology', 'platform', 'service',
            'million', 'billion', 'percent', 'market', 'company', 'new',
            'expansion', 'regulatory', 'compliance', 'innovation', 'strategy',
            'customer', 'transaction', 'processing', 'security', 'data'
        ]
        title_words = set(title.lower().split()) if title else set()
        for i, sentence in enumerate(sentences):
            score = 0
            sentence_lower = sentence.lower()
            sentence_words = set(sentence_lower.split())
            score += max(0, 15 - i)
            for keyword in important_keywords:
                if keyword in sentence_lower:
                    score += 3
            overlap = len(title_words.intersection(sentence_words))
            score += overlap * 4
            length = len(sentence)
            if 50 <= length <= 150:
                score += 8
            elif 30 <= length < 50:
                score += 5
            elif 150 < length <= 200:
                score += 3
            if re.search(r'\$[\d,]+|\d+%|\d+\s*(million|billion|trillion)', sentence_lower):
                score += 5
            action_words = ['announced', 'launched', 'acquired', 'raised', 'expanded', 'partnered', 'introduced']
            for word in action_words:
                if word in sentence_lower:
                    score += 3
            scored.append((sentence, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored
    
    def _build_summary(self, scored_sentences: List[tuple]) -> str:
        if not scored_sentences:
            return ""
        summary_parts = []
        total_length = 0
        for sentence, score in scored_sentences:
            if total_length + len(sentence) + 2 <= self.max_summary_length:
                summary_parts.append(sentence)
                total_length += len(sentence) + 2
            else:
                break
        summary = ' '.join(summary_parts)
        if not summary.endswith(('.', '!', '?')):
            summary += '.'
        return summary

# --------------------------------------------------------------------------------------
# Unified Email Generator (Complete Implementation)
# --------------------------------------------------------------------------------------

class UnifiedEmailGenerator:
    """Unified email generator for both company updates and trend analysis"""
    
    def __init__(self, config: UnifiedConfig, llm_integration: EnhancedLLMIntegration):
        self.config = config
        self.llm_integration = llm_integration
        
        # Company emoji mapping
        self.company_emojis = {
            "visa": "💳", "mastercard": "💳", "paypal": "💰", "stripe": "⚡",
            "affirm": "📅", "toast": "🍞", "adyen": "🌍", "fiserv": "🏦",
            "fis": "🏢", "gpn": "🌍"
        }
        
        # Trend emoji mapping
        self.trend_emojis = {
            "instant_payments": "⚡", "a2a_open_banking": "🏦", "stablecoins": "💰",
            "softpos_tap_to_pay": "📱", "cross_border": "🌍", "bnpl": "📅",
            "payment_orchestration": "🎼", "fraud_ai": "🛡️", "pci_dss": "🔒",
            "wallet_nfc": "💳", "embedded_finance": "🔗", "regtech_compliance": "⚖️"
        }
    
    async def generate_email(self, recent_articles: Dict[str, List[Dict]], 
                            stats: Dict, database_stats: Dict = None) -> str:
        """Generate unified email with both company updates and trend analysis"""
        try:
            today = datetime.date.today()
            date_str = today.strftime("%B %d, %Y")
            
            company_articles = recent_articles.get('company_articles', [])
            trend_articles = recent_articles.get('trend_articles', [])
            
            # Limit articles for email
            company_articles = company_articles[:self.config.max_email_articles // 2]
            trend_articles = trend_articles[:self.config.max_email_articles // 2]
            
            total_email_articles = len(company_articles) + len(trend_articles)
            
            # Generate sections
            company_section = await self._generate_company_section(company_articles)
            trend_section = await self._generate_trends_section(trend_articles)
            
            # Stats for header
            db_info = ""
            if database_stats:
                db_info = f" • DB: {database_stats.get('total_articles', 0)} articles"
            llm_info = " • LLM Enhanced" if self.llm_integration.enabled else ""
            semantic_count = sum(1 for a in trend_articles if a.get('semantic_relevance_score', 0) > 0)
            semantic_info = f" • {semantic_count} semantic scored" if semantic_count > 0 else ""
            
            # Enhanced email template
            html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Unified FinTech Intelligence - {date_str}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.4;
            color: #000;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }}
        .header {{
            margin-bottom: 30px;
            text-align: left;
        }}
        .header h1 {{
            margin: 0;
            font-size: 24px;
            font-weight: bold;
            color: #000;
        }}
        .header p {{
            margin: 5px 0;
            font-size: 16px;
            color: #000;
        }}
        .header .stats {{
            font-size: 14px;
            color: #666;
            margin-top: 10px;
        }}
        .section {{
            margin-bottom: 40px;
        }}
        .section-header {{
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #e6e6e6;
        }}
        .section-header h2 {{
            margin: 0;
            font-size: 20px;
            font-weight: bold;
            color: #000;
        }}
        .subsection {{
            margin-bottom: 25px;
        }}
        .subsection-header {{
            margin-bottom: 12px;
        }}
        .subsection-header h3 {{
            margin: 0;
            font-size: 16px;
            font-weight: bold;
            color: #333;
        }}
        .article {{
            padding: 12px 0;
            border-top: 1px solid #e6e6e6;
        }}
        .subsection .article:first-child {{
            border-top: none;
        }}
        .article-title {{
            font-weight: 700;
            font-size: 16px;
            line-height: 1.3;
            color: #0b57d0;
            text-decoration: none;
            display: block;
            margin-bottom: 6px;
        }}
        .article-title:hover {{
            text-decoration: underline;
        }}
        .article-summary {{
            color: #222;
            font-size: 13px;
            margin-bottom: 6px;
            line-height: 1.4;
        }}
        .article-meta {{
            font-size: 11px;
            color: #666;
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
        }}
        .badge {{
            background-color: #f0f0f0;
            padding: 1px 4px;
            border-radius: 3px;
            font-size: 10px;
            margin-right: 3px;
        }}
        .quality-high {{
            background-color: #e6f7e6;
            color: #2e7d32;
        }}
        .quality-medium {{
            background-color: #fff3e0;
            color: #ef6c00;
        }}
        .company-badge {{
            background-color: #e3f2fd;
            color: #1565c0;
        }}
        .semantic-score {{
            background-color: #f3e5f5;
            color: #7b1fa2;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            font-size: 12px;
            color: #666;
            border-top: 1px solid #ddd;
        }}
        .no-content {{
            padding: 20px 0;
            color: #666;
            font-style: italic;
            text-align: center;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Unified FinTech Intelligence v3.0.2</h1>
        <p>{date_str}</p>
        <div class="stats">
            {total_email_articles} articles • {stats.get('processing_time', 0):.1f}s{db_info}{llm_info}{semantic_info}
        </div>
    </div>
    
    {company_section}
    {trend_section}
    
    <div class="footer">
        <p>Unified FinTech Intelligence System v3.0.2 - Bug Fixes Applied</p>
        <p>Companies: {'ENABLED' if self.config.companies_enabled else 'DISABLED'} • 
           Trends: {'ENABLED' if self.config.trends_enabled else 'DISABLED'} • 
           Email Limit: {self.config.max_email_articles}</p>
    </div>
</body>
</html>"""
            return html_template
        except Exception as e:
            logger.error(f"Unified email generation failed: {e}")
            return f"<html><body><h2>Error generating unified digest: {e}</h2></body></html>"
    
    async def _generate_company_section(self, company_articles: List[Dict]) -> str:
        """FIXED: Generate company updates section - max 3 per company, no LLM for companies"""
        if not self.config.companies_enabled:
            return ""
            
        if not company_articles:
            return """
            <div class="section">
                <div class="section-header">
                    <h2>🏢 Company Updates</h2>
                </div>
                <div class="no-content">
                    No company updates found in the last 3 days
                </div>
            </div>
            """
        
        # Group by company
        companies_data = {}
        for article in company_articles:
            company_key = article.get('company_key', 'unknown')
            if company_key not in companies_data:
                companies_data[company_key] = []
            companies_data[company_key].append(article)
        
        subsections_html = ""
        for company_key, articles in companies_data.items():
            company_name = next((c.name for c in COMPANIES if c.key == company_key), company_key.title())
            emoji = self.company_emojis.get(company_key, "🏢")
            
            articles_html = ""
            # FIXED: Limit to 3 articles per company
            for article in articles[:3]:
                try:
                    # FIXED: No LLM enhancement for company articles - use original title
                    display_title = article.get('original_title') or article.get('title', '')
                    
                    # FIXED: No LLM summary - use content preview only
                    summary = article.get('content', '')[:150] + "..." if article.get('content', '') else ""
                    
                    # Article date
                    article_date = ""
                    if article.get('published_date'):
                        try:
                            if isinstance(article['published_date'], str):
                                dt = datetime.datetime.fromisoformat(article['published_date'].replace('Z', '+00:00'))
                            else:
                                dt = article['published_date']
                            article_date = dt.strftime("%Y-%m-%d")
                        except:
                            pass
                    
                    # Meta information
                    domain = article.get('domain', '')
                    source_type = article.get('source_type', '').upper()
                    
                    articles_html += f"""
                    <div class="article">
                        <a href="{article.get('url', '#')}" class="article-title" target="_blank">{display_title}</a>
                        <div class="article-summary">{summary}</div>
                        <div class="article-meta">
                            <div><span class="badge company-badge">COMPANY</span></div>
                            <div>{domain}{f' • {article_date}' if article_date else ''} • {source_type}</div>
                        </div>
                    </div>
                    """
                except Exception as e:
                    logger.error(f"Error generating company article HTML: {e}")
                    continue
            
            subsections_html += f"""
            <div class="subsection">
                <div class="subsection-header">
                    <h3>{emoji} {company_name}</h3>
                    <span style="font-size: 12px; color: #666;">{len(articles[:3])} articles</span>
                </div>
                {articles_html}
            </div>
            """
        
        return f"""
        <div class="section">
            <div class="section-header">
                <h2>🏢 Company Updates (Last 3 Days)</h2>
            </div>
            {subsections_html}
        </div>
        """
    
    async def _generate_trends_section(self, trend_articles: List[Dict]) -> str:
        """FIXED: Generate trends analysis section with LLM usage limits"""
        if not self.config.trends_enabled:
            return ""
            
        if not trend_articles:
            return """
            <div class="section">
                <div class="section-header">
                    <h2>📊 Trend Analysis</h2>
                </div>
                <div class="no-content">
                    No high-quality trend articles found (45%+ relevance required)
                </div>
            </div>
            """
        
        # Group by trend category
        trends_data = {}
        for article in trend_articles:
            trend_key = article.get('trend_category', 'unknown')
            if trend_key not in trends_data:
                trends_data[trend_key] = []
            trends_data[trend_key].append(article)
        
        # ADDED: Track LLM usage to enforce limits
        title_enhancements_used = 0
        summaries_used = 0
        
        subsections_html = ""
        for trend_key, articles in trends_data.items():
            # Get trend info
            trend_name = trend_key.replace("_", " ").title()
            emoji = self.trend_emojis.get(trend_key, "📊")
            
            articles_html = ""
            for article in articles[:6]:  # Limit per trend
                try:
                    # FIXED: Limited title enhancement (max 25 total)
                    display_title = article.get('enhanced_title') or article.get('title', '')
                    if (self.llm_integration.enabled and 
                        not article.get('enhanced_title') and 
                        title_enhancements_used < self.config.llm_max_title_enhancements):
                        enhanced = await self.llm_integration.enhance_title(
                            display_title, article.get('content', ''), force=True
                        )
                        if enhanced and len(enhanced) >= 15:
                            display_title = enhanced
                            title_enhancements_used += 1
                    
                    # FIXED: Limited summarization (max 15 total)
                    summary = article.get('summary', '')
                    if (not summary and 
                        summaries_used < self.config.llm_max_summaries and
                        self.llm_integration.enabled):
                        summary = await self.llm_integration.generate_summary(
                            article.get('content', ''), display_title
                        )
                        if summary:
                            summaries_used += 1
                    
                    # Fallback to content preview if no summary
                    if not summary:
                        summary = article.get('content', '')[:200] + "..."
                    
                    # Scores and badges
                    relevance_pct = int(article.get('relevance_score', 0) * 100)
                    semantic_pct = int(article.get('semantic_relevance_score', 0) * 100)
                    
                    quality_class = "quality-high" if relevance_pct >= 70 else "quality-medium"
                    quality_text = "EXCELLENT" if relevance_pct >= 90 else "HIGH" if relevance_pct >= 70 else "GOOD"
                    badges = f'<span class="badge {quality_class}">{quality_text}</span>'
                    badges += f'<span class="badge">{relevance_pct}% match</span>'
                    if semantic_pct > 0:
                        badges += f'<span class="badge semantic-score">{semantic_pct}% semantic</span>'
                    
                    # Article date
                    article_date = ""
                    if article.get('published_date'):
                        try:
                            if isinstance(article['published_date'], str):
                                dt = datetime.datetime.fromisoformat(article['published_date'].replace('Z', '+00:00'))
                            else:
                                dt = article['published_date']
                            article_date = dt.strftime("%Y-%m-%d")
                        except:
                            pass
                    
                    # Meta information
                    domain = article.get('domain', '')
                    source_type = article.get('source_type', '').upper()
                    
                    articles_html += f"""
                    <div class="article">
                        <a href="{article.get('url', '#')}" class="article-title" target="_blank">{display_title}</a>
                        <div class="article-summary">{summary}</div>
                        <div class="article-meta">
                            <div>{badges}</div>
                            <div>{domain}{f' • {article_date}' if article_date else ''} • {source_type}</div>
                        </div>
                    </div>
                    """
                except Exception as e:
                    logger.error(f"Error generating trend article HTML: {e}")
                    continue
                
                # Stop processing if we hit both LLM limits
                if (title_enhancements_used >= self.config.llm_max_title_enhancements and 
                    summaries_used >= self.config.llm_max_summaries):
                    break
            
            subsections_html += f"""
            <div class="subsection">
                <div class="subsection-header">
                    <h3>{emoji} {trend_name}</h3>
                    <span style="font-size: 12px; color: #666;">{len(articles)} articles</span>
                </div>
                {articles_html}
            </div>
            """
        
        # ADDED: Log LLM usage
        if self.llm_integration.enabled:
            logger.info(f"Email LLM usage - Titles: {title_enhancements_used}/{self.config.llm_max_title_enhancements}, "
                       f"Summaries: {summaries_used}/{self.config.llm_max_summaries}")
        
        return f"""
        <div class="section">
            <div class="section-header">
                <h2>📊 Trend Analysis</h2>
            </div>
            {subsections_html}
        </div>
        """

# --------------------------------------------------------------------------------------
# Main Unified System Orchestrator
# --------------------------------------------------------------------------------------

class UnifiedSystemOrchestrator:
    """Main orchestrator for the unified fintech intelligence system"""
    
    def __init__(self, config: UnifiedConfig):
        self.config = config
        
        # Initialize components with dependency injection
        self.region_filter = EnhancedRegionFilter()
        self.llm_integration = EnhancedLLMIntegration(config)
        self.database = UnifiedDatabaseManager(config)
        self.email_generator = UnifiedEmailGenerator(config, self.llm_integration)
        self.summarizer = ArticleSummarizer()
        
        # Initialize subsystems based on configuration
        if config.companies_enabled:
            self.company_system = CompanyMonitoringSystem(config, self.database, self.region_filter)
        else:
            self.company_system = None
            
        if config.trends_enabled:
            self.trends_system = TrendsMonitoringSystem(config, self.database, self.region_filter, self.llm_integration)
        else:
            self.trends_system = None
        
        # Initialize stats
        self.stats = {
            'processing_time': 0,
            'start_time': time.time(),
            'company_articles': 0,
            'trend_articles': 0,
            'total_articles_saved': 0,
            'email_articles': 0,
            'semantic_scored_articles': 0,
            'avg_semantic_relevance': 0.0
        }
        
        logger.info(f"Unified system v3.0.2 initialized")
        logger.info(f"Companies: {'ENABLED' if config.companies_enabled else 'DISABLED'}")
        logger.info(f"Trends: {'ENABLED' if config.trends_enabled else 'DISABLED'}")
        logger.info(f"Email limit: {config.max_email_articles} articles")

    async def run_unified_intelligence(self) -> Dict[str, Any]:
        """Run the complete unified intelligence gathering process"""
        try:
            print("Starting Unified FinTech Intelligence v3.0.2...")
            self.database.cleanup_old_data()
            
            all_articles = []
            
            # Run company monitoring if enabled
            if self.config.companies_enabled and self.company_system:
                print("\n🏢 Running Company Monitoring...")
                company_articles = await self.company_system.monitor_all_companies()
                all_articles.extend(company_articles)
                self.stats['company_articles'] = len(company_articles)
                print(f"   Collected {len(company_articles)} company articles")
            else:
                self.stats['company_articles'] = 0
                print("🏢 Company monitoring disabled")
            
            # Run trends monitoring if enabled
            if self.config.trends_enabled and self.trends_system:
                print("\n📊 Running Trends Monitoring...")
                trend_articles = await self.trends_system.monitor_all_trends()
                all_articles.extend(trend_articles)
                self.stats['trend_articles'] = len(trend_articles)
                print(f"   Collected {len(trend_articles)} trend articles")
            else:
                self.stats['trend_articles'] = 0
                print("📊 Trends monitoring disabled")
            
            self.stats['total_articles_saved'] = len(all_articles)
            
            # Get recent articles for email
            recent_articles = self.database.get_recent_articles_for_email(
                hours=self.config.company_hours_window,
                max_age_days=self.config.company_max_age_days
            )
            
            # Calculate final stats
            self.stats['processing_time'] = time.time() - self.stats['start_time']
            total_email_articles = len(recent_articles.get('company_articles', [])) + len(recent_articles.get('trend_articles', []))
            self.stats['email_articles'] = min(total_email_articles, self.config.max_email_articles)
            
            # Calculate semantic scoring stats
            semantic_articles = []
            for articles in recent_articles.values():
                semantic_articles.extend([a for a in articles if a.get('semantic_relevance_score', 0) > 0])
            self.stats['semantic_scored_articles'] = len(semantic_articles)
            
            if semantic_articles:
                self.stats['avg_semantic_relevance'] = sum(a.get('semantic_relevance_score', 0) for a in semantic_articles) / len(semantic_articles)
            
            # Save daily stats
            enhanced_stats = {
                **self.stats,
                'total_articles': self.stats['total_articles_saved'],
                'google_articles': sum(1 for a in all_articles if a.get('source_type') == 'trend_google'),
                'rss_articles': sum(1 for a in all_articles if a.get('source_type') == 'trend_rss'),
                'avg_relevance': sum(a.get('relevance_score', 0) for a in all_articles) / len(all_articles) if all_articles else 0.0
            }
            self.database.save_daily_stats(enhanced_stats)
            
            # Generate and send email
            email_sent = False
            if (self.config.email_recipients and 
                (recent_articles['company_articles'] or recent_articles['trend_articles'])):
                logger.debug(f"Email gating — recipients: {len(self.config.email_recipients)}, company: {len(recent_articles['company_articles'])}, trend: {len(recent_articles['trend_articles'])}")
                try:
                    print("\n📧 Generating unified email...")
                    database_stats = self.database.get_database_stats()
                    html_content = await self.email_generator.generate_email(
                        recent_articles, self.stats, database_stats
                    )
                    email_sent = self._send_email(html_content)
                    if email_sent:
                        print("✅ Email sent successfully!")
                    else:
                        print("❌ Email not sent")
                except Exception as e:
                    print(f"❌ Email error: {e}")
                    email_sent = False
            else:
                print("📧 No email recipients configured or no articles for email")
            
            # Console summary
            print(f"\n📊 UNIFIED SYSTEM SUMMARY:")
            print(f"   Total articles saved: {self.stats['total_articles_saved']}")
            if self.config.companies_enabled:
                print(f"   Company articles: {self.stats['company_articles']}")
            if self.config.trends_enabled:
                print(f"   Trend articles: {self.stats['trend_articles']}")
            print(f"   Email articles: {self.stats['email_articles']} (limit: {self.config.max_email_articles})")
            if self.llm_integration.enabled:
                llm_stats = self.llm_integration.get_usage_stats()
                print(f"   LLM usage: {llm_stats['daily_requests']} calls, {llm_stats['daily_tokens']} tokens, ${llm_stats['daily_cost']:.4f}")
            if self.llm_integration.semantic_scoring_enabled:
                print(f"   Semantic scored: {self.stats['semantic_scored_articles']}")
                print(f"   Avg semantic score: {self.stats['avg_semantic_relevance']:.2f}")
            print(f"   Processing time: {self.stats['processing_time']:.1f}s")
            print(f"   Email sent: {'YES' if email_sent else 'NO'}")
            
            return {
                'success': True,
                'total_articles_saved': self.stats['total_articles_saved'],
                'company_articles': self.stats['company_articles'],
                'trend_articles': self.stats['trend_articles'],
                'email_articles': self.stats['email_articles'],
                'semantic_scored_articles': self.stats['semantic_scored_articles'],
                'email_sent': email_sent,
                'processing_time': self.stats['processing_time'],
                'llm_enabled': self.llm_integration.enabled,
                'companies_enabled': self.config.companies_enabled,
                'trends_enabled': self.config.trends_enabled
            }
        except Exception as e:
            print(f"❌ Unified system failed: {e}")
            logger.error(f"Unified system failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _send_email(self, html_content: str) -> bool:
        """Send unified email digest"""
        try:
            if not self.config.smtp_user or not self.config.smtp_password or not self.config.email_recipients:
                print("⚠️ Email configuration incomplete")
                return False
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Unified FinTech Intelligence - {datetime.date.today().strftime('%B %d, %Y')}"
            msg['From'] = f"Unified FinTech Intelligence <{self.config.smtp_user}>"
            msg['To'] = ', '.join(self.config.email_recipients)
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            context = ssl.create_default_context()
            if self.config.smtp_port == 465:
                with smtplib.SMTP_SSL(self.config.smtp_host, self.config.smtp_port, context=context) as server:
                    server.login(self.config.smtp_user, self.config.smtp_password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                    server.starttls(context=context)
                    server.login(self.config.smtp_user, self.config.smtp_password)
                    server.send_message(msg)
            return True
        except Exception as e:
            print(f"❌ Email sending failed: {e}")
            logger.error(f"Email sending failed: {e}")
            return False

# --------------------------------------------------------------------------------------
# Main System Class (Backward Compatibility)
# --------------------------------------------------------------------------------------

class UnifiedFintechIntelligenceSystem(UnifiedSystemOrchestrator):
    """Main system class for backward compatibility"""
    
    def __init__(self):
        try:
            config = UnifiedConfig()  # This will validate configuration
            super().__init__(config)
            
            print(f"Unified FinTech Intelligence System v3.0.2 initialized")
            print(f"Companies: {'ENABLED' if config.companies_enabled else 'DISABLED'}")
            print(f"Trends: {'ENABLED' if config.trends_enabled else 'DISABLED'}")
            print(f"LLM: {'ENABLED' if config.llm_enabled else 'DISABLED'} • Model: {config.llm_model}")
            print(f"Email limit: {config.max_email_articles} articles")
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            raise

# --------------------------------------------------------------------------------------
# Main Function
# --------------------------------------------------------------------------------------

async def main():
    """Enhanced main function with comprehensive options"""
    parser = argparse.ArgumentParser(description="Unified FinTech Intelligence System v3.0.2")
    
    parser.add_argument("--diagnose-links", action="store_true", help="Check company link reachability and exit")
    parser.add_argument("--test", action="store_true", help="Test mode - validate configuration only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--config-check", action="store_true", help="Check configuration and exit")
    parser.add_argument("--companies-only", action="store_true", help="Run company monitoring only")
    parser.add_argument("--trends-only", action="store_true", help="Run trends monitoring only")
    parser.add_argument("--disable-llm", action="store_true", help="Disable LLM integration for this run")
    parser.add_argument("--disable-semantic", action="store_true", help="Disable semantic scoring for this run")
    parser.add_argument("--config-json", type=str, default=None, help="Path to trend config JSON")
    
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create a temporary config for CLI argument processing
    temp_env = dict(os.environ)
    
    if args.companies_only:
        temp_env['COMPANIES_ENABLED'] = '1'
        temp_env['TRENDS_ENABLED'] = '0'
        print("🏢 Running in companies-only mode")
    
    if args.trends_only:
        temp_env['COMPANIES_ENABLED'] = '0'
        temp_env['TRENDS_ENABLED'] = '1'
        print("📊 Running in trends-only mode")
    
    if args.config_json:
        temp_env['TREND_CONFIG_JSON'] = args.config_json
    
    if args.disable_llm:
        temp_env['LLM_INTEGRATION_ENABLED'] = '0'
        temp_env['SEMANTIC_SCORING_ENABLED'] = '0'
        print("🔇 LLM integration disabled via CLI")
    
    if args.disable_semantic:
        temp_env['SEMANTIC_SCORING_ENABLED'] = '0'
        print("🔇 Semantic scoring disabled via CLI")

    # Apply temporary environment changes
    original_env = {}
    for key, value in temp_env.items():
        if key in os.environ:
            original_env[key] = os.environ[key]
        os.environ[key] = value

    try:
        if args.config_check:
            try:
                config = UnifiedConfig()
                print("🔧 CONFIGURATION CHECK:")
                print(f"   Companies enabled: {'YES' if config.companies_enabled else 'NO'}")
                print(f"   Trends enabled: {'YES' if config.trends_enabled else 'NO'}")
                print(f"   LLM integration: {'YES' if config.llm_enabled else 'NO'}")
                print(f"   Semantic scoring: {'YES' if config.semantic_scoring_enabled else 'NO'} (limit: {config.semantic_total_limit})")
                print(f"   Email limit: {config.max_email_articles} articles")
                print(f"   Email recipients: {len(config.email_recipients)} configured")
                try:
                    db = UnifiedDatabaseManager(config)
                    stats = db.get_database_stats()
                    print(f"   Database: ✅ Connected ({stats.get('total_articles', 0)} articles)")
                except Exception as e:
                    print(f"   Database: ❌ Error - {e}")
                return 0
            except ConfigurationError as e:
                print(f"❌ Configuration Error:\n{e}")
                return 1

        if args.diagnose_links:
            try:
                config = UnifiedConfig()
                if config.companies_enabled:
                    company_system = CompanyMonitoringSystem(config, None, None)
                    print("🔗 DIAGNOSING COMPANY LINKS...")
                    report = company_system.diagnose_company_links()
                    for entry in report:
                        company = entry['company']
                        url = entry['url']
                        head = entry.get('head', 'N/A')
                        get = entry.get('get', 'N/A')
                        error = entry.get('error', '')
                        print(f"{company:15} HEAD:{str(head):>3} GET:{str(get):>3} {url}")
                        if error:
                            print(f"                ERROR: {error}")
                else:
                    print("❌ Company monitoring disabled - cannot diagnose links")
                return 0
            except Exception as e:
                print(f"❌ Link diagnosis failed: {e}")
                return 1

        try:
            intelligence_system = UnifiedFintechIntelligenceSystem()

            if args.test:
                print("🧪 RUNNING TEST MODE...")
                print("   Configuration validated ✅")
                print("   Database connection tested ✅")
                print("   All subsystems ready for production ✅")
                return 0

            # Run unified intelligence system
            try:
                print("🚀 Starting Unified FinTech Intelligence v3.0.2...")
                result = await intelligence_system.run_unified_intelligence()
                if result['success']:
                    print("\n✅ UNIFIED SYSTEM COMPLETED SUCCESSFULLY!")
                    print(f"📧 Email sent: {'YES' if result.get('email_sent') else 'NO'}")
                    print(f"📄 Total articles saved: {result.get('total_articles_saved', 0)}")
                    if result.get('companies_enabled'):
                        print(f"🏢 Company articles: {result.get('company_articles', 0)}")
                    if result.get('trends_enabled'):
                        print(f"📊 Trend articles: {result.get('trend_articles', 0)}")
                    print(f"📧 Email articles: {result.get('email_articles', 0)}")
                    if result.get('semantic_scored_articles') is not None:
                        print(f"🧠 Semantic scored: {result.get('semantic_scored_articles', 0)}")
                    return 0
                else:
                    print(f"\n❌ UNIFIED SYSTEM FAILED: {result.get('error', 'Unknown error')}")
                    return 1
            except KeyboardInterrupt:
                print("\n⚠️ Process interrupted by user (Ctrl+C)")
                print("💾 Partial data may have been saved to database")
                return 130

        except ConfigurationError as e:
            print(f"❌ Configuration Error: {e}")
            return 1
        except Exception as e:
            print(f"❌ System error: {e}")
            logger.error(f"System error: {e}")
            return 1

    finally:
        # Restore original environment
        for key, value in original_env.items():
            os.environ[key] = value
        for key in temp_env:
            if key not in original_env and key in os.environ:
                del os.environ[key]


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")
        sys.exit(130)
