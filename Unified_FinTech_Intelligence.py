#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UNIFIED FinTech Daily Update System v3.2.0 - Production Ready with Full LLM Integration
=======================================================================================

COMPREHENSIVE FEATURES:
- Company Updates: 10 major fintech companies with web scraping + RSS
- Trend Analysis: 10 key fintech trends with Google Search + RSS  
- Full LLM Integration: Title enhancement + summarization for BOTH companies and trends
- Advanced Email: Professional card layout with quality scoring
- Production Database: Connection pooling + robust error handling
- Region Filtering: US/EU focus, excludes Asia/Africa
- Semantic Scoring: LLM-powered relevance evaluation

COMPANIES MONITORED:
Visa, Mastercard, PayPal, Stripe, Affirm, Toast, Adyen, Fiserv, FIS, Global Payments

TRENDS MONITORED:  
Instant Payments, A2A/Open Banking, Stablecoins, SoftPOS, Cross-Border, BNPL,
Payment Orchestration, Fraud AI, PCI Security, Digital Wallets
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
from urllib.parse import urlparse, urljoin
from contextlib import contextmanager
from functools import wraps
import random

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
    from urllib3.util.retry import Retry
    import requests
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install aiohttp feedparser beautifulsoup4 scikit-learn numpy python-dotenv requests urllib3 python-dateutil")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure logging
DEBUG_MODE = os.getenv("DEBUG_MODE", "0") == "1"
log_level = logging.DEBUG if DEBUG_MODE else getattr(logging, os.getenv("LOG_LEVEL", "INFO"))

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_fintech_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# FIXED: Timezone-aware datetime utilities
# --------------------------------------------------------------------------------------

def utc_now() -> datetime.datetime:
    """Get current UTC time (timezone-aware)"""
    return datetime.datetime.now(datetime.timezone.utc)

def ensure_utc(dt: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    """Ensure datetime is timezone-aware UTC"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        return dt.replace(tzinfo=datetime.timezone.utc)
    else:
        # Convert to UTC
        return dt.astimezone(datetime.timezone.utc)

def utc_date_filter(days: int) -> datetime.datetime:
    """Get UTC cutoff date for filtering (timezone-aware)"""
    return utc_now() - datetime.timedelta(days=days)

# --------------------------------------------------------------------------------------
# Custom Exceptions
# --------------------------------------------------------------------------------------

class DigestSystemError(Exception):
    """Base exception for digest system errors"""
    pass

class ConfigurationError(DigestSystemError):
    """Configuration validation errors"""
    pass

class DatabaseError(DigestSystemError):
    """Database operation errors"""
    pass

class ContentProcessingError(DigestSystemError):
    """Content processing errors"""
    pass

class APIError(DigestSystemError):
    """External API errors"""
    pass

# --------------------------------------------------------------------------------------
# Enhanced Configuration Management with Validation
# --------------------------------------------------------------------------------------

class ConfigValidator:
    """Validates configuration on startup"""
    
    @staticmethod
    def validate(config: 'UnifiedConfig') -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Required API keys for trends
        if config.trends_enabled and not config.google_api_key:
            errors.append("GOOGLE_SEARCH_API_KEY required when trends enabled")
        
        if config.trends_enabled and not config.google_search_engine_id:
            errors.append("GOOGLE_SEARCH_ENGINE_ID required when trends enabled")
        
        # LLM configuration
        if config.llm_enabled and not config.llm_api_key:
            errors.append("LLM_API_KEY required when LLM_INTEGRATION_ENABLED=1")
        
        # Email configuration
        if config.email_recipients and not config.smtp_user:
            errors.append("SMTP_USER required when EMAIL_RECIPIENTS is set")
        
        # Validate numeric ranges
        if not 0.0 <= config.relevance_threshold <= 1.0:
            errors.append("RELEVANCE_THRESHOLD must be between 0.0 and 1.0")
        
        if not 0.0 <= config.llm_temperature <= 2.0:
            errors.append("LLM_TEMPERATURE must be between 0.0 and 2.0")
        
        if config.max_age_days < 1:
            errors.append("MAX_AGE_DAYS must be at least 1")
        
        # Validate new limits
        if not 10 <= config.max_email_articles <= 50:
            errors.append("MAX_EMAIL_ARTICLES must be between 10 and 50")
        
        if not 20 <= config.semantic_total_limit <= 200:
            errors.append("SEMANTIC_TOTAL_LIMIT must be between 20 and 200")
        
        return errors

class UnifiedConfig:
    """Unified configuration for both company and trend monitoring with validation"""
    
    def __init__(self):
        # Core settings
        self.max_age_days = self._safe_int("MAX_AGE_DAYS", 7)  # 7-day filter
        self.max_email_articles = self._safe_int("MAX_EMAIL_ARTICLES", 30)
        self.relevance_threshold = float(os.getenv("RELEVANCE_THRESHOLD", "0.55"))
        
        # Company monitoring settings
        self.companies_enabled = os.getenv("COMPANIES_ENABLED", "1") == "1"
        self.bypass_robots_txt = os.getenv("BYPASS_ROBOTS_TXT", "1") == "1"
        self.aggressive_scraping = os.getenv("AGGRESSIVE_SCRAPING", "1") == "1"
        self.company_max_items_per_source = self._safe_int("COMPANY_MAX_ITEMS_PER_SOURCE", 20)
        
        # Trend monitoring settings
        self.trends_enabled = os.getenv("TRENDS_ENABLED", "1") == "1"
        self.semantic_scoring_enabled = os.getenv("SEMANTIC_SCORING_ENABLED", "1") == "1"
        self.keyword_expansion_enabled = os.getenv("KEYWORD_EXPANSION_ENABLED", "1") == "1"
        self.semantic_total_limit = self._safe_int("SEMANTIC_TOTAL_LIMIT", 40)
        self.keep_historical_days = self._safe_int("KEEP_HISTORICAL_DAYS", 365)
        
        # LLM integration - ENABLED FOR BOTH COMPANIES AND TRENDS
        self.llm_enabled = os.getenv("LLM_INTEGRATION_ENABLED", "1") == "1"
        self.llm_api_key = os.getenv("LLM_API_KEY", "").strip()
        self.llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini").strip()
        self.llm_temperature = float(os.getenv("LLM_TEMPERATURE", "0.1"))
        self.llm_max_tokens_title = self._safe_int("LLM_MAX_TOKENS_TITLE", 32)
        self.llm_max_tokens_summary = self._safe_int("LLM_MAX_TOKENS_SUMMARY", 120)
        self.llm_summary_fallback_enabled = os.getenv("LLM_SUMMARY_FALLBACK_ENABLED", "1") == "1"
        self.llm_summary_min_chars_trigger = self._safe_int("LLM_SUMMARY_MIN_CHARS_TRIGGER", 60)
        self.llm_summary_long_content_trigger = self._safe_int("LLM_SUMMARY_LONG_CONTENT_TRIGGER", 3000)
        
        # Google Search API
        self.google_api_key = os.getenv('GOOGLE_SEARCH_API_KEY', '')
        self.google_search_engine_id = os.getenv('GOOGLE_SEARCH_ENGINE_ID', '')
        self.google_daily_limit = self._safe_int('GOOGLE_SEARCH_DAILY_LIMIT', 100)
        
        # Database and email
        self.database_path = os.getenv('DATABASE_PATH', 'unified_fintech_update.db')
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = self._safe_int('SMTP_PORT', 587)
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.email_recipients = [r.strip() for r in os.getenv('EMAIL_RECIPIENTS', '').split(',') if r.strip()]
        
        # Region filtering
        self.strict_region_filter = os.getenv("STRICT_REGION_FILTER", "1") == "1"
        
        # Validate configuration
        self._validate()
        
        logger.info(f"Unified config loaded - Age limit: {self.max_age_days} days")
        logger.info(f"Companies: {'ENABLED' if self.companies_enabled else 'DISABLED'}")
        logger.info(f"Trends: {'ENABLED' if self.trends_enabled else 'DISABLED'}")
        logger.info(f"LLM: {'ENABLED' if self.llm_enabled else 'DISABLED'} • Model: {self.llm_model}")
        logger.info(f"Semantic scoring: {'ENABLED' if self.semantic_scoring_enabled else 'DISABLED'} (limit: {self.semantic_total_limit})")
    
    def _safe_int(self, env_var: str, default: int) -> int:
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
            "amex.com", "stripe.com", "adyen.com", "checkout.com", "squareup.com", 
            "wise.com", "revolut.com", "paypal.com", "klarna.com", "affirm.com",
            "n26.com", "monzo.com", "starling.com", "plaid.com", "toasttab.com",
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
# Database Manager with Connection Pooling
# --------------------------------------------------------------------------------------

class DatabaseConnectionPool:
    """Fixed connection pool for SQLite database"""
    
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
    """Enhanced SQLite database with connection pooling for unified system"""
    
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
            logger.info(f"Database initialized: {self.db_path}")
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
    
    def _create_tables(self):
        with self._get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    original_title TEXT,
                    enhanced_title TEXT,
                    content TEXT,
                    summary TEXT,
                    source_type TEXT,  -- 'company' or 'trend'
                    category TEXT,     -- company key or trend key
                    domain TEXT,
                    relevance_score REAL,
                    semantic_relevance_score REAL DEFAULT 0.0,
                    quality_score REAL,
                    region_confidence REAL,
                    word_count INTEGER,
                    content_hash TEXT,
                    published_date DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    search_keywords TEXT
                )
            ''')
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
            # Add columns if they don't exist
            try:
                conn.execute('ALTER TABLE articles ADD COLUMN semantic_relevance_score REAL DEFAULT 0.0')
            except sqlite3.OperationalError:
                pass
    
    def _create_indexes(self):
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_articles_source_type ON articles(source_type)',
            'CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category)',
            'CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at)',
            'CREATE INDEX IF NOT EXISTS idx_articles_relevance ON articles(relevance_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_semantic_relevance ON articles(semantic_relevance_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_quality ON articles(quality_score)',
            'CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain)',
            'CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(content_hash)',
            'CREATE INDEX IF NOT EXISTS idx_daily_stats_date ON daily_stats(date)',
        ]
        with self._get_connection() as conn:
            for index_sql in indexes:
                try:
                    conn.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Index creation failed: {e}")
    
    def save_article(self, article: 'ContentSource') -> bool:
        try:
            keywords_json = json.dumps(article.search_keywords) if article.search_keywords else "[]"
            
            # FIXED: Ensure timezone-aware datetime for database storage
            published_date_str = None
            if article.published_date:
                utc_date = ensure_utc(article.published_date)
                published_date_str = utc_date.isoformat() if utc_date else None
            
            with self._get_connection() as conn:
                conn.execute('SELECT 1').fetchone()
                
                conn.execute('''
                    INSERT OR REPLACE INTO articles 
                    (url, title, original_title, enhanced_title, content, summary, source_type, 
                     category, domain, relevance_score, semantic_relevance_score, quality_score, 
                     region_confidence, word_count, content_hash, published_date, search_keywords)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    article.url, article.title, article.original_title, article.enhanced_title,
                    article.content, article.summary, article.source_type, article.category,
                    article.domain, article.relevance_score, article.semantic_relevance_score,
                    article.quality_score, article.region_confidence, article.word_count,
                    article.content_hash, published_date_str, keywords_json
                ))
                
                logger.debug(f"Saved article: {article.title[:50]}...")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save article '{article.title[:50]}...': {e}")
            
            if self.use_connection_pool:
                try:
                    logger.warning("Retrying with direct connection")
                    conn = self._get_connection_direct()
                    try:
                        conn.execute('''
                            INSERT OR REPLACE INTO articles 
                            (url, title, original_title, enhanced_title, content, summary, source_type, 
                             category, domain, relevance_score, semantic_relevance_score, quality_score, 
                             region_confidence, word_count, content_hash, published_date, search_keywords)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            article.url, article.title, article.original_title, article.enhanced_title,
                            article.content, article.summary, article.source_type, article.category,
                            article.domain, article.relevance_score, article.semantic_relevance_score,
                            article.quality_score, article.region_confidence, article.word_count,
                            article.content_hash, published_date_str, keywords_json
                        ))
                        logger.info("Fallback save successful")
                        return True
                    finally:
                        conn.close()
                except Exception as e2:
                    logger.error(f"Fallback save also failed: {e2}")
                    
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
                    today, stats.get('total_articles', 0), stats.get('company_articles', 0),
                    stats.get('trend_articles', 0), stats.get('google_articles', 0),
                    stats.get('rss_articles', 0), stats.get('avg_relevance', 0.0),
                    stats.get('avg_semantic_relevance', 0.0), stats.get('processing_time', 0.0), 
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
                hashes = conn.execute('''
                    SELECT content_hash FROM articles 
                    WHERE created_at >= ? AND content_hash IS NOT NULL
                ''', (cutoff_date,)).fetchall()
            return {h[0] for h in hashes}
        except Exception as e:
            logger.error(f"Failed to get recent hashes: {e}")
            return set()
    
    def cleanup_old_data(self):
        try:
            cutoff_date = datetime.date.today() - datetime.timedelta(days=self.keep_days)
            with self._get_connection() as conn:
                result = conn.execute('DELETE FROM articles WHERE created_at < ?', (cutoff_date,))
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
                stats['company_articles'] = conn.execute("SELECT COUNT(*) FROM articles WHERE source_type = 'company'").fetchone()[0]
                stats['trend_articles'] = conn.execute("SELECT COUNT(*) FROM articles WHERE source_type = 'trend'").fetchone()[0]
                week_ago = datetime.date.today() - datetime.timedelta(days=7)
                stats['recent_articles'] = conn.execute('''
                    SELECT COUNT(*) FROM articles WHERE created_at >= ?
                ''', (week_ago,)).fetchone()[0]
                stats['high_quality_articles'] = conn.execute('''
                    SELECT COUNT(*) FROM articles WHERE relevance_score >= 0.55
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
# Retry Decorator with Exponential Backoff
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
# ENHANCED LLM Integration with Professional Title Generation (FOR BOTH COMPANIES AND TRENDS)
# --------------------------------------------------------------------------------------

class EnhancedLLMIntegration:
    """Enhanced LLM integration with professional title generation for both companies and trends"""
    
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
        """ENHANCED: Professional title enhancement for equity research context - WORKS FOR BOTH COMPANIES AND TRENDS"""
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
                "- 65–92 characters (HARD CAP 100). AP-like style, present tense, numerals OK. No emojis, no site names, no quotes. "
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
        """Enhanced LLM summary generation - WORKS FOR BOTH COMPANIES AND TRENDS"""
        if not self.enabled or not self.config.llm_summary_fallback_enabled:
            return ""
        prompt = (
            "You are a financial analyst. Summarize for a daily payments/fintech digest in 80–120 words, "
            "2–3 sentences, neutral tone. Include one concrete detail if available ($, metric, regulation, partner, geography). "
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
# HTTP Utilities with Robots.txt Bypass
# --------------------------------------------------------------------------------------

class UserAgentRotator:
    """Rotate User-Agent strings for company scraping"""
    
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (compatible; FinancialNewsBot/1.0; +http://www.example.com/bot)",
        ]
        self.current_index = 0
    
    def get_random_user_agent(self) -> str:
        return random.choice(self.user_agents)

UA_ROTATOR = UserAgentRotator()

def create_session_with_bypass(bypass_robots: bool = True) -> requests.Session:
    """Create HTTP session with robots.txt bypass capabilities"""
    session = requests.Session()
    
    headers = {
        "User-Agent": UA_ROTATOR.get_random_user_agent() if bypass_robots else "unified-fintech-update/3.2",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    session.headers.update(headers)
    
    retry_strategy = Retry(
        total=2,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

def http_get_bypass(url: str, timeout: int = 20, bypass_robots: bool = True) -> requests.Response:
    """HTTP GET with robots.txt bypass"""
    session = create_session_with_bypass(bypass_robots)
    
    try:
        url = _q4_friendly_url(url)
        extra_headers = _domain_specific_headers(url)
        resp = session.get(url, timeout=timeout, allow_redirects=True, headers=extra_headers or None)
        resp.raise_for_status()
        return resp
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403 and not bypass_robots:
            logger.info(f"403 error without bypass, retrying with bypass: {url}")
            return http_get_bypass(url, timeout, bypass_robots=True)
        raise
    except Exception as e:
        logger.error(f"HTTP GET failed for {url}: {e}")
        raise

# --- Domain-specific helpers for Q4 IR and strict IR pages ---
def _q4_friendly_url(url: str) -> str:
    # Many Q4 IR pages serve simpler HTML when 'mobile=1' is present.
    if "default.aspx" in url and "mobile=1" not in url:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}mobile=1"
    return url

def _domain_specific_headers(url: str) -> dict:
    netloc = urlparse(url).netloc
    headers = {}
    if netloc.endswith(("pypl.com", "affirm.com", "toasttab.com")):
        headers.update({
            "Referer": f"https://{netloc}/",
            "Upgrade-Insecure-Requests": "1",
            "Accept-Language": "en-US,en;q=0.9",
        })
    return headers

# --- RSS auto-discovery ---
def autodiscover_rss(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    feeds = []
    for link in soup.select('link[rel="alternate"][type*="rss"], link[rel="alternate"][type*="atom"]'):
        href = link.get("href")
        if href:
            feeds.append(urljoin(base_url, href))
    # Q4-style fallbacks if nothing was advertised
    if not feeds and ("default.aspx" in base_url or "investors." in base_url):
        candidates = [
            "rss/press-release.xml", "rss/news.xml", "rss/events.xml",
            "rss-feeds", "rssfeeds", "rss/default.aspx"
        ]
        base = base_url.rstrip("/")
        # try prefix path up to the domain root or investor root
        root = base.split("/news")[0] if "/news" in base else base
        for c in candidates:
            feeds.append(urljoin(root + "/", c))
    # de-dupe
    seen, uniq = set(), []
    for f in feeds:
        if f not in seen:
            uniq.append(f); seen.add(f)
    return uniq

# --- JSON-LD extraction ---
def extract_articles_from_jsonld(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, str]] = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue
        blocks = data if isinstance(data, dict) else {"@graph": data}
        nodes = blocks.get("@graph", [blocks])
        for n in nodes:
            t = n.get("@type")
            if t in ("NewsArticle", "BlogPosting", "Article"):
                out.append({
                    "title": (n.get("headline") or n.get("name") or "").strip(),
                    "url": urljoin(base_url, n.get("url") or ""),
                    "summary": (n.get("description") or "")[:500].strip(),
                    "date": n.get("datePublished") or n.get("dateModified") or "",
                })
    # de-dupe by URL
    seen, uniq = set(), []
    for item in out:
        u = item.get("url", "")
        if u and u not in seen:
            uniq.append(item); seen.add(u)
    return uniq

# --- Domain fallback parsers ---
def parse_mastercard_press(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    for a in soup.select('a[href*="/press/"]'):
        href = urljoin(base_url, a.get("href") or "")
        # heuristic: deeper URLs are article pages, not category
        if href.rstrip("/").count("/") < 7:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        wrap = a.find_parent(["article","li","div"])
        date_el = (wrap.find(["time","span"]) if wrap else None)
        date_txt = ""
        if date_el:
            date_txt = date_el.get("datetime") or date_el.get_text(strip=True)
        items.append({"title": title, "url": href, "summary": "", "date": date_txt})
    return items

def parse_adyen_press(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    for a in soup.select('a[href*="/press-and-media/"]'):
        href = urljoin(base_url, a.get("href") or "")
        if not href or "/press-and-media/" not in href:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        wrap = a.find_parent(["article","li","div"])
        date_el = (wrap.find(["time","span"]) if wrap else None)
        date_txt = ""
        if date_el:
            date_txt = date_el.get("datetime") or date_el.get_text(strip=True)
        items.append({"title": title, "url": href, "summary": "", "date": date_txt})
    return items

def parse_q4_list(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []
    for a in soup.select('a[href*=\"news-release\"], a[href*=\"/news-releases/\"], a[href*=\"/news/\"]'):
        href = urljoin(base_url, a.get("href") or "")
        title = a.get_text(" ", strip=True)
        if not title:
            continue
        li = a.find_parent(["li","article","div"])
        date_el = li.find(["time","span","p"]) if li else None
        date_txt = date_el.get_text(strip=True) if date_el else ""
        items.append({"title": title, "url": href, "summary": "", "date": date_txt})
    return items


def parse_date_robust(text: str) -> Optional[datetime.datetime]:
    """FIXED: Robust date parsing with timezone-aware output"""
    if not text:
        return None
        
    text = re.sub(r'\s+', ' ', text.strip())
    
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt:
            return ensure_utc(dt)  # FIXED: Always return timezone-aware UTC
    except:
        pass
    
    patterns = [
        r'(\d{4}-\d{2}-\d{2})',
        r'([A-Z][a-z]{2,8}\s+\d{1,2},?\s+\d{4})',
        r'(\d{1,2}/\d{1,2}/\d{4})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                dt = dateparser.parse(match.group(1))
                if dt:
                    return ensure_utc(dt)  # FIXED: Always return timezone-aware UTC
            except:
                continue
    
    return None

# --------------------------------------------------------------------------------------
# Data Structures
# --------------------------------------------------------------------------------------

@dataclass
class ContentSource:
    """Unified content source for both companies and trends"""
    url: str
    title: str
    content: str = ""
    summary: str = ""
    source_type: str = ""  # 'company' or 'trend'
    category: str = ""  # company key or trend key
    domain: str = ""
    published_date: Optional[datetime.datetime] = None
    relevance_score: float = 0.0
    semantic_relevance_score: float = 0.0
    quality_score: float = 0.0
    region_confidence: float = 1.0
    word_count: int = 0
    content_hash: str = ""
    search_keywords: List[str] = field(default_factory=list)
    original_title: str = ""
    enhanced_title: str = ""
    
    def __post_init__(self):
        if not self.url or not self.title:
            raise ValueError("URL and title are required")
        if not self.original_title:
            self.original_title = str(self.title).strip()
        if not self.content_hash:
            content = f"{self.title}{self.content}"
            self.content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        if not self.word_count and self.content:
            self.word_count = len(self.content.split())
        if not self.domain and self.url:
            self.domain = urlparse(self.url).netloc
        # FIXED: Ensure published_date is timezone-aware
        if self.published_date:
            self.published_date = ensure_utc(self.published_date)

@dataclass
class Company:
    """Company configuration"""
    key: str
    name: str
    pages: List[str]
    feeds: List[str]
    scrape_enabled: bool = True

@dataclass
class TrendConfig:
    """Trend configuration"""
    name: str
    keywords: List[str]
    rss_feeds: List[str] = field(default_factory=list)
    min_relevance_score: float = 0.3
    email_relevance_score: float = 0.45
    max_articles: int = 6
    google_search_enabled: bool = True
    priority: str = "medium"
    description: str = ""

# --------------------------------------------------------------------------------------
# Company and Trend Definitions
# --------------------------------------------------------------------------------------

COMPANIES: List[Company] = [
    Company("visa", "Visa",
            pages=["https://usa.visa.com/about-visa/newsroom.html",
                   "https://investor.visa.com/news/default.aspx"],
            feeds=[]),
    
    Company("mastercard", "Mastercard", 
            pages=["https://www.mastercard.com/us/en/news-and-trends/press.html",
                   "https://investor.mastercard.com/investor-news/default.aspx"],
            feeds=[]),
    
    Company("paypal", "PayPal",
            pages=["https://newsroom.paypal-corp.com/",
                   "https://investor.pypl.com/news-and-events/news/default.aspx"],
            feeds=[]),
    
    Company("stripe", "Stripe",
            pages=["https://stripe.com/newsroom",
                   "https://stripe.com/blog"],
            feeds=["https://stripe.com/blog/feed.rss"]),
    
    Company("affirm", "Affirm",
            pages=["https://investors.affirm.com/news-releases",
                   "https://investors.affirm.com/news-events/newsroom"],
            feeds=[]),
    
    Company("toast", "Toast",
            pages=["https://investors.toasttab.com/events-and-presentations/default.aspx",
                   "https://investors.toasttab.com/news/default.aspx"],
            feeds=[]),
    
    Company("adyen", "Adyen",
            pages=["https://www.adyen.com/press-and-media"],
            feeds=[]),
    
    Company("fiserv", "Fiserv",
            pages=["https://newsroom.fiserv.com/",
                   "https://investors.fiserv.com/newsroom"],
            feeds=[]),
    
    Company("fis", "FIS",
            pages=["https://www.fisglobal.com/en/about-us/media-room"],
            feeds=[]),
    
    Company("gpn", "Global Payments",
            pages=["https://investors.globalpayments.com/news-events/press-releases"],
            feeds=[]),
]

TRENDS = {
    "instant_payments": TrendConfig(
        name="Instant Payments",
        keywords=["instant payments", "real-time payments", "RTP", "FedNow", "faster payments"],
        rss_feeds=["https://www.federalreserve.gov/feeds/press_all.xml"],
    ),
    "a2a_open_banking": TrendConfig(
        name="A2A & Open Banking",
        keywords=["open banking", "PSD2", "account to account", "A2A payments", "banking APIs"],
        rss_feeds=["https://www.consumerfinance.gov/about-us/newsroom/rss/"],
    ),
    "stablecoins": TrendConfig(
        name="Stablecoins & CBDC",
        keywords=["stablecoin", "CBDC", "central bank digital currency", "digital dollar"],
        rss_feeds=["https://www.ecb.europa.eu/rss/fie.html"],
    ),
    "softpos_tap_to_pay": TrendConfig(
        name="SoftPOS & Tap to Pay",
        keywords=["SoftPOS", "tap to pay", "contactless payments", "mobile POS", "NFC payments"],
        rss_feeds=[],
    ),
    "cross_border": TrendConfig(
        name="Cross-Border Payments",
        keywords=["cross border payments", "international payments", "remittance", "global payments"],
        rss_feeds=[],
    ),
    "bnpl": TrendConfig(
        name="Buy Now, Pay Later",
        keywords=["buy now pay later", "BNPL", "installment payments", "Klarna", "Afterpay"],
        rss_feeds=[],
    ),
    "payment_orchestration": TrendConfig(
        name="Payment Orchestration",
        keywords=["payment orchestration", "smart routing", "payment optimization"],
        rss_feeds=[],
    ),
    "fraud_ai": TrendConfig(
        name="Fraud Prevention & AI",
        keywords=["fraud prevention", "AI payments", "machine learning payments", "AML"],
        rss_feeds=[],
    ),
    "pci_dss": TrendConfig(
        name="PCI DSS & Security",
        keywords=["PCI DSS", "payment security", "data security standard", "compliance"],
        rss_feeds=[],
    ),
    "wallet_nfc": TrendConfig(
        name="Digital Wallets & NFC",
        keywords=["digital wallet", "mobile wallet", "NFC payments", "Apple Pay", "Google Pay"],
        rss_feeds=[],
    ),
}

# --------------------------------------------------------------------------------------
# Keyword Expansion System
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
# Article Processing with Enhanced LLM Integration
# --------------------------------------------------------------------------------------

class ArticleSummarizer:
    """Enhanced article summarization (rule-based primary, LLM fallback)"""
    
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

class ArticleProcessor:
    """Handles article processing pipeline with enhanced LLM integration"""
    
    def __init__(self, config: UnifiedConfig, region_filter: EnhancedRegionFilter, llm_integration: EnhancedLLMIntegration):
        self.config = config
        self.region_filter = region_filter
        self.llm_integration = llm_integration
        self.summarizer = ArticleSummarizer()
    
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
    
    async def process_article(self, article: ContentSource, trend_config: Optional[TrendConfig] = None) -> ContentSource:
        """Process a single article through the complete pipeline with FULL LLM INTEGRATION"""
        try:
            # Calculate relevance for trends
            if trend_config:
                article.relevance_score = self.calculate_relevance_score(
                    article.content, article.title, trend_config.keywords
                )
            else:
                # For companies, set high relevance since they're inherently relevant
                article.relevance_score = 0.8
            
            # Update quality score
            if hasattr(article, 'region_confidence'):
                article.quality_score = (article.relevance_score + article.region_confidence) / 2
            else:
                article.quality_score = article.relevance_score
            
            # ENHANCED: LLM title enhancement (WORKS FOR BOTH COMPANIES AND TRENDS)
            if self.llm_integration.enabled and not article.enhanced_title:
                article.enhanced_title = await self.llm_integration.enhance_title(
                    article.title, article.content, force=False
                )
                # Use enhanced title as display title
                if article.enhanced_title and len(article.enhanced_title) >= 15:
                    article.title = article.enhanced_title
            
            # ENHANCED: Generate summary (rule-based first, then LLM fallback)
            if not article.summary:
                article.summary = await self.summarizer.summarize_article(
                    article.content, article.enhanced_title or article.title
                )
            
            # ENHANCED: LLM summary fallback (WORKS FOR BOTH COMPANIES AND TRENDS)
            if self._should_use_llm_summary_fallback(article):
                llm_summary = await self.llm_integration.generate_summary(
                    article.content, article.enhanced_title or article.title
                )
                if llm_summary:
                    article.summary = llm_summary
            
            return article
        except Exception as e:
            raise ContentProcessingError(f"Article processing failed: {e}")
    
    def _should_use_llm_summary_fallback(self, article: ContentSource) -> bool:
        """Determine if LLM summary fallback should be used"""
        if not self.llm_integration.enabled or not self.config.llm_summary_fallback_enabled:
            return False
        
        summary_too_short = len(article.summary.strip()) < self.config.llm_summary_min_chars_trigger
        content_too_long = len(article.content or "") > self.config.llm_summary_long_content_trigger
        
        return summary_too_short or (content_too_long and summary_too_short)
    
    def is_duplicate(self, source: ContentSource, recent_hashes: Set[str], existing: List[ContentSource]) -> bool:
        """Efficient duplicate detection"""
        if source.content_hash in recent_hashes:
            return True
        for existing_source in existing[-30:]:
            if (source.content_hash == existing_source.content_hash or
                source.url == existing_source.url or
                self._title_similarity(source.title, existing_source.title) > 0.9):
                return True
        return False
    
    def _title_similarity(self, title1: str, title2: str) -> float:
        """Calculate title similarity"""
        if not title1 or not title2:
            return 0.0
        if abs(len(title1) - len(title2)) > max(len(title1), len(title2)) * 0.6:
            return 0.0
        words1 = set(title1.lower().split())
        words2 = set(title2.lower().split())
        if not words1 or not words2:
            return 0.0
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        return len(intersection) / len(union) if union else 0.0

# --------------------------------------------------------------------------------------
# Company Monitoring System
# --------------------------------------------------------------------------------------

class CompanyMonitoringSystem:
    """Monitors fintech companies using web scraping and RSS with FULL LLM INTEGRATION"""
    
    def __init__(self, config: UnifiedConfig, region_filter: EnhancedRegionFilter, llm: EnhancedLLMIntegration, processor: ArticleProcessor):
        self.config = config
        self.region_filter = region_filter
        self.llm = llm
        self.processor = processor
        
    def collect_from_rss_feed(self, feed_url: str, company_key: str) -> List[ContentSource]:
        """Collect articles from RSS feed"""
        articles = []
        
        try:
            resp = http_get_bypass(feed_url, bypass_robots=self.config.bypass_robots_txt)
            feed = feedparser.parse(resp.content)
            
            if not hasattr(feed, 'entries') or not feed.entries:
                return articles
            
            # FIXED: Use timezone-aware cutoff date
            cutoff = utc_date_filter(self.config.max_age_days)
            
            for entry in feed.entries[:self.config.company_max_items_per_source]:
                try:
                    title = getattr(entry, 'title', '').strip()
                    link = getattr(entry, 'link', '').strip()
                    
                    if not title or not link:
                        continue
                    
                    # Parse content
                    content = ""
                    if hasattr(entry, 'summary'):
                        content = BeautifulSoup(entry.summary, 'html.parser').get_text(strip=True)
                    
                    # FIXED: Parse date with timezone awareness
                    entry_date = None
                    if hasattr(entry, 'published'):
                        entry_date = parse_date_robust(entry.published)
                    elif hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            # Convert to timezone-aware UTC
                            naive_dt = datetime.datetime(*entry.published_parsed[:6])
                            entry_date = naive_dt.replace(tzinfo=datetime.timezone.utc)
                        except:
                            pass
                    
                    # FIXED: Compare timezone-aware dates
                    if entry_date and entry_date < cutoff:
                        continue
                    
                    if self.region_filter.should_exclude_by_title(title):
                        continue
                    
                    if not self.region_filter.is_us_eu_domain(link):
                        continue
                    
                    article = ContentSource(
                        url=link,
                        title=title,
                        content=content,
                        source_type='company',
                        category=company_key,
                        published_date=entry_date,
                        relevance_score=0.8  # Company articles are inherently relevant
                    )
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.debug(f"Error processing RSS entry: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"RSS collection failed for {feed_url}: {e}")
        
        return articles
    
    def collect_from_page_scraping(self, page_url: str, company_key: str) -> List[ContentSource]:
        """Collect articles from web page scraping"""
        articles = []
        
        try:
            resp = http_get_bypass(page_url, bypass_robots=self.config.bypass_robots_txt)
            soup = BeautifulSoup(resp.text, "html.parser")
            # First try RSS auto-discovery
            try:
                rss_feeds = autodiscover_rss(resp.text, page_url)
                for feed in rss_feeds[:3]:
                    try:
                        articles.extend(self.collect_from_rss_feed(feed, company_key))
                        if articles:
                            return articles[: self.config.company_max_items_per_source]
                    except Exception as _:
                        continue
            except Exception as _:
                pass

            # Next, try JSON-LD extraction (often present on press pages)
            try:
                jsonld_items = extract_articles_from_jsonld(resp.text, page_url)
                if jsonld_items:
                    for item in jsonld_items[: self.config.company_max_items_per_source]:
                        articles.append(ContentSource(
                            title=item.get("title",""),
                            summary=item.get("summary",""),
                            url=item.get("url",""),
                            domain=urlparse(item.get("url","")).netloc,
                            published_date=parse_date_robust(item.get("date","")),
                            relevance_score=0.8,
                            source_type='company',
                            category=company_key
                        ))
                    return articles
            except Exception as _:
                pass

            # Domain-specific fallbacks
            try:
                host = urlparse(page_url).netloc
                fallback_items: List[Dict[str,str]] = []
                if "mastercard.com" in host:
                    fallback_items = parse_mastercard_press(resp.text, page_url)
                elif "adyen.com" in host:
                    fallback_items = parse_adyen_press(resp.text, page_url)
                elif any(h in host for h in ("pypl.com","affirm.com","toasttab.com")):
                    fallback_items = parse_q4_list(resp.text, page_url)

                for item in fallback_items[: self.config.company_max_items_per_source]:
                    articles.append(ContentSource(
                        title=item.get("title",""),
                        summary=item.get("summary",""),
                        url=item.get("url",""),
                        domain=urlparse(item.get("url","")).netloc,
                        published_date=parse_date_robust(item.get("date","")),
                        relevance_score=0.75,
                        source_type='company',
                        category=company_key
                    ))
                if articles:
                    return articles
            except Exception as _:
                pass

            
            # Enhanced selectors for news articles
            selectors = [
                'article', '[class*="news"]', '[class*="press"]', '[class*="release"]',
                '[class*="article"]', '[class*="story"]', '[class*="post"]',
                '.newsroom-item', '.press-release', '.news-item'
            ]
            
            candidates = []
            for selector in selectors:
                candidates.extend(soup.select(selector))
            
            # Remove duplicates
            seen = set()
            unique_candidates = []
            for candidate in candidates:
                if id(candidate) not in seen:
                    seen.add(id(candidate))
                    unique_candidates.append(candidate)
            
            # FIXED: Use timezone-aware cutoff date
            cutoff = utc_date_filter(self.config.max_age_days)
            
            for candidate in unique_candidates[:self.config.company_max_items_per_source]:
                try:
                    # Find link
                    link_elem = candidate.find('a', href=True)
                    if not link_elem:
                        link_elem = candidate.find_parent('a', href=True)
                    if not link_elem:
                        continue
                    
                    href = urljoin(page_url, link_elem['href'])
                    title = link_elem.get_text(strip=True)
                    
                    if not title or len(title) < 10:
                        # Try to find title in nearby elements
                        title_candidates = [
                            candidate.find('h1'), candidate.find('h2'), candidate.find('h3'),
                            candidate.find('[class*="title"]'), candidate.find('[class*="headline"]')
                        ]
                        for t in title_candidates:
                            if t and t.get_text(strip=True):
                                title = t.get_text(strip=True)
                                break
                    
                    if not title or len(title) < 10:
                        continue
                    
                    if self.region_filter.should_exclude_by_title(title):
                        continue
                    
                    if not self.region_filter.is_us_eu_domain(href):
                        continue
                    
                    # Extract content
                    content = candidate.get_text(strip=True)[:500]  # First 500 chars
                    
                    # FIXED: Extract date with timezone awareness
                    entry_date = None
                    date_elem = candidate.find('time')
                    if date_elem:
                        date_text = date_elem.get('datetime') or date_elem.get_text(strip=True)
                        entry_date = parse_date_robust(date_text)
                    
                    # For recent content, be more lenient with dates
                    if not entry_date:
                        entry_date = utc_now()
                    
                    article = ContentSource(
                        url=href,
                        title=title,
                        content=content,
                        source_type='company',
                        category=company_key,
                        published_date=entry_date,
                        relevance_score=0.7  # Page scraping articles are relevant
                    )
                    
                    articles.append(article)
                    
                except Exception as e:
                    logger.debug(f"Error processing page candidate: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Page scraping failed for {page_url}: {e}")
        
        return articles
    
    async def monitor_companies(self) -> List[ContentSource]:
        """Monitor all companies for updates with FULL LLM INTEGRATION"""
        logger.info("Starting company monitoring...")
        all_articles = []
        
        for company in COMPANIES:
            if not company.scrape_enabled:
                continue
                
            logger.info(f"Processing company: {company.name}")
            company_articles = []
            
            # Process RSS feeds
            for feed_url in company.feeds:
                try:
                    feed_articles = self.collect_from_rss_feed(feed_url, company.key)
                    company_articles.extend(feed_articles)
                    logger.info(f"RSS: {len(feed_articles)} articles from {company.name}")
                except Exception as e:
                    logger.error(f"RSS error for {company.name}: {e}")
            
            # Process web pages
            for page_url in company.pages:
                try:
                    page_articles = self.collect_from_page_scraping(page_url, company.key)
                    company_articles.extend(page_articles)
                    logger.info(f"Page scraping: {len(page_articles)} articles from {company.name}")
                except Exception as e:
                    logger.error(f"Page scraping error for {company.name}: {e}")
            
            # ENHANCED: Process each article with FULL LLM INTEGRATION
            for article in company_articles:
                try:
                    processed_article = await self.processor.process_article(article)
                    all_articles.append(processed_article)
                except Exception as e:
                    logger.error(f"Error processing company article: {e}")
                    continue
            
            logger.info(f"Total for {company.name}: {len(company_articles)} articles")
            
            # Polite delay
            await asyncio.sleep(0.5)
        
        logger.info(f"Company monitoring completed. Total: {len(all_articles)} articles")
        return all_articles

# --------------------------------------------------------------------------------------
# Trend Monitoring System with Google Search
# --------------------------------------------------------------------------------------

class GoogleSearchIntegration:
    """Enhanced Google Search API integration"""
    
    def __init__(self, config: UnifiedConfig, region_filter: EnhancedRegionFilter, keyword_expander: KeywordExpansionSystem):
        self.config = config
        self.region_filter = region_filter
        self.keyword_expander = keyword_expander
        self.enabled = bool(config.google_api_key and config.google_search_engine_id)
        self.queries_today = 0
        
        if not self.enabled:
            logger.warning("Google Search API not configured - trends will use RSS only")
        else:
            logger.info(f"Google Search API enabled with daily limit: {config.google_daily_limit}")
    
    async def search_trend(self, trend_name: str, base_keywords: List[str], max_results: int = 20) -> List[ContentSource]:
        """Enhanced trend search with expanded keywords"""
        if not self.enabled or self.queries_today >= self.config.google_daily_limit:
            if not self.enabled:
                logger.debug(f"Google Search not available for {trend_name}")
            else:
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
    
    async def _process_search_result(self, item: Dict, trend_name: str, keywords: List[str]) -> Optional[ContentSource]:
        """Process search result into ContentSource"""
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
        
        source = ContentSource(
            url=url,
            title=cleaned_title,
            original_title=full_title,
            source_type='trend',
            domain=domain,
            content=snippet,
            published_date=published_date,
            category=trend_name,
            word_count=len(snippet.split()) if snippet else 0,
            search_keywords=keywords[:5],
            region_confidence=region_confidence
        )
        return source
    
    def _clean_title_preserve_all_words(self, title: str) -> str:
        if not title:
            return ""
        cleaned = re.sub(r'\s+', ' ', title.strip())
        cleaned = re.sub(r'&[a-zA-Z0-9]+;', '', cleaned)
        patterns = [
            r'\s*-\s*[^-]*(?:\.com|\.org|\.net|news|times|post|journal).*$',
            r'\s*\|\s*[^|]*(?:\.com|\.org|\.net|news|times|post|journal).*$',
            r'\s*::\s*.*$',
        ]
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned).strip()
        if len(cleaned) < len(title) * 0.6:
            cleaned = title.strip()
        return cleaned
    
    def _extract_published_date(self, item: Dict) -> Optional[datetime.datetime]:
        """FIXED: Extract published date with timezone awareness"""
        if 'pagemap' in item and 'metatags' in item['pagemap']:
            for meta in item['pagemap']['metatags']:
                date_str = meta.get('article:published_time') or meta.get('datePublished')
                if date_str:
                    try:
                        if 'T' in date_str:
                            # Parse ISO format and ensure timezone awareness
                            dt = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            return ensure_utc(dt)
                    except:
                        pass
        return None
    
    def _deduplicate_sources(self, sources: List[ContentSource]) -> List[ContentSource]:
        unique_sources = []
        seen_urls = set()
        seen_hashes = set()
        for source in sources:
            if (source.url not in seen_urls and 
                source.content_hash not in seen_hashes):
                unique_sources.append(source)
                seen_urls.add(source.url)
                seen_hashes.add(source.content_hash)
        return unique_sources

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

    async def extract_from_rss_preserve_titles(self, rss_url: str, source_name: str) -> List[ContentSource]:
        sources = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; UnifiedFinTechUpdate/3.2; +https://example.com/bot)',
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
                
                # FIXED: Use timezone-aware cutoff date
                cutoff_date = utc_date_filter(14)  # 14-day window for RSS
                
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

    async def _process_rss_entry(self, entry, source_name: str, cutoff_date: datetime.datetime) -> Optional[ContentSource]:
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
        
        # FIXED: Parse date with timezone awareness
        pub_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                naive_dt = datetime.datetime(*entry.published_parsed[:6])
                pub_date = naive_dt.replace(tzinfo=datetime.timezone.utc)
            except:
                pass
        elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
            try:
                naive_dt = datetime.datetime(*entry.updated_parsed[:6])
                pub_date = naive_dt.replace(tzinfo=datetime.timezone.utc)
            except:
                pass
        
        # FIXED: Compare timezone-aware dates
        if pub_date and pub_date < cutoff_date:
            return None
        
        source = ContentSource(
            url=url,
            title=full_title,
            original_title=full_title_raw,
            source_type='trend',
            domain=domain,
            published_date=pub_date,
            content=content,
            word_count=len(content.split()) if content else 0,
            region_confidence=region_confidence
        )
        return source
    
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
        if len(cleaned) < len(title) * 0.6:
            cleaned = title.strip()
        return cleaned

class TrendMonitoringSystem:
    """Monitors fintech trends using Google Search and RSS with FULL LLM INTEGRATION"""
    
    def __init__(self, config: UnifiedConfig, region_filter: EnhancedRegionFilter, llm: EnhancedLLMIntegration, processor: ArticleProcessor):
        self.config = config
        self.region_filter = region_filter
        self.llm = llm
        self.processor = processor
        self.keyword_expander = KeywordExpansionSystem(config)
        self.google_search = GoogleSearchIntegration(config, region_filter, self.keyword_expander)
        self.rss_validator = EnhancedRSSFeedValidator(region_filter)
        
    async def search_google_for_trend(self, trend_key: str, trend_config: TrendConfig) -> List[ContentSource]:
        """Search Google for trend-related articles"""
        return await self.google_search.search_trend(trend_key, trend_config.keywords, max_results=20)
    
    async def collect_from_rss_feeds(self, trend_key: str, trend_config: TrendConfig) -> List[ContentSource]:
        """Collect articles from RSS feeds for a trend"""
        articles = []
        
        for rss_url in trend_config.rss_feeds:
            try:
                rss_articles = await self.rss_validator.extract_from_rss_preserve_titles(
                    rss_url, f"{trend_key}_rss"
                )
                for article in rss_articles:
                    article.category = trend_key
                    article.source_type = 'trend'
                    article.search_keywords = trend_config.keywords[:5]
                articles.extend(rss_articles)
                domain = urlparse(rss_url).netloc
                logger.info(f"RSS: {len(rss_articles)} articles from {domain} for {trend_key}")
            except Exception as e:
                logger.error(f"RSS collection failed for {rss_url}: {e}")
        
        return articles
    
    async def monitor_trends(self) -> List[ContentSource]:
        """Monitor all trends for updates with FULL LLM INTEGRATION"""
        logger.info("Starting trend monitoring...")
        all_articles = []
        
        for trend_key, trend_config in TRENDS.items():
            logger.info(f"Processing trend: {trend_config.name}")
            trend_articles = []
            
            # Google Search
            if trend_config.google_search_enabled:
                try:
                    google_articles = await self.search_google_for_trend(trend_key, trend_config)
                    trend_articles.extend(google_articles)
                    logger.info(f"Google: {len(google_articles)} articles for {trend_key}")
                except Exception as e:
                    logger.error(f"Google Search error for {trend_key}: {e}")
            
            # RSS feeds
            try:
                rss_articles = await self.collect_from_rss_feeds(trend_key, trend_config)
                trend_articles.extend(rss_articles)
                logger.info(f"RSS: {len(rss_articles)} articles for {trend_key}")
            except Exception as e:
                logger.error(f"RSS error for {trend_key}: {e}")
            
            # ENHANCED: Process each article with FULL LLM INTEGRATION
            for article in trend_articles:
                try:
                    processed_article = await self.processor.process_article(article, trend_config)
                    all_articles.append(processed_article)
                except Exception as e:
                    logger.error(f"Error processing trend article: {e}")
                    continue
            
            logger.info(f"Total for {trend_key}: {len(trend_articles)} articles")
        
        logger.info(f"Trend monitoring completed. Total: {len(all_articles)} articles")
        return all_articles

# --------------------------------------------------------------------------------------
# ENHANCED Email Generator with Professional Card Layout
# --------------------------------------------------------------------------------------

class UnifiedEmailGenerator:
    """ENHANCED: Email generator with improved formatting and professional card layout"""
    
    def __init__(self, config: UnifiedConfig, llm_integration: EnhancedLLMIntegration):
        self.config = config
        self.llm_integration = llm_integration
        
        # Company name mapping
        self.company_names = {company.key: company.name for company in COMPANIES}
        
        # Trend info with emojis
        self.trend_info = {
            "instant_payments": {"name": "Instant Payments", "emoji": "⚡"},
            "a2a_open_banking": {"name": "A2A & Open Banking", "emoji": "🏦"},
            "stablecoins": {"name": "Stablecoins & CBDC", "emoji": "💰"},
            "softpos_tap_to_pay": {"name": "SoftPOS & Tap to Pay", "emoji": "📱"},
            "cross_border": {"name": "Cross-Border Payments", "emoji": "🌍"},
            "bnpl": {"name": "Buy Now, Pay Later", "emoji": "📅"},
            "payment_orchestration": {"name": "Payment Orchestration", "emoji": "🎼"},
            "fraud_ai": {"name": "Fraud Prevention & AI", "emoji": "🛡️"},
            "pci_dss": {"name": "PCI DSS & Security", "emoji": "🔒"},
            "wallet_nfc": {"name": "Digital Wallets & NFC", "emoji": "💳"},
        }
    
    async def generate_unified_email(self, company_data: Dict[str, List[ContentSource]], 
                                   trend_data: Dict[str, List[ContentSource]], 
                                   stats: Dict, database_stats: Dict = None) -> str:
        """Generate unified email with both company and trend sections"""
        try:
            today = datetime.date.today()
            date_str = today.strftime("%B %d, %Y")
            
            # Generate sections
            company_section = await self._generate_company_section(company_data)
            trend_section = await self._generate_trend_section(trend_data)
            
            # Calculate stats
            total_company_articles = sum(len(articles) for articles in company_data.values())
            total_trend_articles = sum(len(articles) for articles in trend_data.values())
            total_articles = total_company_articles + total_trend_articles
            high_quality_articles = sum(1 for articles in list(company_data.values()) + list(trend_data.values()) 
                                      for article in articles if article.relevance_score >= self.config.relevance_threshold)
            semantic_scored_articles = sum(1 for articles in list(company_data.values()) + list(trend_data.values()) 
                                         for article in articles if article.semantic_relevance_score > 0)
            
            db_info = ""
            if database_stats:
                db_info = f" • DB: {database_stats.get('total_articles', 0)} articles"
            llm_info = " • LLM Enhanced" if self.llm_integration.enabled else ""
            semantic_info = f" • {semantic_scored_articles} semantic scored" if semantic_scored_articles > 0 else ""
            
            # ENHANCED: Professional email template with card layout
            html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FinTech Daily Update - {date_str}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.4;
            color: #000;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }}
        .header {{
            margin-bottom: 30px;
            text-align: left;
            border-bottom: 2px solid #e6e6e6;
            padding-bottom: 15px;
        }}
        .header h1 {{
            margin: 0 0 5px 0;
            font-size: 26px;
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
        .main-section {{
            margin-bottom: 40px;
        }}
        .main-section h2 {{
            font-size: 22px;
            color: #000;
            margin: 0 0 20px 0;
            padding: 10px 0;
            border-bottom: 1px solid #ddd;
        }}
        .subsection {{
            margin-bottom: 25px;
        }}
        .subsection h3 {{
            font-size: 18px;
            color: #333;
            margin: 0 0 12px 0;
            display: flex;
            align-items: center;
        }}
        .subsection h3 .emoji {{
            margin-right: 8px;
        }}
        .article {{
            margin-bottom: 18px;
        }}
        .article-card {{
            background: #fafafa;
            border: 1px solid #e8e8e8;
            border-radius: 8px;
            padding: 12px 14px;
            margin-bottom: 14px;
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
            color: #444;
            font-size: 14px;
            margin-bottom: 6px;
            line-height: 1.4;
        }}
        .article-meta {{
            font-size: 12px;
            color: #666;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
        }}
        .article-meta .left {{
            display: flex;
            gap: 8px;
            align-items: center;
        }}
        .badge {{
            background-color: #f0f0f0;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 11px;
        }}
        .badge.company {{
            background-color: #e8f5e8;
            color: #2e7d32;
        }}
        .badge.trend {{
            background-color: #e3f2fd;
            color: #1565c0;
        }}
        .badge.high {{
            background-color: #e6f7e6;
            color: #2e7d32;
        }}
        .badge.llm {{
            background-color: #f3e5f5;
            color: #7b1fa2;
        }}
        .no-articles {{
            color: #666;
            font-style: italic;
            padding: 15px 0;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            font-size: 12px;
            color: #666;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>FinTech Daily Update v3.2.0</h1>
        <p>{date_str}</p>
        <div class="stats">
            {total_articles} articles • Companies: {total_company_articles} • Trends: {total_trend_articles} • {high_quality_articles} high-quality • {stats.get('processing_time', 0):.1f}s{db_info}{llm_info}{semantic_info}
        </div>
    </div>
    
    <div class="main-section">
        <h2>📈 Company Updates</h2>
        {company_section}
    </div>
    
    <div class="main-section">
        <h2>🔍 Trend Analysis</h2>
        {trend_section}
    </div>
    
    <div class="footer">
        <p>Unified FinTech Daily Update System v3.2.0</p>
        <p>Companies: {len(COMPANIES)} monitored • Trends: {len(TRENDS)} tracked • LLM Enhanced Titles & Summaries • Filter: {self.config.max_age_days} days</p>
    </div>
</body>
</html>"""
            return html_template
        except Exception as e:
            logger.error(f"Email generation failed: {e}")
            return f"<html><body><h2>Error generating digest: {e}</h2></body></html>"
    
    async def _generate_company_section(self, company_data: Dict[str, List[ContentSource]]) -> str:
        """Generate company updates section with LLM-enhanced titles"""
        if not any(company_data.values()):
            return '<div class="no-articles">No company updates found in the last 7 days.</div>'
        
        section_html = ""
        
        for company_key, articles in company_data.items():
            if not articles:
                continue
            
            company_name = self.company_names.get(company_key, company_key.title())
            
            articles_html = ""
            for article in articles[:5]:  # Limit per company
                # ENHANCED: Force professional LLM title rewrite for email display
                display_title = article.enhanced_title or article.title
                if self.llm_integration.enabled and not article.enhanced_title:
                    forced_title = await self.llm_integration.enhance_title(display_title, article.content, force=True)
                    if forced_title and len(forced_title) >= 15:
                        display_title = forced_title
                        article.enhanced_title = forced_title
                
                content = article.summary or article.content
                url = article.url
                domain = article.domain
                published_date = article.published_date
                relevance_score = article.relevance_score * 100
                semantic_score = article.semantic_relevance_score * 100 if article.semantic_relevance_score > 0 else 0
                
                # FIXED: Format date with timezone awareness
                date_str = ""
                if published_date:
                    try:
                        if isinstance(published_date, str):
                            dt = datetime.datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                        else:
                            dt = ensure_utc(published_date)
                        if dt:
                            date_str = dt.strftime("%Y-%m-%d")
                    except:
                        date_str = ""
                
                # Truncate content
                if content and len(content) > 200:
                    content = content[:197] + "..."
                
                # Quality badges
                relevance_class = "high" if relevance_score >= 70 else ""
                badges = f'<span class="badge company">COMPANY</span>'
                badges += f'<span class="badge {relevance_class}">{relevance_score:.0f}% match</span>'
                if semantic_score > 0:
                    badges += f'<span class="badge llm">{semantic_score:.0f}% semantic</span>'
                if article.enhanced_title:
                    badges += f'<span class="badge llm">LLM Enhanced</span>'
                
                articles_html += f"""
                <div class="article">
                    <div class="article-card">
                        <a href="{url}" class="article-title" target="_blank">{display_title}</a>
                        {f'<div class="article-summary">{content}</div>' if content else ''}
                        <div class="article-meta">
                            <div class="left">{badges}</div>
                            <div>{domain}{f' • {date_str}' if date_str else ''}</div>
                        </div>
                    </div>
                </div>
                """
            
            section_html += f"""
            <div class="subsection">
                <h3>{company_name} ({len(articles)} updates)</h3>
                {articles_html}
            </div>
            """
        
        return section_html
    
    async def _generate_trend_section(self, trend_data: Dict[str, List[ContentSource]]) -> str:
        """Generate trend analysis section with LLM-enhanced titles"""
        if not any(trend_data.values()):
            return '<div class="no-articles">No trend updates found in the last 7 days.</div>'
        
        section_html = ""
        
        for trend_key, articles in trend_data.items():
            if not articles:
                continue
            
            trend_info = self.trend_info.get(trend_key, {"name": trend_key.title(), "emoji": "📊"})
            
            articles_html = ""
            for article in articles[:4]:  # Limit per trend
                # ENHANCED: Force professional LLM title rewrite for email display
                display_title = article.enhanced_title or article.title
                if self.llm_integration.enabled and not article.enhanced_title:
                    forced_title = await self.llm_integration.enhance_title(display_title, article.content, force=True)
                    if forced_title and len(forced_title) >= 15:
                        display_title = forced_title
                        article.enhanced_title = forced_title
                
                content = article.summary or article.content
                url = article.url
                domain = article.domain
                relevance_score = article.relevance_score * 100
                semantic_score = article.semantic_relevance_score * 100 if article.semantic_relevance_score > 0 else 0
                
                # Truncate content
                if content and len(content) > 150:
                    content = content[:147] + "..."
                
                # Quality badges
                relevance_class = "high" if relevance_score >= 70 else ""
                badges = f'<span class="badge trend">TREND</span>'
                badges += f'<span class="badge {relevance_class}">{relevance_score:.0f}% match</span>'
                if semantic_score > 0:
                    badges += f'<span class="badge llm">{semantic_score:.0f}% semantic</span>'
                if article.enhanced_title:
                    badges += f'<span class="badge llm">LLM Enhanced</span>'
                
                articles_html += f"""
                <div class="article">
                    <div class="article-card">
                        <a href="{url}" class="article-title" target="_blank">{display_title}</a>
                        {f'<div class="article-summary">{content}</div>' if content else ''}
                        <div class="article-meta">
                            <div class="left">{badges}</div>
                            <div>{domain} • {article.source_type.upper()}</div>
                        </div>
                    </div>
                </div>
                """
            
            section_html += f"""
            <div class="subsection">
                <h3><span class="emoji">{trend_info['emoji']}</span>{trend_info['name']} ({len(articles)} articles)</h3>
                {articles_html}
            </div>
            """
        
        return section_html

# --------------------------------------------------------------------------------------
# Unified System Orchestrator
# --------------------------------------------------------------------------------------

class UnifiedFintechUpdateSystem:
    """Main orchestrator for the unified fintech update system with FULL LLM INTEGRATION"""
    
    def __init__(self):
        try:
            self.config = UnifiedConfig()
            self.region_filter = EnhancedRegionFilter()
            self.llm = EnhancedLLMIntegration(self.config)
            self.database = UnifiedDatabaseManager(self.config)
            
            # Initialize article processor with LLM integration
            self.article_processor = ArticleProcessor(self.config, self.region_filter, self.llm)
            
            # Initialize monitoring systems with LLM-aware processors
            self.company_monitor = CompanyMonitoringSystem(self.config, self.region_filter, self.llm, self.article_processor)
            self.trend_monitor = TrendMonitoringSystem(self.config, self.region_filter, self.llm, self.article_processor)
            self.email_generator = UnifiedEmailGenerator(self.config, self.llm)
            
            # Initialize stats
            self.stats = {
                'processing_time': 0,
                'start_time': time.time(),
                'company_articles': 0,
                'trend_articles': 0,
                'total_articles_saved': 0,
                'email_articles': 0,
                'semantic_scored_articles': 0,
                'llm_enhanced_titles': 0
            }
            
            logger.info("Unified FinTech Update System v3.2.0 initialized")
            logger.info(f"Companies: {'ENABLED' if self.config.companies_enabled else 'DISABLED'}")
            logger.info(f"Trends: {'ENABLED' if self.config.trends_enabled else 'DISABLED'}")
            logger.info(f"LLM: {'ENABLED' if self.config.llm_enabled else 'DISABLED'} • Model: {self.config.llm_model}")
            logger.info(f"Age filter: {self.config.max_age_days} days")
            
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            raise
    
    def _is_duplicate(self, article: ContentSource, recent_hashes: Set[str], existing: List[ContentSource]) -> bool:
        """Check if article is a duplicate"""
        if article.content_hash in recent_hashes:
            return True
        
        for existing_article in existing[-50:]:  # Check last 50 articles
            if (article.content_hash == existing_article.content_hash or 
                article.url == existing_article.url):
                return True
        
        return False
    
    async def _is_url_accessible(self, url: str, timeout: int = 8) -> bool:
        """Check if URL is accessible"""
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    url, 
                    headers=headers, 
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=timeout), 
                    ssl=False
                ) as resp:
                    return 200 <= resp.status < 400
        except:
            return False

    def _is_recent(self, dt: Optional[datetime.datetime], days: int = 7) -> bool:
        """FIXED: Check if datetime is recent (timezone-aware)"""
        if not dt:
            return True
        
        # Ensure both dates are timezone-aware for comparison
        dt_utc = ensure_utc(dt)
        cutoff = utc_date_filter(days)
        
        return dt_utc >= cutoff
    
    async def run_unified_update(self) -> Dict[str, Any]:
        """Run the complete unified update process with FULL LLM INTEGRATION"""
        try:
            print("🚀 Starting Unified FinTech Daily Update v3.2.0...")
            
            # Cleanup old data
            self.database.cleanup_old_data()
            
            # Get recent hashes for deduplication
            recent_hashes = self.database.get_recent_article_hashes(7)
            all_articles = []
            semantic_scored_count = 0
            
            # Company monitoring
            company_articles = []
            if self.config.companies_enabled:
                print("\n📈 Running Company Monitoring with FULL LLM Integration...")
                try:
                    company_articles = await self.company_monitor.monitor_companies()
                    self.stats['company_articles'] = len(company_articles)
                    print(f"   Collected {len(company_articles)} company articles")
                    
                    # Count LLM enhancements
                    enhanced_count = sum(1 for article in company_articles if article.enhanced_title)
                    self.stats['llm_enhanced_titles'] += enhanced_count
                    print(f"   LLM Enhanced: {enhanced_count} titles")
                    
                except Exception as e:
                    print(f"   Company monitoring error: {e}")
            
            # Trend monitoring
            trend_articles = []
            if self.config.trends_enabled:
                print("\n🔍 Running Trend Monitoring with FULL LLM Integration...")
                try:
                    trend_articles = await self.trend_monitor.monitor_trends()
                    self.stats['trend_articles'] = len(trend_articles)
                    print(f"   Collected {len(trend_articles)} trend articles")
                    
                    # Count LLM enhancements
                    enhanced_count = sum(1 for article in trend_articles if article.enhanced_title)
                    self.stats['llm_enhanced_titles'] += enhanced_count
                    print(f"   LLM Enhanced: {enhanced_count} titles")
                    
                except Exception as e:
                    print(f"   Trend monitoring error: {e}")
            
            # Combine and process articles
            all_articles = company_articles + trend_articles
            
            # Semantic scoring for top articles (respecting TOTAL limit)
            if (self.llm.semantic_scoring_enabled and all_articles):
                print(f"\n🧠 Running Semantic Scoring (limit: {self.config.semantic_total_limit})...")
                
                # Sort all articles by relevance and take top ones for semantic scoring
                sorted_for_semantic = sorted(all_articles, key=lambda a: a.relevance_score, reverse=True)
                articles_to_score = sorted_for_semantic[:self.config.semantic_total_limit]
                
                for article in articles_to_score:
                    try:
                        if article.source_type == 'trend':
                            # Get trend config for keywords
                            trend_config = TRENDS.get(article.category)
                            if trend_config:
                                semantic_score = await self.llm.evaluate_semantic_relevance(
                                    article.title, article.content, trend_config.keywords
                                )
                                if semantic_score > 0:
                                    article.semantic_relevance_score = semantic_score
                                    semantic_scored_count += 1
                                    
                                    # Update quality score with semantic component
                                    combined = article.relevance_score * 0.6 + semantic_score * 0.4
                                    article.quality_score = (combined + article.region_confidence) / 2
                        
                        # Stop if we've hit the total limit
                        if semantic_scored_count >= self.config.semantic_total_limit:
                            break
                            
                    except Exception as e:
                        logger.debug(f"Semantic scoring failed for article: {e}")
                        continue
                
                self.stats['semantic_scored_articles'] = semantic_scored_count
                print(f"   Semantic scored: {semantic_scored_count} articles")
            
            # Save articles and filter for email
            saved_articles = []
            email_company_data = {company.key: [] for company in COMPANIES}
            email_trend_data = {trend_key: [] for trend_key in TRENDS.keys()}
            
            print(f"\n💾 Processing {len(all_articles)} total articles...")
            
            for article in all_articles:
                # Skip duplicates
                if self._is_duplicate(article, recent_hashes, saved_articles):
                    continue
                
                # Save to database
                if self.database.save_article(article):
                    saved_articles.append(article)
                    self.stats['total_articles_saved'] += 1
                    
                    # Check if article should be included in email
                    if (article.quality_score >= self.config.relevance_threshold and
                        self._is_recent(article.published_date, days=self.config.max_age_days)):
                        
                        if article.source_type == 'company':
                            email_company_data[article.category].append(article)
                        elif article.source_type == 'trend':
                            email_trend_data[article.category].append(article)
            
            # Sort articles by quality for email
            for company_key in email_company_data:
                email_company_data[company_key].sort(key=lambda x: x.quality_score, reverse=True)
                email_company_data[company_key] = email_company_data[company_key][:5]  # Limit per company
            
            for trend_key in email_trend_data:
                email_trend_data[trend_key].sort(key=lambda x: x.quality_score, reverse=True)
                email_trend_data[trend_key] = email_trend_data[trend_key][:4]  # Limit per trend
            
            # Apply global email limit
            all_email_articles = []
            for articles in list(email_company_data.values()) + list(email_trend_data.values()):
                all_email_articles.extend(articles)
            
            all_email_articles.sort(key=lambda x: x.quality_score, reverse=True)
            limited_email_articles = all_email_articles[:self.config.max_email_articles]
            
            # Redistribute limited articles back
            final_company_data = {company.key: [] for company in COMPANIES}
            final_trend_data = {trend_key: [] for trend_key in TRENDS.keys()}
            
            for article in limited_email_articles:
                if article.source_type == 'company':
                    final_company_data[article.category].append(article)
                elif article.source_type == 'trend':
                    final_trend_data[article.category].append(article)
            
            # Check accessibility for email articles
            if limited_email_articles:
                print(f"\n🔗 Checking accessibility of {len(limited_email_articles)} email articles...")
                accessibility_checks = await asyncio.gather(
                    *[self._is_url_accessible(a.url) for a in limited_email_articles], 
                    return_exceptions=True
                )
                accessible_articles = []
                for article, is_accessible in zip(limited_email_articles, accessibility_checks):
                    if is_accessible is True:
                        accessible_articles.append(article)
                    else:
                        logger.debug(f"Excluding inaccessible URL: {article.url}")
                
                # Update final data with only accessible articles
                final_company_data = {company.key: [] for company in COMPANIES}
                final_trend_data = {trend_key: [] for trend_key in TRENDS.keys()}
                
                for article in accessible_articles:
                    if article.source_type == 'company':
                        final_company_data[article.category].append(article)
                    elif article.source_type == 'trend':
                        final_trend_data[article.category].append(article)
                
                self.stats['email_articles'] = len(accessible_articles)
            
            # Calculate final stats
            self.stats['processing_time'] = time.time() - self.stats['start_time']
            
            # Save daily stats
            enhanced_stats = {
                **self.stats,
                'total_articles': len(all_articles),
                'google_articles': sum(1 for a in all_articles if a.source_type == 'trend' and 'google' in getattr(a, 'search_keywords', [])),
                'rss_articles': sum(1 for a in all_articles if 'rss' in getattr(a, 'domain', '')),
                'avg_relevance': sum(a.relevance_score for a in all_articles) / len(all_articles) if all_articles else 0,
                'avg_semantic_relevance': sum(a.semantic_relevance_score for a in all_articles if a.semantic_relevance_score > 0) / max(1, semantic_scored_count) if semantic_scored_count > 0 else 0
            }
            
            self.database.save_daily_stats(enhanced_stats)
            database_stats = self.database.get_database_stats()
            
            # Generate and send email with FULL LLM ENHANCEMENT
            email_sent = False
            if self.config.email_recipients and self.stats['email_articles'] > 0:
                try:
                    print("\n📧 Generating unified email with LLM enhancements...")
                    html_content = await self.email_generator.generate_unified_email(
                        final_company_data, final_trend_data, enhanced_stats, database_stats
                    )
                    email_sent = self._send_email(html_content)
                    if email_sent:
                        print("   ✅ Email sent successfully!")
                    else:
                        print("   ❌ Email not sent")
                except Exception as e:
                    print(f"   ❌ Email error: {e}")
            else:
                print("\n📧 No email recipients configured or no articles found")
            
            # Summary
            print(f"\n📊 UNIFIED UPDATE SUMMARY:")
            print(f"   Company articles: {self.stats['company_articles']}")
            print(f"   Trend articles: {self.stats['trend_articles']}")
            print(f"   Total articles saved: {self.stats['total_articles_saved']}")
            print(f"   Email articles: {self.stats['email_articles']}")
            if self.llm.enabled:
                llm_stats = self.llm.get_usage_stats()
                print(f"   LLM usage: {llm_stats['daily_requests']} calls, {llm_stats['daily_tokens']} tokens, ${llm_stats['daily_cost']:.4f}")
                print(f"   LLM enhanced titles: {self.stats['llm_enhanced_titles']}")
            if semantic_scored_count > 0:
                print(f"   Semantic scored: {semantic_scored_count}/{self.config.semantic_total_limit}")
            print(f"   Processing time: {self.stats['processing_time']:.1f}s")
            print(f"   Email sent: {'YES' if email_sent else 'NO'}")
            
            return {
                'success': True,
                'company_articles': self.stats['company_articles'],
                'trend_articles': self.stats['trend_articles'],
                'total_articles_saved': self.stats['total_articles_saved'],
                'email_articles': self.stats['email_articles'],
                'semantic_scored_articles': semantic_scored_count,
                'llm_enhanced_titles': self.stats['llm_enhanced_titles'],
                'email_sent': email_sent,
                'processing_time': self.stats['processing_time']
            }
            
        except Exception as e:
            print(f"❌ Unified update failed: {e}")
            logger.error(f"Unified update failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _send_email(self, html_content: str) -> bool:
        """Send unified email"""
        try:
            if not self.config.smtp_user or not self.config.smtp_password:
                return False
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"FinTech Daily Update v3.2.0 - {datetime.date.today().strftime('%B %d, %Y')}"
            msg['From'] = f"FinTech Daily Update <{self.config.smtp_user}>"
            msg['To'] = ', '.join(self.config.email_recipients)
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))
            
            context = ssl.create_default_context()
            with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
                server.starttls(context=context)
                server.login(self.config.smtp_user, self.config.smtp_password)
                server.send_message(msg)
            
            return True
        except Exception as e:
            logger.error(f"Email sending failed: {e}")
            return False

# --------------------------------------------------------------------------------------
# Main Function
# --------------------------------------------------------------------------------------

async def main():
    """Main function with comprehensive command line options"""
    parser = argparse.ArgumentParser(description="Unified FinTech Daily Update System v3.2.0")
    parser.add_argument("--test", action="store_true", help="Test mode - validate configuration only")
    parser.add_argument("--companies-only", action="store_true", help="Run company monitoring only")
    parser.add_argument("--trends-only", action="store_true", help="Run trend monitoring only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--disable-llm", action="store_true", help="Disable LLM integration")
    parser.add_argument("--disable-semantic", action="store_true", help="Disable semantic scoring")
    parser.add_argument("--config-check", action="store_true", help="Check configuration and exit")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Apply CLI overrides
    if args.disable_llm:
        os.environ['LLM_INTEGRATION_ENABLED'] = '0'
        os.environ['SEMANTIC_SCORING_ENABLED'] = '0'
        print("🔇 LLM integration disabled via CLI")
    
    if args.disable_semantic:
        os.environ['SEMANTIC_SCORING_ENABLED'] = '0'
        print("🔇 Semantic scoring disabled via CLI")
    
    if args.companies_only:
        os.environ['TRENDS_ENABLED'] = '0'
        print("📈 Companies only mode")
    
    if args.trends_only:
        os.environ['COMPANIES_ENABLED'] = '0'
        print("🔍 Trends only mode")
    
    try:
        if args.config_check:
            try:
                config = UnifiedConfig()
                print("🔧 CONFIGURATION CHECK:")
                print(f"   Companies: {'ENABLED' if config.companies_enabled else 'DISABLED'}")
                print(f"   Trends: {'ENABLED' if config.trends_enabled else 'DISABLED'}")
                print(f"   LLM: {'ENABLED' if config.llm_enabled else 'DISABLED'} • Model: {config.llm_model}")
                print(f"   Semantic scoring: {'ENABLED' if config.semantic_scoring_enabled else 'DISABLED'} (limit: {config.semantic_total_limit})")
                print(f"   Email limit: {config.max_email_articles} articles")
                print(f"   Age filter: {config.max_age_days} days")
                print(f"   Database path: {config.database_path}")
                print(f"   Email recipients: {len(config.email_recipients)} configured")
                if config.trends_enabled:
                    print(f"   Google Search API: {'CONFIGURED' if config.google_api_key else 'NOT CONFIGURED'}")
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
        
        if args.test:
            print("🧪 RUNNING TEST MODE...")
            try:
                config = UnifiedConfig()
                print("   ✅ Configuration validated")
                
                # Test database
                db = UnifiedDatabaseManager(config)
                print("   ✅ Database connection tested")
                
                # Test LLM (if enabled)
                if config.llm_enabled:
                    llm = EnhancedLLMIntegration(config)
                    print(f"   ✅ LLM integration ready ({config.llm_model})")
                
                print("   ✅ All systems ready for production")
                return 0
            except Exception as e:
                print(f"   ❌ Test failed: {e}")
                return 1
        
        # Run unified update
        system = UnifiedFintechUpdateSystem()
        result = await system.run_unified_update()
        
        if result['success']:
            print("\n✅ UNIFIED UPDATE COMPLETED SUCCESSFULLY!")
            print(f"📧 Email sent: {'YES' if result.get('email_sent') else 'NO'}")
            print(f"🧠 LLM enhanced titles: {result.get('llm_enhanced_titles', 0)}")
            return 0
        else:
            print(f"\n❌ UNIFIED UPDATE FAILED: {result.get('error', 'Unknown error')}")
            return 1
    
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ System error: {e}")
        logger.error(f"System error: {e}")
        return 1

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
