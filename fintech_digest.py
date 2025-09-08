#!/usr/bin/env python3 
# -*- coding: utf-8 -*-
"""
PRODUCTION FinTech Digest System - Enhanced Version v2.1.1 - Production Ready
=====================================================================================================

CRITICAL ENHANCEMENTS v2.1.1:
1. ENHANCED: Region filtering now excludes Africa in addition to specific Asian countries
2. ENHANCED: Email delivery limited to 20-30 highest quality articles
3. ENHANCED: Semantic scoring limited to 60 articles total across all trends
4. ENHANCED: Removed metadata trend from configuration
5. MAINTAINED: All working functionality from previous version

ARCHITECTURE ENHANCEMENTS:
- Split large classes into focused components
- Removed global state dependencies
- Added database connection pooling with fallback
- Enhanced error handling with specific exceptions
- Added configuration validation
- Improved input validation
- Better separation of concerns
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
from urllib.parse import urlparse
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
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install aiohttp feedparser beautifulsoup4 scikit-learn numpy python-dotenv")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fintech_digest.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
    def validate(config: 'Config') -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Required API keys
        if config.llm_enabled and not config.llm_api_key:
            errors.append("LLM_API_KEY required when LLM_INTEGRATION_ENABLED=1")
        
        if not config.google_api_key:
            errors.append("GOOGLE_SEARCH_API_KEY is required")
        
        if not config.google_search_engine_id:
            errors.append("GOOGLE_SEARCH_ENGINE_ID is required")
        
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
        if not 10 <= config.max_email_articles <= 50:
            errors.append("MAX_EMAIL_ARTICLES must be between 10 and 50")
        
        if not 20 <= config.semantic_total_limit <= 200:
            errors.append("SEMANTIC_TOTAL_LIMIT must be between 20 and 200")
        
        return errors

class Config:
    """Centralized configuration management with validation"""
    
    def __init__(self):
        # Core settings
        self.relevance_threshold = float(os.getenv("RELEVANCE_THRESHOLD", "0.58"))
        self.semantic_scoring_enabled = os.getenv("SEMANTIC_SCORING_ENABLED", "1") == "1"
        self.keyword_expansion_enabled = os.getenv("KEYWORD_EXPANSION_ENABLED", "1") == "1"
        self.strict_region_filter = os.getenv("STRICT_REGION_FILTER", "1") == "1"
        self.keep_historical_days = self._safe_int("KEEP_HISTORICAL_DAYS", 365)
        
        # NEW: Email and semantic limits
        self.max_email_articles = self._safe_int("MAX_EMAIL_ARTICLES", 25)
        self.semantic_total_limit = self._safe_int("SEMANTIC_TOTAL_LIMIT", 60)
        
        # LLM integration settings
        self.llm_enabled = os.getenv("LLM_INTEGRATION_ENABLED", "1") == "1"
        self.llm_api_key = os.getenv("LLM_API_KEY", "").strip()
        self.llm_model = os.getenv("LLM_MODEL", "gpt-4o-mini").strip()
        self.llm_temperature = float(os.getenv("LLM_TEMPERATURE", "0.1"))
        self.llm_max_retries = self._safe_int("LLM_MAX_RETRIES", 3)
        self.llm_batch_size = self._safe_int("LLM_BATCH_SIZE", 5)

        # Hard caps & gates
        self.llm_max_tokens_title = self._safe_int("LLM_MAX_TOKENS_TITLE", 32)
        self.llm_max_tokens_summary = self._safe_int("LLM_MAX_TOKENS_SUMMARY", 120)

        # Summaries: rule-based by default; LLM fallback only
        self.llm_summary_fallback_enabled = os.getenv("LLM_SUMMARY_FALLBACK_ENABLED", "1") == "1"
        self.llm_summary_min_chars_trigger = self._safe_int("LLM_SUMMARY_MIN_CHARS_TRIGGER", 60)
        self.llm_summary_long_content_trigger = self._safe_int("LLM_SUMMARY_LONG_CONTENT_TRIGGER", 3000)

        # Optional batch-queueing (JSONL) — not used by default here
        self.llm_batch_enabled = os.getenv("LLM_BATCH_ENABLED", "0").lower() in ("1", "true", "yes")
        self.llm_batch_file = os.getenv("LLM_BATCH_FILE", "./batch_requests.jsonl")

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
        self.database_path = os.getenv('DATABASE_PATH', 'fintech_digest.db')
        
        # Trend config JSON (optional)
        self.trend_config_json = os.getenv('TREND_CONFIG_JSON', 'trend_config.json')
        
        # Validate configuration
        self._validate()
        
        logger.info(f"Configuration loaded - Relevance threshold: {self.relevance_threshold}")
        logger.info(f"LLM: {'ENABLED' if self.llm_enabled else 'DISABLED'} • Model: {self.llm_model}")
        logger.info(f"Semantic scoring: {'ENABLED' if self.semantic_scoring_enabled else 'DISABLED'} (limit: {self.semantic_total_limit})")
        logger.info(f"Email limit: {self.max_email_articles} articles")
        logger.info(f"Keyword expansion: {'ENABLED' if self.keyword_expansion_enabled else 'DISABLED'}")
    
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
# Enhanced Region Filter (Now includes Africa)
# --------------------------------------------------------------------------------------

class EnhancedRegionFilter:
    """Enhanced region filtering - now excludes Africa as well"""
    
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

class RobustDatabaseManager:
    """Enhanced SQLite database with connection pooling and debugging"""
    
    def __init__(self, db_path: str = None, config: Config = None):
        self.db_path = db_path or (config.database_path if config else 'fintech_digest.db')
        self.keep_days = config.keep_historical_days if config else 365
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
                    source_type TEXT,
                    domain TEXT,
                    trend_category TEXT,
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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE UNIQUE NOT NULL,
                    total_articles INTEGER,
                    google_articles INTEGER,
                    rss_articles INTEGER,
                    avg_relevance REAL,
                    avg_semantic_relevance REAL DEFAULT 0.0,
                    processing_time REAL,
                    trends_with_articles INTEGER,
                    email_articles INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            try:
                conn.execute('ALTER TABLE articles ADD COLUMN semantic_relevance_score REAL DEFAULT 0.0')
                logger.debug("Added semantic_relevance_score column")
            except sqlite3.OperationalError:
                pass
    
    def _create_indexes(self):
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_articles_trend_date ON articles(trend_category, processed_date)',
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
            
            with self._get_connection() as conn:
                conn.execute('SELECT 1').fetchone()
                
                conn.execute('''
                    INSERT OR REPLACE INTO articles 
                    (url, title, original_title, enhanced_title, content, summary, source_type, domain,
                     trend_category, relevance_score, semantic_relevance_score, quality_score, region_confidence, 
                     word_count, content_hash, published_date, search_keywords, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    article.url, article.title, article.original_title, article.enhanced_title,
                    article.content, article.summary, article.source_type, article.domain,
                    article.trend_category, article.relevance_score, article.semantic_relevance_score,
                    article.quality_score, article.region_confidence, article.word_count, 
                    article.content_hash, article.published_date, keywords_json
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
                            (url, title, original_title, enhanced_title, content, summary, source_type, domain,
                             trend_category, relevance_score, semantic_relevance_score, quality_score, region_confidence, 
                             word_count, content_hash, published_date, search_keywords, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ''', (
                            article.url, article.title, article.original_title, article.enhanced_title,
                            article.content, article.summary, article.source_type, article.domain,
                            article.trend_category, article.relevance_score, article.semantic_relevance_score,
                            article.quality_score, article.region_confidence, article.word_count, 
                            article.content_hash, article.published_date, keywords_json
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
                    (date, total_articles, google_articles, rss_articles, 
                     avg_relevance, avg_semantic_relevance, processing_time, trends_with_articles, email_articles)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    today, stats.get('total_articles', 0), stats.get('google_articles', 0),
                    stats.get('rss_articles', 0), stats.get('avg_relevance', 0.0),
                    stats.get('avg_semantic_relevance', 0.0), stats.get('processing_time', 0.0), 
                    stats.get('trends_with_articles', 0), stats.get('email_articles', 0)
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
                    WHERE processed_date >= ? AND content_hash IS NOT NULL
                ''', (cutoff_date,)).fetchall()
            return {h[0] for h in hashes}
        except Exception as e:
            logger.error(f"Failed to get recent hashes: {e}")
            return set()
    
    def cleanup_old_data(self):
        try:
            cutoff_date = datetime.date.today() - datetime.timedelta(days=self.keep_days)
            with self._get_connection() as conn:
                result = conn.execute('DELETE FROM articles WHERE processed_date < ?', (cutoff_date,))
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
                trend_counts = conn.execute('''
                    SELECT trend_category, COUNT(*) FROM articles 
                    GROUP BY trend_category ORDER BY COUNT(*) DESC
                ''').fetchall()
                stats['articles_by_trend'] = dict(trend_counts)
                week_ago = datetime.date.today() - datetime.timedelta(days=7)
                stats['recent_articles'] = conn.execute('''
                    SELECT COUNT(*) FROM articles WHERE processed_date >= ?
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
# LLM Integration with Enhanced Error Handling
# --------------------------------------------------------------------------------------

class EnhancedLLMIntegration:
    """Enhanced LLM integration with better error handling"""
    
    def __init__(self, config: Config):
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
        """Title enhancement with retry logic"""
        if not self.enabled or not title:
            return title

        if not force:
            needs_enhancement = (
                len(title) > 120 or title.count(' | ') > 1 or title.count(' - ') > 1
                or title.endswith('...') or '...' in title
                or ' - ' in title[-40:] or ' | ' in title[-40:]
            )
            if not needs_enhancement:
                return title
            prompt = (
                "You are a financial newsletter editor. "
                "Clean this title for a payments/fintech digest: remove site names, fix truncation, "
                "keep key entities, add one concrete signal if present, and keep it under 100 characters. "
                "Return only the cleaned title.\n\n"
                f"Original: {title}\n\nCleaned title:"
            )
            max_tok = self.config.llm_max_tokens_title
        else:
            prompt = (
                "You are a financial newsletter editor for payments/fintech readers.\n"
                "Rewrite the article title to be precise and informative.\n"
                "Constraints: 12–16 words; neutral tone; keep key entities; include one concrete signal "
                "(metric, region, regulation, product, or action); American English; do not invent facts.\n\n"
                f"Original title:\n{title}\n\n"
                f"Context (snippet):\n{(content or '')[:600]}\n\n"
                "Return only the rewritten title."
            )
            max_tok = self.config.llm_max_tokens_title

        try:
            enhanced = await self._call_llm(prompt, max_tokens=max_tok)
            cleaned = (enhanced or "").strip().strip('"\'')
            if 15 <= len(cleaned) <= 140 and not cleaned.lower().startswith('error'):
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
# Keyword Expansion System
# --------------------------------------------------------------------------------------

class KeywordExpansionSystem:
    """Semantic keyword expansion for better content discovery"""
    
    def __init__(self, config: Config):
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
# Data Structures
# --------------------------------------------------------------------------------------

@dataclass
class ContentSource:
    """Enhanced content source with validation"""
    url: str
    title: str
    source_type: str
    domain: str
    published_date: Optional[datetime.datetime] = None
    relevance_score: float = 0.0
    semantic_relevance_score: float = 0.0
    content: str = ""
    summary: str = ""
    word_count: int = 0
    last_checked: Optional[datetime.datetime] = None
    content_hash: str = ""
    trend_category: str = ""
    search_keywords: List[str] = field(default_factory=list)
    original_title: str = ""
    display_title: str = ""
    enhanced_title: str = ""
    region_confidence: float = 1.0
    quality_score: float = 0.0
    
    def __post_init__(self):
        if not self.url or not self.title:
            raise ValueError("URL and title are required")
        if not self.original_title:
            self.original_title = str(self.title).strip()
        self.title = self.original_title
        if not self.display_title:
            self.display_title = self.title
        if not self.content_hash:
            content = f"{self.title}{self.content}"
            self.content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        if not self.word_count and self.content:
            self.word_count = len(self.content.split())
        if not self.last_checked:
            self.last_checked = datetime.datetime.now()

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

@dataclass
class TrendAnalysis:
    """Historical trend analysis data"""
    trend_name: str
    period_days: int
    total_articles: int
    avg_relevance: float
    top_domains: List[Tuple[str, int]]
    keyword_frequency: Dict[str, int]
    growth_rate: float
    sentiment_score: float = 0.0

# --------------------------------------------------------------------------------------
# Google Search Integration
# --------------------------------------------------------------------------------------

class GoogleSearchIntegration:
    """Enhanced Google Search API integration"""
    
    def __init__(self, config: Config, region_filter: EnhancedRegionFilter, keyword_expander: KeywordExpansionSystem):
        self.config = config
        self.region_filter = region_filter
        self.keyword_expander = keyword_expander
        self.enabled = bool(config.google_api_key and config.google_search_engine_id)
        self.queries_today = 0
        
        if not self.enabled:
            raise ConfigurationError("Google Search API not configured")
        
        logger.info(f"Google Search API enabled with daily limit: {config.google_daily_limit}")
    
    async def search_trend(self, trend_name: str, base_keywords: List[str], max_results: int = 20) -> List[ContentSource]:
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
    
    async def _process_search_result(self, item: Dict, trend_name: str, keywords: List[str]) -> Optional[ContentSource]:
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
        display_title = cleaned_title
        published_date = self._extract_published_date(item)
        domain = urlparse(url).netloc
        source = ContentSource(
            url=url,
            title=cleaned_title,
            original_title=full_title,
            display_title=display_title,
            source_type='google_search',
            domain=domain,
            content=snippet,
            published_date=published_date,
            trend_category=trend_name,
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
            r'\s*â€"\s*.*$'
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

# --------------------------------------------------------------------------------------
# RSS Feed Validator
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

    async def extract_from_rss_preserve_titles(self, rss_url: str, source_name: str) -> List[ContentSource]:
        sources = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; FinTechDigest/2.1; +https://example.com/bot)',
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
        source = ContentSource(
            url=url,
            title=full_title,
            original_title=full_title_raw,
            display_title=full_title,
            source_type='rss',
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
            r'\s*-\s*[^-]*(?:\.com|\.org|\.net|News|Times|Post|Journal).*$',
            r'\s*\|\s*[^|]*(?:\.com|\.org|\.net|News|Times|Post|Journal).*$',
            r'\s*::\s*.*$',
            r'\s*â€"\s*.*$',
            r'\s*â€"\s*.*$'
        ]
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', cleaned).strip()
        if len(cleaned) < len(title) * 0.5:
            cleaned = title.strip()
        return cleaned

# --------------------------------------------------------------------------------------
# Article Processor
# --------------------------------------------------------------------------------------

class ArticleProcessor:
    """Handles article processing pipeline"""
    
    def __init__(self, config: Config, region_filter: EnhancedRegionFilter, llm_integration: EnhancedLLMIntegration):
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
    
    async def process_article(self, article: ContentSource, trend_config: TrendConfig) -> ContentSource:
        """Process a single article through the complete pipeline"""
        try:
            # Calculate relevance
            article.relevance_score = self.calculate_relevance_score(
                article.content, article.title, trend_config.keywords
            )
            
            # Update quality score
            if hasattr(article, 'region_confidence'):
                article.quality_score = (article.relevance_score + article.region_confidence) / 2
            else:
                article.quality_score = article.relevance_score
            
            # Title enhancement (conservative)
            if self.llm_integration.enabled and not article.enhanced_title:
                article.enhanced_title = await self.llm_integration.enhance_title(
                    article.title, article.content, force=False
                )
            
            # Generate summary (rule-based first)
            if not article.summary:
                article.summary = await self.summarizer.summarize_article(
                    article.content, article.enhanced_title or article.title
                )
            
            # LLM summary fallback
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
# Content Aggregator
# --------------------------------------------------------------------------------------

class ContentAggregator:
    """Manages RSS and Google Search collection"""
    
    def __init__(self, config: Config, google_search: GoogleSearchIntegration, rss_validator: EnhancedRSSFeedValidator):
        self.config = config
        self.google_search = google_search
        self.rss_validator = rss_validator
    
    async def collect_content_for_trend(self, trend_key: str, trend_config: TrendConfig) -> List[ContentSource]:
        """Collect content from all sources for a single trend"""
        articles = []
        
        # Google Search
        if trend_config.google_search_enabled:
            try:
                google_articles = await self.google_search.search_trend(
                    trend_key, trend_config.keywords, max_results=20
                )
                articles.extend(google_articles)
                logger.info(f"Google Search: {len(google_articles)} articles for {trend_key}")
            except Exception as e:
                logger.error(f"Google Search error for {trend_key}: {e}")
        
        # RSS feeds
        for rss_url in trend_config.rss_feeds:
            try:
                rss_articles = await self.rss_validator.extract_from_rss_preserve_titles(
                    rss_url, f"{trend_key}_rss"
                )
                articles.extend(rss_articles)
                domain = urlparse(rss_url).netloc
                logger.info(f"RSS: {len(rss_articles)} articles from {domain} for {trend_key}")
            except Exception as e:
                domain = urlparse(rss_url).netloc
                logger.error(f"RSS error for {domain}: {e}")
        
        return articles

# --------------------------------------------------------------------------------------
# Email Generator
# --------------------------------------------------------------------------------------

class ProductionEmailGenerator:
    """Enhanced email generator with improved formatting"""
    
    def __init__(self, config: Config, llm_integration: EnhancedLLMIntegration, summarizer: ArticleSummarizer):
        self.config = config
        self.llm_integration = llm_integration
        self.summarizer = summarizer
        self.all_trends = {
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
            "embedded_finance": {"name": "Embedded Finance & BaaS", "emoji": "🔗"},
            "regtech_compliance": {"name": "RegTech & Compliance", "emoji": "⚖️"}
        }
    
    async def generate_email(self, trend_data: Dict[str, List[ContentSource]], 
                            stats: Dict, database_stats: Dict = None) -> str:
        try:
            today = datetime.date.today()
            date_str = today.strftime("%B %d, %Y")
            content_sections = []
            total_articles = 0
            google_articles = 0
            rss_articles = 0
            high_quality_articles = 0
            semantic_scored_articles = 0
            
            for trend_key, articles in trend_data.items():
                trend_info = self.all_trends.get(trend_key, {
                    "name": trend_key.replace("_", " ").title(), 
                    "emoji": "📊"
                })
                total_articles += len(articles)
                for article in articles:
                    if article.source_type == 'google_search':
                        google_articles += 1
                    elif article.source_type == 'rss':
                        rss_articles += 1
                    if article.relevance_score >= self.config.relevance_threshold:
                        high_quality_articles += 1
                    if article.semantic_relevance_score > 0:
                        semantic_scored_articles += 1
                section_html = await self._generate_trend_section(trend_key, trend_info, articles)
                content_sections.append(section_html)
            
            content = ''.join(content_sections)
            db_info = ""
            if database_stats:
                db_info = f" • DB: {database_stats.get('total_articles', 0)} articles"
            llm_info = " • LLM Enhanced" if self.llm_integration.enabled else ""
            semantic_info = f" • {semantic_scored_articles} semantic scored" if semantic_scored_articles > 0 else ""
            
            html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FinTech Trends Digest - {date_str}</title>
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
        .trend-section {{
            margin-bottom: 30px;
        }}
        .trend-header {{
            margin-bottom: 15px;
        }}
        .trend-header h2 {{
            margin: 0;
            font-size: 18px;
            font-weight: bold;
            color: #000;
        }}
        .article {{
            margin-bottom: 20px;
            padding: 0;
        }}
        .article-title {{
            color: #0066cc;
            text-decoration: none;
            display: block;
            margin-bottom: 8px;
            font-size: 16px;
            line-height: 1.3;
            font-weight: normal;
        }}
        .article-title:hover {{
            text-decoration: underline;
        }}
        .article-summary {{
            color: #333;
            font-size: 14px;
            margin-bottom: 8px;
            line-height: 1.4;
        }}
        .article-meta {{
            font-size: 12px;
            color: #666;
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding: 20px;
            font-size: 12px;
            color: #666;
            border-top: 1px solid #ddd;
        }}
        .no-update {{
            padding: 20px 0;
            color: #666;
            font-style: italic;
        }}
        .badge {{
            background-color: #f0f0f0;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
            margin-right: 4px;
        }}
        .quality-high {{
            background-color: #e6f7e6;
            color: #2e7d32;
        }}
        .quality-medium {{
            background-color: #fff3e0;
            color: #ef6c00;
        }}
        .quality-score {{
            background-color: #e3f2fd;
            color: #1565c0;
        }}
        .semantic-score {{
            background-color: #f3e5f5;
            color: #7b1fa2;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>FinTech Trends Digest v2.1.1</h1>
        <p>{date_str}</p>
        <div class="stats">
            {total_articles} articles • {high_quality_articles} high-quality • {stats.get('processing_time', 0):.1f}s{db_info}{llm_info}{semantic_info}
        </div>
    </div>
    
    {content}
    
    <div class="footer">
        <p>Enhanced FinTech Intelligence System v2.1.1</p>
        <p>Region Filtering: US/EU • Email Limit: {self.config.max_email_articles} • Semantic Limit: {self.config.semantic_total_limit}</p>
    </div>
</body>
</html>"""
            return html_template
        except Exception as e:
            logger.error(f"Email generation failed: {e}")
            return f"<html><body><h2>Error generating digest: {e}</h2></body></html>"
    
    async def _generate_trend_section(self, trend_key: str, trend_info: Dict, articles: List[ContentSource]) -> str:
        """Generate enhanced section for a trend"""
        try:
            emoji = trend_info['emoji']
            name = trend_info['name']
            if not articles:
                return f"""
                <div class="trend-section">
                    <div class="trend-header">
                        <h2>{emoji} {name}</h2>
                    </div>
                    <div class="no-update">
                        No high-quality updates found (45%+ relevance required)
                    </div>
                </div>
                """
            articles_html = ""
            for i, article in enumerate(articles, 1):
                try:
                    relevance_pct = int(article.relevance_score * 100)
                    semantic_pct = int(article.semantic_relevance_score * 100) if article.semantic_relevance_score > 0 else 0
                    article_date = article.published_date.strftime("%Y-%m-%d") if article.published_date else ""
                    
                    # Force LLM title rewrite for email-visible items
                    display_title = article.enhanced_title or article.display_title or article.title
                    if self.llm_integration.enabled:
                        forced = await self.llm_integration.enhance_title(display_title, article.content, force=True)
                        if forced:
                            display_title = forced
                            article.enhanced_title = forced
                    
                    # Ensure summary exists
                    if not article.summary:
                        article.summary = self.summarizer._rule_based_summarization(article.content, display_title)
                    
                    quality_class = "quality-high" if relevance_pct >= 70 else "quality-medium"
                    quality_text = "EXCELLENT" if relevance_pct >= 90 else "HIGH" if relevance_pct >= 70 else "GOOD"
                    badges = f'<span class="badge {quality_class}">{quality_text}</span>'
                    badges += f'<span class="badge quality-score">{relevance_pct}% match</span>'
                    if semantic_pct > 0:
                        badges += f'<span class="badge semantic-score">{semantic_pct}% semantic</span>'
                    region_pct = int(article.region_confidence * 100) if hasattr(article, 'region_confidence') else 100
                    if region_pct < 100:
                        badges += f'<span class="badge quality-score">{region_pct}% US/EU</span>'
                    
                    articles_html += f"""
                    <div class="article">
                        <a href="{article.url}" class="article-title" target="_blank">{display_title}</a>
                        <div class="article-summary">{article.summary}</div>
                        <div class="article-meta">
                            <div>{badges}</div>
                            <div>{article.domain}{f' • {article_date}' if article_date else ''} • {article.source_type.upper()}</div>
                        </div>
                    </div>
                    """
                except Exception as e:
                    logger.error(f"Error generating article HTML: {e}")
                    continue
            
            return f"""
            <div class="trend-section">
                <div class="trend-header">
                    <h2>{emoji} {name}</h2>
                    <span style="font-size: 14px; color: #6c757d;">{len(articles)} articles</span>
                </div>
                {articles_html}
            </div>
            """
        except Exception as e:
            logger.error(f"Error generating trend section: {e}")
            return ""

# --------------------------------------------------------------------------------------
# Main Digest Orchestrator with NEW Email and Semantic Limits
# --------------------------------------------------------------------------------------

class DigestOrchestrator:
    """Coordinates the overall digest process with enhanced limits"""
    
    def __init__(self, config: Config):
        self.config = config
        
        # Initialize components with dependency injection
        self.region_filter = EnhancedRegionFilter()
        self.llm_integration = EnhancedLLMIntegration(config)
        self.keyword_expander = KeywordExpansionSystem(config)
        self.database = RobustDatabaseManager(config=config)
        self.google_search = GoogleSearchIntegration(config, self.region_filter, self.keyword_expander)
        self.rss_validator = EnhancedRSSFeedValidator(self.region_filter)
        self.content_aggregator = ContentAggregator(config, self.google_search, self.rss_validator)
        self.article_processor = ArticleProcessor(config, self.region_filter, self.llm_integration)
        self.email_generator = ProductionEmailGenerator(config, self.llm_integration, ArticleSummarizer())
        
        # Load trends (without metadata)
        self.trends = self._load_trends()
        
        # Initialize stats
        self.stats = {
            'processing_time': 0,
            'start_time': time.time(),
            'google_articles_found': 0,
            'rss_articles_found': 0,
            'high_quality_articles': 0,
            'email_articles': 0,
            'articles_processed': 0,
            'total_articles_saved': 0,
            'semantic_scored_articles': 0,
            'avg_semantic_relevance': 0.0
        }
        
        logger.info(f"Enhanced system v2.1.1 initialized with {len(self.trends)} trends")
        logger.info(f"Email relevance filter: {config.relevance_threshold*100:.0f}%")
        logger.info(f"Email limit: {config.max_email_articles} articles")
        logger.info(f"Semantic scoring: {'ENABLED' if self.llm_integration.semantic_scoring_enabled else 'DISABLED'} (limit: {config.semantic_total_limit})")
        logger.info(f"Keyword expansion: {'ENABLED' if config.keyword_expansion_enabled else 'DISABLED'}")
        logger.info(f"Region filtering: Enhanced - excludes Asia and Africa")

    def _load_trends(self) -> Dict[str, TrendConfig]:
        """Load trend configurations (without metadata)"""
        try:
            cfg_path = getattr(self.config, "trend_config_json", "trend_config.json")
            if cfg_path and os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                trends: Dict[str, TrendConfig] = {}
                for key, val in raw.items():
                    # Skip metadata entries
                    if key.startswith("_"):
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
        
        # Default configurations
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

    def _is_recent(self, dt: Optional[datetime.datetime], days: int = 3) -> bool:
        """Check if datetime is recent"""
        if not dt:
            return True
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
        return dt >= cutoff

    async def run_production_digest(self) -> Dict[str, Any]:
        """Run the complete digest generation process with enhanced limits"""
        try:
            print("Starting enhanced production digest v2.1.1...")
            self.database.cleanup_old_data()
            recent_hashes = self.database.get_recent_article_hashes(7)
            all_trend_data = {trend_key: [] for trend_key in self.trends.keys()}
            email_trend_data = {trend_key: [] for trend_key in self.trends.keys()}
            all_articles = []
            semantic_scored_count = 0  # Track total semantic scoring
            
            for trend_key, trend_config in self.trends.items():
                print(f"\nProcessing trend: {trend_config.name}")
                
                # Collect articles from all sources
                articles = await self.content_aggregator.collect_content_for_trend(trend_key, trend_config)
                self.stats['google_articles_found'] += len([a for a in articles if a.source_type == 'google_search'])
                self.stats['rss_articles_found'] += len([a for a in articles if a.source_type == 'rss'])
                
                saved_articles: List[ContentSource] = []
                email_articles: List[ContentSource] = []

                # Process articles through the pipeline
                for article in articles:
                    try:
                        article.trend_category = trend_key
                        
                        # Process article through complete pipeline
                        processed_article = await self.article_processor.process_article(article, trend_config)
                        
                        # Save articles above minimum threshold
                        if processed_article.relevance_score >= trend_config.min_relevance_score:
                            if not self.article_processor.is_duplicate(processed_article, recent_hashes, all_articles):
                                
                                # Save to database
                                if self.database.save_article(processed_article):
                                    saved_articles.append(processed_article)
                                    all_articles.append(processed_article)
                                    self.stats['total_articles_saved'] += 1
                                    
                                    # Email inclusion check
                                    if (processed_article.quality_score >= trend_config.email_relevance_score and 
                                        self._is_recent(processed_article.published_date, days=3)):
                                        email_articles.append(processed_article)
                                        self.stats['high_quality_articles'] += 1
                    except Exception as e:
                        logger.error(f"Article processing error: {e}")
                        continue

                # Semantic scoring for top articles only (respecting TOTAL limit)
                if (self.llm_integration.semantic_scoring_enabled and 
                    saved_articles and 
                    semantic_scored_count < self.config.semantic_total_limit):
                    
                    # Calculate how many more we can score
                    remaining_semantic_quota = self.config.semantic_total_limit - semantic_scored_count
                    k = min(6, len(saved_articles), remaining_semantic_quota)  # Max 6 per trend, but respect total limit
                    
                    if k > 0:
                        top_for_semantics = sorted(saved_articles, key=lambda a: a.relevance_score, reverse=True)[:k]
                        
                        print(f"  Running semantic scoring for top {len(top_for_semantics)} of {len(saved_articles)} saved articles...")
                        print(f"  Semantic quota: {semantic_scored_count}/{self.config.semantic_total_limit} used")
                        
                        for article in top_for_semantics:
                            try:
                                semantic_score = await self.llm_integration.evaluate_semantic_relevance(
                                    article.title, article.content, trend_config.keywords
                                )
                                if semantic_score > 0:
                                    article.semantic_relevance_score = semantic_score
                                    semantic_scored_count += 1
                                    self.stats['semantic_scored_articles'] += 1
                                    
                                    # Update quality score with semantic component
                                    combined = article.relevance_score * 0.6 + semantic_score * 0.4
                                    article.quality_score = (combined + article.region_confidence) / 2
                                    
                                    # Re-save with updated semantic score
                                    self.database.save_article(article)
                                    
                                    # Stop if we've hit the total limit
                                    if semantic_scored_count >= self.config.semantic_total_limit:
                                        break
                            except Exception as e:
                                logger.debug(f"Semantic scoring failed for article: {e}")
                                continue

                # Sort by quality
                saved_articles.sort(key=lambda x: x.quality_score, reverse=True)
                email_articles.sort(key=lambda x: x.quality_score, reverse=True)

                # Check accessibility for email articles
                if email_articles:
                    print(f"  Checking accessibility of {len(email_articles)} email articles...")
                    accessibility_checks = await asyncio.gather(
                        *[self._is_url_accessible(a.url) for a in email_articles], 
                        return_exceptions=True
                    )
                    accessible_articles = []
                    for article, is_accessible in zip(email_articles, accessibility_checks):
                        if is_accessible is True:
                            accessible_articles.append(article)
                        else:
                            logger.debug(f"Excluding inaccessible URL: {article.url}")
                    email_articles = accessible_articles

                all_trend_data[trend_key] = saved_articles
                email_trend_data[trend_key] = email_articles[:trend_config.max_articles]
                
                print(f"  Saved: {len(saved_articles)} articles")
                print(f"  Email: {len(email_trend_data[trend_key])} high-quality articles")
                if self.llm_integration.semantic_scoring_enabled:
                    semantic_count = sum(1 for a in saved_articles if a.semantic_relevance_score > 0)
                    print(f"  Semantic: {semantic_count} articles with LLM scores")
            
            # ENHANCED: Limit total email articles to MAX_EMAIL_ARTICLES
            all_email_articles = []
            for articles in email_trend_data.values():
                all_email_articles.extend(articles)
            
            # Sort all email articles by quality and limit to max
            all_email_articles.sort(key=lambda x: x.quality_score, reverse=True)
            limited_email_articles = all_email_articles[:self.config.max_email_articles]
            
            # Redistribute limited articles back to trends for email generation
            final_email_trend_data = {trend_key: [] for trend_key in self.trends.keys()}
            for article in limited_email_articles:
                final_email_trend_data[article.trend_category].append(article)
            
            print(f"\n📧 EMAIL CURATION:")
            print(f"   Total email candidates: {len(all_email_articles)}")
            print(f"   Limited to top: {len(limited_email_articles)} articles")
            print(f"   Email limit: {self.config.max_email_articles}")
            
            # Calculate final stats
            self.stats['processing_time'] = time.time() - self.stats['start_time']
            self.stats['articles_processed'] = len(all_articles)
            self.stats['email_articles'] = len(limited_email_articles)
            
            if limited_email_articles:
                avg_relevance = sum(a.relevance_score for a in limited_email_articles) / len(limited_email_articles)
                semantic_articles = [a for a in limited_email_articles if a.semantic_relevance_score > 0]
                avg_semantic_relevance = (
                    sum(a.semantic_relevance_score for a in semantic_articles) / len(semantic_articles)
                    if semantic_articles else 0.0
                )
            else:
                avg_relevance = 0.0
                avg_semantic_relevance = 0.0
            
            self.stats['avg_semantic_relevance'] = avg_semantic_relevance
            
            enhanced_stats = {
                **self.stats,
                'avg_relevance': avg_relevance,
                'avg_semantic_relevance': avg_semantic_relevance,
                'trends_with_articles': sum(1 for articles in final_email_trend_data.values() if articles)
            }
            
            self.database.save_daily_stats(enhanced_stats)
            database_stats = self.database.get_database_stats()
            
            # Generate and send email
            try:
                html_content = await self.email_generator.generate_email(
                    final_email_trend_data, enhanced_stats, database_stats
                )
                email_sent = self._send_email(html_content)
                if email_sent:
                    print("✅ Email sent successfully!")
                else:
                    print("❌ Email not sent")
            except Exception as e:
                print(f"❌ Email error: {e}")
                email_sent = False
            
            # Console summary
            print(f"\n📊 DIGEST SUMMARY:")
            print(f"   Total articles saved: {self.stats['total_articles_saved']}")
            print(f"   Email articles: {self.stats['email_articles']} (limit: {self.config.max_email_articles})")
            print(f"   Google articles: {self.stats['google_articles_found']}")
            print(f"   RSS articles: {self.stats['rss_articles_found']}")
            if self.llm_integration.enabled:
                llm_stats = self.llm_integration.get_usage_stats()
                print(f"   LLM usage: {llm_stats['daily_requests']} calls, {llm_stats['daily_tokens']} tokens, ${llm_stats['daily_cost']:.4f}")
                print(f"   LLM model: {llm_stats['model']}")
            if self.llm_integration.semantic_scoring_enabled:
                print(f"   Semantic scored: {self.stats['semantic_scored_articles']}/{self.config.semantic_total_limit}")
                print(f"   Avg semantic score: {avg_semantic_relevance:.2f}")
            print(f"   Processing time: {self.stats['processing_time']:.1f}s")
            print(f"   Database size: {database_stats.get('db_size_mb', 0):.1f}MB")
            
            return {
                'success': True,
                'total_articles_saved': self.stats['total_articles_saved'],
                'email_articles': self.stats['email_articles'],
                'high_quality_articles': self.stats['high_quality_articles'],
                'google_articles': self.stats['google_articles_found'],
                'rss_articles': self.stats['rss_articles_found'],
                'semantic_scored_articles': self.stats['semantic_scored_articles'],
                'email_sent': email_sent,
                'processing_time': self.stats['processing_time'],
                'avg_relevance': avg_relevance,
                'avg_semantic_relevance': avg_semantic_relevance,
                'database_stats': database_stats,
                'llm_enabled': self.llm_integration.enabled,
                'semantic_scoring_enabled': self.llm_integration.semantic_scoring_enabled,
                'semantic_total_limit': self.config.semantic_total_limit,
                'max_email_articles': self.config.max_email_articles
            }
        except Exception as e:
            print(f"❌ Production digest failed: {e}")
            logger.error(f"Production digest failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def _send_email(self, html_content: str) -> bool:
        """Send email digest"""
        try:
            if not self.config.smtp_user or not self.config.smtp_password or not self.config.email_recipients:
                print("⚠️ Email configuration incomplete")
                return False
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"FinTech Trends Digest v2.1.1 - {datetime.date.today().strftime('%B %d, %Y')}"
            msg['From'] = f"Enhanced FinTech Digest <{self.config.smtp_user}>"
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
# Main Production System Class (Backward Compatibility)
# --------------------------------------------------------------------------------------

class ProductionFintechDigestSystem(DigestOrchestrator):
    """Main system class for backward compatibility"""
    
    def __init__(self):
        try:
            config = Config()  # This will validate configuration
            super().__init__(config)
            
            print(f"Enhanced system v2.1.1 initialized with {len(self.trends)} trends")
            print(f"Email relevance filter: {config.relevance_threshold*100:.0f}%")
            print(f"Email limit: {config.max_email_articles} articles")
            print(f"Semantic scoring: {'ENABLED' if self.llm_integration.semantic_scoring_enabled else 'DISABLED'} (limit: {config.semantic_total_limit})")
            print(f"Keyword expansion: {'ENABLED' if config.keyword_expansion_enabled else 'DISABLED'}")
            print(f"Region filtering: Enhanced - excludes Asia and Africa")
        except Exception as e:
            logger.error(f"System initialization failed: {e}")
            raise

# --------------------------------------------------------------------------------------
# Main Function
# --------------------------------------------------------------------------------------

async def main():
    """Enhanced main function with comprehensive options"""
    parser = argparse.ArgumentParser(description="Enhanced FinTech Digest System v2.1.1")
    parser.add_argument("--test", action="store_true", help="Test mode - validate configuration only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--config-check", action="store_true", help="Check configuration and exit")
    parser.add_argument("--validate-feeds", action="store_true", help="Validate all RSS feeds and exit")
    parser.add_argument("--config-json", type=str, default=None, help="Path to trend config JSON")
    parser.add_argument("--disable-llm", action="store_true", help="Disable LLM integration for this run")
    parser.add_argument("--disable-semantic", action="store_true", help="Disable semantic scoring for this run")
    parser.add_argument("--test-single-trend", type=str, help="Test processing for a single trend")
    
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Create a temporary config for CLI argument processing
    temp_env = dict(os.environ)
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
                config = Config()
                print("🔧 CONFIGURATION CHECK:")
                print(f"   Relevance threshold: {config.relevance_threshold}")
                print(f"   Region filter: {'ENABLED' if config.strict_region_filter else 'DISABLED'} (excludes Asia & Africa)")
                print(f"   LLM integration: {'ENABLED' if config.llm_enabled else 'DISABLED'}")
                print(f"   Semantic scoring: {'ENABLED' if config.semantic_scoring_enabled else 'DISABLED'} (limit: {config.semantic_total_limit})")
                print(f"   Email limit: {config.max_email_articles} articles")
                print(f"   Keyword expansion: {'ENABLED' if config.keyword_expansion_enabled else 'DISABLED'}")
                print(f"   Google CSE daily limit: {config.google_daily_limit}")
                print(f"   Database path: {config.database_path}")
                print(f"   Trend config JSON: {getattr(config, 'trend_config_json', 'N/A')}")
                print(f"   Email recipients: {len(config.email_recipients)} configured")
                try:
                    db = RobustDatabaseManager(config=config)
                    stats = db.get_database_stats()
                    print(f"   Database: ✅ Connected ({stats.get('total_articles', 0)} articles)")
                except Exception as e:
                    print(f"   Database: ❌ Error - {e}")
                try:
                    region_filter = EnhancedRegionFilter()
                    keyword_expander = KeywordExpansionSystem(config)
                    google = GoogleSearchIntegration(config, region_filter, keyword_expander)
                    print(f"   Google Search API: ✅ Configured")
                except Exception as e:
                    print(f"   Google Search API: ❌ Error - {e}")
                return 0
            except ConfigurationError as e:
                print(f"❌ Configuration Error:\n{e}")
                return 1

        try:
            digest_system = ProductionFintechDigestSystem()

            if args.validate_feeds:
                print("🔍 VALIDATING RSS FEEDS...")
                unique_feeds = {}
                for trend_key, trend_config in digest_system.trends.items():
                    for url in (trend_config.rss_feeds or []):
                        if url not in unique_feeds:
                            unique_feeds[url] = set()
                        unique_feeds[url].add(trend_config.name)

                print(f"Found {len(unique_feeds)} unique RSS feeds to validate\n")
                
                async def check_feed_with_details(url):
                    print(f"🔍 Checking: {url}")
                    start_time = time.time()
                    is_valid = await digest_system.rss_validator.validate_rss_feed(url)
                    check_time = time.time() - start_time
                    return (url, is_valid, check_time)
                
                results = await asyncio.gather(*[check_feed_with_details(u) for u in unique_feeds.keys()])
                print("\n📊 VALIDATION RESULTS:")
                ok_count = 0
                for url, is_valid, check_time in sorted(results, key=lambda x: x[1], reverse=True):
                    owners = ", ".join(sorted(unique_feeds[url]))
                    status = "✅ OK" if is_valid else "❌ FAIL"
                    if is_valid:
                        ok_count += 1
                    print(f"{status:8} ({check_time:.1f}s) {url}")
                    print(f"         → Used by: {owners}")
                print(f"\n📈 Summary: {ok_count}/{len(results)} feeds accessible ({ok_count/len(results)*100:.1f}%)")
                return 0

            if args.test_single_trend:
                trend_key = args.test_single_trend
                if trend_key not in digest_system.trends:
                    print(f"❌ Unknown trend: {trend_key}")
                    print(f"Available trends: {', '.join(digest_system.trends.keys())}")
                    return 1
                print(f"🧪 TESTING SINGLE TREND: {trend_key}")
                trend_config = digest_system.trends[trend_key]
                if trend_config.google_search_enabled:
                    articles = await digest_system.google_search.search_trend(
                        trend_key, trend_config.keywords, max_results=5
                    )
                    print(f"   Google Search: {len(articles)} articles found")
                    for i, article in enumerate(articles[:3], 1):
                        print(f"   {i}. {article.title[:80]}... ({article.domain})")
                for rss_url in trend_config.rss_feeds:
                    try:
                        articles = await digest_system.rss_validator.extract_from_rss_preserve_titles(
                            rss_url, f"{trend_key}_test"
                        )
                        print(f"   RSS ({urlparse(rss_url).netloc}): {len(articles)} articles")
                        for i, article in enumerate(articles[:2], 1):
                            print(f"   {i}. {article.title[:80]}... ({article.domain})")
                    except Exception as e:
                        print(f"   RSS ({urlparse(rss_url).netloc}): ❌ {e}")
                return 0

            if args.test:
                print("🧪 RUNNING TEST MODE...")
                print("   Configuration validated ✅")
                print("   Database connection tested ✅")
                print("   All systems ready for production ✅")
                return 0

            # Run enhanced production digest
            try:
                print("🚀 Starting Enhanced FinTech Digest v2.1.1...")
                result = await digest_system.run_production_digest()
                if result['success']:
                    print("\n✅ DIGEST COMPLETED SUCCESSFULLY!")
                    print(f"📧 Email sent: {'YES' if result.get('email_sent') else 'NO'}")
                    print(f"📄 Total articles saved: {result.get('total_articles_saved', 0)}")
                    print(f"⭐ High-quality articles: {result.get('high_quality_articles', 0)}")
                    print(f"📧 Email articles: {result.get('email_articles', 0)}/{result.get('max_email_articles', 25)}")
                    if result.get('semantic_scored_articles') is not None:
                        print(f"🧠 Semantic scored: {result.get('semantic_scored_articles', 0)}/{result.get('semantic_total_limit', 60)}")
                    return 0
                else:
                    print(f"\n❌ DIGEST FAILED: {result.get('error', 'Unknown error')}")
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