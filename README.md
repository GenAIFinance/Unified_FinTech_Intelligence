ion, and historical data management
- **Production Ready**: GitHub Actions automation, comprehensive error handling, and monitoring
- **Token Management**: Precise LLM usage controls (25 title enhancements, 15 summaries, 60 semantic scores)

## Quick Start

### Prerequisites

- Python 3.11+
- Google Custom Search API key
- OpenAI API key (optional, for AI features)
- Email account with app password

### Installation
```bash
# Clone repository
git clone https://github.com/yourusername/unified-fintech-intelligence.git
cd unified-fintech-intelligence

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys and settings
Configuration
Create a .env file:
env# Required APIs
GOOGLE_SEARCH_API_KEY=your_google_api_key
GOOGLE_SEARCH_ENGINE_ID=your_search_engine_id
LLM_API_KEY=your_openai_api_key

# Email Configuration
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com

# System Controls
COMPANIES_ENABLED=1
TRENDS_ENABLED=1
LLM_INTEGRATION_ENABLED=1
SEMANTIC_SCORING_ENABLED=1

# LLM Limits
LLM_MAX_TITLE_ENHANCEMENTS=25
LLM_MAX_SUMMARIES=15
SEMANTIC_TOTAL_LIMIT=60
MAX_EMAIL_ARTICLES=40

# Content Quality
RELEVANCE_THRESHOLD=0.58
COMPANY_MAX_AGE_DAYS=3
Usage
bash# Configuration check
python Unified_FinTech_Intelligence.py --config-check

# Test run (no email)
python Unified_FinTech_Intelligence.py --test

# Companies only
python Unified_FinTech_Intelligence.py --companies-only

# Trends only  
python Unified_FinTech_Intelligence.py --trends-only

# Full intelligence run
python Unified_FinTech_Intelligence.py
Monitored Companies
The system tracks these major payments companies:

Visa - Global payments technology
Mastercard - Payment processing and technology
PayPal - Digital payments platform
Stripe - Online payment infrastructure
Toast - Restaurant technology platform
Adyen - Global payment technology
Fiserv - Financial services technology
FIS - Financial technology solutions
Global Payments - Payment processing services

Note: Affirm temporarily disabled due to connectivity issues
Tracked Trends
High Priority:

Instant Payments & RTP: Real-time payments, FedNow, settlement
A2A & Open Banking: Account-to-account, PSD2, banking APIs
Stablecoins & CBDC: Digital currencies, central bank initiatives
Fraud Prevention & AI: Risk management, ML detection

Medium Priority:

Cross-Border Payments: International transfers, remittances
Buy Now, Pay Later: Installment payments, consumer credit
SoftPOS & Tap to Pay: Mobile POS, contactless payments
Payment Orchestration: Smart routing, optimization
Digital Wallets & NFC: Mobile payments, contactless
PCI DSS & Security: Compliance, payment security

Emerging:

Embedded Finance & BaaS: Banking-as-a-Service, partnerships
RegTech & Compliance: Regulatory technology, automation

GitHub Actions Automation
Setup

Add Repository Secrets:

   GOOGLE_SEARCH_API_KEY
   GOOGLE_SEARCH_ENGINE_ID  
   LLM_API_KEY
   SMTP_USER
   SMTP_PASSWORD
   EMAIL_RECIPIENTS

Create Configuration: Add unified_config.json with company and trend settings
Workflow File: Use .github/workflows/fintech-intelligence.yml

Workflow Features

Daily execution at 6 AM EST
Database persistence via GitHub artifacts
Comprehensive monitoring with logs and statistics
Error handling for network issues and API limits
Manual triggering for testing

Manual Operations
bash# Trigger workflow
gh workflow run fintech-intelligence.yml

# Check recent runs  
gh run list --workflow=fintech-intelligence.yml

# Download artifacts
gh run download [run-id]
Configuration File
The unified_config.json file controls both companies and trends:
json{
  "_metadata": {
    "version": "3.0",
    "last_updated": "2024-12-15"
  },
  "companies": {
    "visa": {
      "name": "Visa",
      "enabled": true,
      "priority": "high",
      "pages": ["https://usa.visa.com/about-visa/newsroom.html"],
      "max_articles": 3,
      "max_age_days": 3
    }
  },
  "instant_payments": {
    "name": "Instant Payments & Real-Time Rails",
    "keywords": ["instant payments", "real-time payments", "FedNow"],
    "rss_feeds": ["https://www.federalreserve.gov/feeds/press_all.xml"],
    "min_relevance_score": 0.45,
    "max_articles": 6
  }
}
Architecture
Unified System Components

UnifiedSystemOrchestrator: Main coordination and execution
CompanyMonitoringSystem: Corporate news collection and processing
TrendsMonitoringSystem: Industry trend analysis and curation
EnhancedLLMIntegration: OpenAI integration with usage controls
UnifiedDatabaseManager: SQLite operations with column validation
UnifiedEmailGenerator: Professional HTML email generation
EnhancedRegionFilter: Geographic content filtering

Data Flow

Dual Collection: Company pages/feeds + trend RSS/Google Search
Content Processing: Relevance scoring, title enhancement, summarization
Quality Filtering: 3-day company limit, relevance thresholds, region filtering
AI Enhancement: Limited LLM processing for top content
Unified Curation: Combined company + trend articles for email
Professional Delivery: HTML email with organized sections
Persistent Storage: Unified SQLite database with full history

Database Schema
Unified Articles Table

Content: URL, title, enhanced_title, content, summary
Classification: source_type, company_key, trend_category
Scoring: relevance_score, semantic_relevance_score, quality_score
Metadata: published_date, processed_date, search_keywords

Daily Statistics

Volume: total_articles, company_articles, trend_articles
Quality: avg_relevance, semantic_scored_articles
Performance: processing_time, email_articles

Performance & Limits
LLM Usage Controls

Title Enhancement: Max 25 per email generation
Summarization: Max 15 per email generation
Semantic Scoring: Max 60 articles per trends run
Token Limits: 32 tokens/title, 120 tokens/summary
Daily Cost: ~$0.50 maximum with gpt-4o-mini

Content Limits

Company Articles: 3 per company, max 3 days old
Email Total: 40 articles maximum
Database Retention: 30 days default
Processing Time: 60-120 seconds typical

Typical Daily Metrics

Total Articles: 150-400 processed
Company Updates: 10-30 articles
Trend Articles: 50-150 articles
Email Articles: 25-40 high-quality articles
Database Growth: ~5-10MB per month

Monitoring & Troubleshooting
Health Checks
bash# System validation
python Unified_FinTech_Intelligence.py --config-check

# Database statistics  
sqlite3 unified_fintech_intelligence.db "SELECT source_type, COUNT(*) FROM articles GROUP BY source_type;"

# Recent performance
sqlite3 unified_fintech_intelligence.db "SELECT * FROM daily_stats ORDER BY date DESC LIMIT 7;"

# LLM usage tracking
grep "LLM usage" unified_fintech_intelligence.log
Common Issues
No company articles:

Check company URLs in config (some may be disabled)
Verify 3-day age limit setting
Review domain timeouts in logs

Email not sending:

Verify SMTP credentials and app password
Check EMAIL_RECIPIENTS format
Ensure articles meet relevance threshold

Database errors:

System auto-handles missing columns (processed_date fix)
Check disk space and permissions
Verify SQLite version compatibility

LLM token limits exceeded:

Check daily usage in logs
Adjust LLM_MAX_* environment variables
Monitor costs via API usage dashboard

Debug Commands
bash# Verbose execution
python Unified_FinTech_Intelligence.py --verbose

# Test mode (no email)
python Unified_FinTech_Intelligence.py --test

# Link diagnosis
python Unified_FinTech_Intelligence.py --diagnose-links

# Disable LLM for testing
python Unified_FinTech_Intelligence.py --disable-llm
Environment Variables
VariableDefaultDescriptionCOMPANIES_ENABLED1Enable company monitoringTRENDS_ENABLED1Enable trend analysisLLM_INTEGRATION_ENABLED1Enable OpenAI featuresLLM_MAX_TITLE_ENHANCEMENTS25Max title enhancements per emailLLM_MAX_SUMMARIES15Max summaries per emailSEMANTIC_TOTAL_LIMIT60Max semantic scoring per runMAX_EMAIL_ARTICLES40Total articles in emailCOMPANY_MAX_AGE_DAYS3Max age for company articlesRELEVANCE_THRESHOLD0.58Min relevance for email inclusionTREND_CONFIG_JSONunified_config.jsonConfiguration file path
Contributing
Development Setup
bash# Install dependencies
pip install -r requirements.txt

# Run tests
python Unified_FinTech_Intelligence.py --test

# Validate configuration
python Unified_FinTech_Intelligence.py --config-check
Adding Companies

Add company config to unified_config.json
Test URLs with --diagnose-links
Verify with --companies-only --test

Adding Trends

Update trend section in unified_config.json
Test with --trends-only --test
Verify keyword relevance and RSS feeds

Changelog
v3.0.2 (Current)

Bug Fixes: Fixed database column 'processed_date' error
URL Updates: Removed broken company URLs (Affirm, Adyen, Fiserv, FIS, GPN)
Enhanced Error Handling: Better network timeout and DNS resolution handling
LLM Controls: Added title enhancement and summary limits for email generation
Company Limits: 3-day age limit and 3 articles max per company
No LLM for Companies: Disabled AI processing for company articles (faster, cleaner)

v3.0.1

Unified System: Combined company monitoring and trend analysis
Enhanced LLM Integration: Professional title generation and semantic scoring
Robust Database: Connection pooling and advanced deduplication
GitHub Actions: Complete automation with artifact management

v3.0.0

Complete Rewrite: Unified architecture for companies and trends
Production Ready: Enterprise-grade error handling and monitoring
AI-Powered: OpenAI integration for content enhancement
Professional Email: HTML digests with organized company and trend sections

License
MIT License - see LICENSE file for details.
Support

Issues: Create GitHub issue with logs and configuration details
Documentation: Check environment variables and configuration options
Debug: Use --verbose, --test, and --config-check flags
Monitoring: Review GitHub Actions logs and database statistics
