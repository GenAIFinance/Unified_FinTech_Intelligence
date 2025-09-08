# FinTech Digest System v2.1.1

An automated financial technology news aggregation and digest system that collects, processes, and curates high-quality FinTech articles from multiple sources using AI-powered content analysis.

## Features

- **Multi-Source Content Aggregation**: RSS feeds and Google Custom Search API
- **AI-Powered Enhancement**: OpenAI integration for title enhancement and semantic scoring
- **Intelligent Filtering**: Region-based filtering (US/EU focus), relevance scoring, duplicate detection
- **Email Digest**: Automated HTML email generation with top articles
- **Database Persistence**: SQLite database with connection pooling and historical data management
- **Trend Tracking**: 10+ FinTech categories including payments, open banking, stablecoins, and fraud prevention
- **Production Ready**: GitHub Actions automation, error handling, and monitoring

## Quick Start

### Prerequisites

- Python 3.11+
- Google Custom Search API key
- OpenAI API key (optional, for AI features)
- Email account with app password (for digest delivery)

### Installation

```bash
# Clone repository
git clone https://github.com/yourusername/fintech-digest.git
cd fintech-digest

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys and settings
```

### Configuration

Create a `.env` file with the following variables:

```env
# Required
GOOGLE_SEARCH_API_KEY=your_google_api_key
GOOGLE_SEARCH_ENGINE_ID=your_search_engine_id
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
EMAIL_RECIPIENTS=recipient1@example.com,recipient2@example.com

# Optional
LLM_API_KEY=your_openai_api_key
RELEVANCE_THRESHOLD=0.58
MAX_EMAIL_ARTICLES=25
SEMANTIC_TOTAL_LIMIT=60
LLM_INTEGRATION_ENABLED=1
SEMANTIC_SCORING_ENABLED=1
```

### Usage

```bash
# Test configuration
python fintech_digest.py --config-check

# Validate RSS feeds
python fintech_digest.py --validate-feeds

# Test single trend
python fintech_digest.py --test-single-trend instant_payments

# Run full digest
python fintech_digest.py
```

## Tracked Trends

The system monitors these FinTech categories:

- **Instant Payments**: Real-time payments, FedNow, RTP
- **A2A & Open Banking**: Account-to-account, PSD2, banking APIs
- **Stablecoins & CBDC**: Digital currencies, central bank initiatives
- **SoftPOS & Tap to Pay**: Mobile POS, contactless payments
- **Cross-Border Payments**: International transfers, remittances
- **Buy Now, Pay Later**: Installment payments, consumer credit
- **Payment Orchestration**: Smart routing, optimization
- **Fraud Prevention & AI**: Risk management, ML detection
- **PCI DSS & Security**: Compliance, payment security
- **Digital Wallets & NFC**: Mobile payments, contactless

## GitHub Actions Automation

### Setup

1. **Add Repository Secrets** (Settings → Secrets and variables → Actions):
   ```
   GOOGLE_SEARCH_API_KEY
   GOOGLE_SEARCH_ENGINE_ID
   LLM_API_KEY
   SMTP_USER
   SMTP_PASSWORD
   EMAIL_RECIPIENTS
   ```

2. **Configure Workflow**: The included `.github/workflows/fintech-digest.yml` runs daily at 6 AM EST

3. **Database Persistence**: The workflow automatically saves and restores the SQLite database using GitHub artifacts

### Manual Trigger

```bash
# Trigger workflow manually
gh workflow run fintech-digest.yml

# View recent runs
gh run list --workflow=fintech-digest.yml
```

## API Setup

### Google Custom Search API

1. Create project at [Google Cloud Console](https://console.cloud.google.com/)
2. Enable Custom Search API
3. Create API key
4. Set up Custom Search Engine at [cse.google.com](https://cse.google.com/)
5. Configure to search entire web

### OpenAI API (Optional)

1. Get API key from [OpenAI Platform](https://platform.openai.com/)
2. Set usage limits as needed
3. Used for title enhancement and semantic scoring

### Email Setup

For Gmail:
1. Enable 2-factor authentication
2. Generate App Password in account settings
3. Use app password (not regular password) for `SMTP_PASSWORD`

## Configuration Options

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RELEVANCE_THRESHOLD` | 0.58 | Minimum relevance score for email inclusion |
| `MAX_EMAIL_ARTICLES` | 25 | Maximum articles in email digest |
| `SEMANTIC_TOTAL_LIMIT` | 60 | Maximum articles for LLM semantic scoring |
| `LLM_INTEGRATION_ENABLED` | 1 | Enable OpenAI integration |
| `SEMANTIC_SCORING_ENABLED` | 1 | Enable LLM semantic analysis |
| `STRICT_REGION_FILTER` | 1 | Filter out Asia/Africa content |
| `KEEP_HISTORICAL_DAYS` | 365 | Days to retain historical data |
| `LOG_LEVEL` | INFO | Logging verbosity |

### Trend Configuration

Customize trends in `trend_config.json`:

```json
{
  "instant_payments": {
    "name": "Instant Payments",
    "keywords": ["instant payments", "real-time payments", "FedNow"],
    "rss_feeds": ["https://example.com/feed.xml"],
    "min_relevance_score": 0.25,
    "email_relevance_score": 0.4,
    "max_articles": 8
  }
}
```

## Architecture

### Components

- **DigestOrchestrator**: Main coordinator class
- **GoogleSearchIntegration**: Google Custom Search API client
- **EnhancedRSSFeedValidator**: RSS feed processing
- **EnhancedLLMIntegration**: OpenAI API integration
- **ArticleProcessor**: Content analysis and scoring
- **RobustDatabaseManager**: SQLite operations with connection pooling
- **ProductionEmailGenerator**: HTML email generation

### Data Flow

1. **Collection**: Gather articles from RSS feeds and Google Search
2. **Processing**: Calculate relevance scores, enhance titles, generate summaries
3. **Filtering**: Apply region filters, relevance thresholds, duplicate detection
4. **Semantic Analysis**: LLM-powered semantic scoring for top articles
5. **Curation**: Select best articles for email digest
6. **Delivery**: Generate and send HTML email digest
7. **Persistence**: Save all data to SQLite database

## Database Schema

### Articles Table
- URL, title, content, domain, source type
- Relevance scores (rule-based and semantic)
- Publishing and processing timestamps
- Trend categorization

### Daily Stats Table
- Processing metrics and performance data
- Article counts by source and quality
- System health monitoring

## Monitoring

### Health Checks

```bash
# Check system status
python fintech_digest.py --config-check

# Database statistics
sqlite3 fintech_digest.db "SELECT trend_category, COUNT(*) FROM articles GROUP BY trend_category;"

# Recent performance
sqlite3 fintech_digest.db "SELECT * FROM daily_stats ORDER BY date DESC LIMIT 7;"
```

### Logs

- Application logs: `fintech_digest.log`
- GitHub Actions logs: Available in workflow runs
- Email delivery status: Included in console output

## Troubleshooting

### Common Issues

**No articles found**:
- Check API keys and quotas
- Validate RSS feed accessibility
- Review relevance threshold settings

**Email not sending**:
- Verify SMTP credentials
- Check app password (not regular password)
- Ensure recipient addresses are correct

**Database errors**:
- Check file permissions
- Verify disk space
- Run database integrity check: `sqlite3 fintech_digest.db "PRAGMA integrity_check;"`

**GitHub Actions failures**:
- Check secrets configuration
- Review workflow logs
- Verify artifact download/upload

### Debug Commands

```bash
# Verbose logging
python fintech_digest.py --verbose

# Test mode (no email)
python fintech_digest.py --test

# Single trend testing
python fintech_digest.py --test-single-trend fraud_ai

# Feed validation
python fintech_digest.py --validate-feeds
```

## Performance

### Typical Metrics
- Processing time: 30-60 seconds
- Articles processed: 100-300 per day
- Email articles: 20-25 high-quality articles
- Database size: ~50-100MB after 1 year

### Optimization
- Connection pooling for database operations
- Async HTTP requests for parallel processing
- Intelligent duplicate detection
- Rate limiting and retry logic

## Contributing

### Development Setup

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests
python fintech_digest.py --test

# Code style
# Follow PEP 8 guidelines
# Use type hints where possible
```

### Adding New Trends

1. Add trend configuration to `trend_config.json`
2. Update RSS feed sources
3. Test with `--test-single-trend`
4. Verify keyword relevance

### Pull Request Process

1. Fork the repository
2. Create feature branch
3. Test thoroughly with `--test` and `--config-check`
4. Update documentation as needed
5. Submit pull request with description

## License

MIT License - see LICENSE file for details.

## Support

- Create GitHub issue for bugs or feature requests
- Check existing issues before creating new ones
- Include logs and configuration details in bug reports

## Changelog

### v2.1.1
- Enhanced region filtering (excludes Asia and Africa)
- Email article limit (20-30 articles maximum)
- Semantic scoring limit (60 articles total)
- Improved GitHub Actions workflow
- Better error handling and logging

### v2.1.0
- LLM integration for title enhancement
- Semantic relevance scoring
- Enhanced database connection pooling
- Production-ready architecture

### v2.0.0
- Complete rewrite with async architecture
- Multi-source content aggregation
- AI-powered content enhancement
- Automated email digest generation# fintech-digest-automation
complete automated GitHub Actions workflow system with comprehensive artifact management and database synchronization capabilities
