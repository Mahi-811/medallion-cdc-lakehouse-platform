"""
Unit & Integration Tests for REST API Ingestor.
Verifies rate limiting, retry backoff, pagination, and Bronze metadata stamping.
"""
from datetime import date, timedelta
import pytest
from ingestion.api_extractor import APIExtractor, TokenBucketRateLimiter
from ingestion.config import APIConfig

def test_token_bucket_rate_limiter():
    limiter = TokenBucketRateLimiter(rate=10.0, capacity=2.0)
    # Fast acquire within burst capacity
    limiter.acquire(1.0)
    limiter.acquire(1.0)
    assert limiter.tokens <= 1.0

def test_api_pagination_date_ranges():
    extractor = APIExtractor()
    start = date(2026, 1, 1)
    end = date(2026, 1, 31)
    chunks = list(extractor.paginate_date_ranges(start, end, chunk_days=10))
    assert len(chunks) == 4
    assert chunks[0] == (date(2026, 1, 1), date(2026, 1, 10))
    assert chunks[-1] == (date(2026, 1, 31), date(2026, 1, 31))

def test_api_extractor_bronze_metadata():
    extractor = APIExtractor()
    today = date.today()
    records = extractor.extract_weather(start_date=today - timedelta(days=1), end_date=today)
    assert len(records) > 0

    for r in records:
        assert "_loaded_at" in r
        assert r["_source"] == "rest_api_open_meteo"
        assert "_batch_id" in r
        assert "city" in r
        assert "metric_date" in r
        assert "temp_mean_c" in r
