# frozen_string_literal: true

# KustoConstants contains shared configuration constants used across the Kusto plugin
# to avoid magic numbers and ensure consistency.
module KustoConstants
  # Authentication and token management constants
  module Authentication
    # Token expiry buffer time in seconds (5 minutes)
    # Used to refresh tokens before they actually expire to prevent race conditions
    TOKEN_EXPIRY_BUFFER_SECONDS = 300
    
    # Default token expiry time in seconds (55 minutes)
    # Used when expires_in is not provided or invalid
    DEFAULT_TOKEN_EXPIRY_SECONDS = 3300
    
    # Maximum retry attempts for token fetching
    DEFAULT_MAX_RETRIES = 3
    
    # Base delay for exponential backoff in seconds
    DEFAULT_BASE_DELAY = 1
    
    # Backoff multiplier for exponential backoff
    DEFAULT_BACKOFF_MULTIPLIER = 2
    
    # Maximum delay between retries in seconds
    DEFAULT_MAX_DELAY = 30
    
    # Minimum retry delay in seconds (prevents too-rapid retries)
    MIN_RETRY_DELAY = 0.1
    
    # HTTP client timeout settings
    HTTP_OPEN_TIMEOUT = 10
    HTTP_READ_TIMEOUT = 30
    HTTP_WRITE_TIMEOUT = 10
  end
  
  # Resource caching and client management constants
  module ResourceCache
    # Base TTL for resource cache in seconds (6 hours)
    BASE_CACHE_TTL_SECONDS = 21_600
    
    # Maximum jitter for cache TTL in seconds (±30 minutes)
    CACHE_TTL_JITTER_SECONDS = 1800
  end
  
  # Long-running pod health check constants
  module HealthCheck
    # Maximum age before forcing reset in seconds (12 hours)
    MAX_COMPONENT_AGE_SECONDS = 43_200
    
    # Maximum refresh cycles before forcing reset
    MAX_REFRESH_CYCLES = 100
    
    # Maximum resource fetch cycles before forcing reset
    MAX_FETCH_CYCLES = 200
  end
end
