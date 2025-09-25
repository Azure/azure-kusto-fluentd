# frozen_string_literal: true

require 'logger'
require 'fluent/plugin/kusto_constants'

# AbstractTokenProvider defines the interface and shared logic for all token providers.
# Enhanced with retry logic and better token expiry management to prevent timeout issues.
class AbstractTokenProvider
  def initialize(outconfiguration)
    @logger = setup_logger(outconfiguration)
    setup_config(outconfiguration)
    @token_state = { 
      access_token: nil, 
      expiry_time: nil, 
      token_details_mutex: Mutex.new,
      refresh_in_progress: false,
      consecutive_failures: 0,
      last_failure_time: nil,
      creation_time: Time.now,
      refresh_count: 0,
      last_successful_refresh: nil
    }
    
    # Simplified retry configuration using constants
    @retry_config = {
      max_retries: KustoConstants::Authentication::DEFAULT_MAX_RETRIES,
      base_delay: KustoConstants::Authentication::DEFAULT_BASE_DELAY,
      backoff_multiplier: KustoConstants::Authentication::DEFAULT_BACKOFF_MULTIPLIER,
      max_delay: KustoConstants::Authentication::DEFAULT_MAX_DELAY
    }
    
    # Minimal health configuration for 12-hour reset
    @health_config = {
      max_token_age: KustoConstants::HealthCheck::MAX_COMPONENT_AGE_SECONDS,
      max_refresh_cycles: KustoConstants::HealthCheck::MAX_REFRESH_CYCLES
    }
    
    # HTTP timeout configuration - consistent across all token providers
    @http_config = {
      open_timeout: KustoConstants::Authentication::HTTP_OPEN_TIMEOUT,
      read_timeout: KustoConstants::Authentication::HTTP_READ_TIMEOUT,
      write_timeout: KustoConstants::Authentication::HTTP_WRITE_TIMEOUT
    }
  end

  # Abstract method: must be implemented by subclasses to fetch a new token.
  def fetch_token
    raise NotImplementedError, 'Subclasses must implement fetch_token'
  end

  # Public method to get a valid token, refreshing if needed with enhanced retry logic.
  def get_token
    @token_state[:token_details_mutex].synchronize do
      if saved_token_need_refresh?
        if @token_state[:refresh_in_progress]
          @logger.debug("Token refresh already in progress, waiting...")
          return wait_for_refresh_completion
        end
        
        @logger.info("Refreshing token. Previous expiry: #{@token_state[:expiry_time]}")
        refresh_saved_token_with_retry
        @logger.info("New token expiry: #{@token_state[:expiry_time]}")
      else
        @logger.debug("Reusing existing token (expires at #{@token_state[:expiry_time]})")
      end
      @token_state[:access_token]
    end
  end

  # Health check method - returns health status as hash
  # Note: This method should be called from within a synchronized context
  def health_status
    {
      token_valid: !saved_token_need_refresh?,
      token_expires_at: @token_state[:expiry_time],
      consecutive_failures: @token_state[:consecutive_failures],
      last_failure_time: @token_state[:last_failure_time],
      refresh_in_progress: @token_state[:refresh_in_progress],
      refresh_count: @token_state[:refresh_count],
      last_successful_refresh: @token_state[:last_successful_refresh],
      token_age_hours: @token_state[:creation_time] ? (Time.now - @token_state[:creation_time]) / 3600 : 0
    }
  end

  # Thread-safe wrapper for health_status when called externally
  def get_health_status
    @token_state[:token_details_mutex].synchronize do
      health_status
    end
  end

  # Log health status for operational visibility
  def log_health_status(context = "")
    status = health_status
    context_prefix = context.empty? ? "" : "#{context}: "
    
    @logger.info("#{context_prefix}Token provider health - " \
                "valid: #{status[:token_valid]}, " \
                "expires_at: #{status[:token_expires_at]}, " \
                "failures: #{status[:consecutive_failures]}, " \
                "refresh_count: #{status[:refresh_count]}, " \
                "age_hours: #{status[:token_age_hours].round(1)}")
    
    if status[:consecutive_failures] > 0
      @logger.warn("#{context_prefix}Token provider has #{status[:consecutive_failures]} consecutive failures, " \
                  "last failure: #{status[:last_failure_time]}")
    end
  end

  private

  def setup_logger(outconfiguration)
    outconfiguration.logger || Logger.new($stdout)
  end

  def setup_config(_outconfiguration)
    # To be optionally overridden by subclasses
  end

  def saved_token_need_refresh?
    return true if @token_state[:access_token].nil? || @token_state[:expiry_time].nil?
    
    # Check for long-running pod health issues
    if long_running_pod_health_check_needed?
      @logger.warn("Long-running pod health issue detected, forcing token refresh")
      return true
    end
    
    # Use token expiry buffer from constants to prevent race conditions
    @token_state[:expiry_time] <= (Time.now + KustoConstants::Authentication::TOKEN_EXPIRY_BUFFER_SECONDS)
  end

  def long_running_pod_health_check_needed?
    current_time = Time.now
    
    # Check if token is too old (12+ hours) - force refresh to prevent staleness
    if @token_state[:creation_time] && 
       (current_time - @token_state[:creation_time]) > @health_config[:max_token_age]
      @logger.warn("Token provider is #{(current_time - @token_state[:creation_time]) / 3600} hours old, forcing refresh")
      reset_token_state_for_long_running_pod
      return true
    end
    
    # Check if too many refresh cycles (potential state corruption)
    if @token_state[:refresh_count] > @health_config[:max_refresh_cycles]
      @logger.warn("Token provider has #{@token_state[:refresh_count]} refresh cycles, resetting state")
      reset_token_state_for_long_running_pod
      return true
    end
    
    # Check if last successful refresh was too long ago
    if @token_state[:last_successful_refresh] &&
       (current_time - @token_state[:last_successful_refresh]) > (@health_config[:max_token_age] / 2)
      @logger.warn("No successful refresh for #{(current_time - @token_state[:last_successful_refresh]) / 3600} hours")
      return true
    end
    
    false
  end

  def reset_token_state_for_long_running_pod
    log_health_status("Before reset")
    @logger.info("Resetting token state for long-running pod health")
    
    @token_state[:access_token] = nil
    @token_state[:expiry_time] = nil
    @token_state[:consecutive_failures] = 0
    @token_state[:last_failure_time] = nil
    @token_state[:creation_time] = Time.now
    @token_state[:refresh_count] = 0
    @token_state[:last_successful_refresh] = nil
    
    log_health_status("After reset")
  end

  def wait_for_refresh_completion
    # Wait for ongoing refresh to complete (max 30 seconds)
    max_wait = 30
    start_time = Time.now
    
    while @token_state[:refresh_in_progress] && (Time.now - start_time) < max_wait
      sleep(0.5)
    end
    
    # Return token if refresh completed successfully
    return @token_state[:access_token] if @token_state[:access_token] && !saved_token_need_refresh?
    
    # If still no valid token, attempt refresh ourselves
    @token_state[:refresh_in_progress] = false
    refresh_saved_token_with_retry
    @token_state[:access_token]
  end

  def refresh_saved_token_with_retry
    @token_state[:refresh_in_progress] = true
    
    begin
      token_response = fetch_token_with_retry
      @token_state[:access_token] = token_response[:access_token]
      @token_state[:expiry_time] = get_token_expiry_time(token_response[:expires_in])
      @token_state[:consecutive_failures] = 0
      @token_state[:last_failure_time] = nil
      @token_state[:refresh_count] += 1
      @token_state[:last_successful_refresh] = Time.now
      
      @logger.info("Token refresh successful (cycle #{@token_state[:refresh_count]})")
      
      # Log health status after successful refresh for operational visibility
      log_health_status("After successful refresh")
    ensure
      @token_state[:refresh_in_progress] = false
    end
  end

  def fetch_token_with_retry
    attempt = 0
    last_exception = nil
    
    while attempt < @retry_config[:max_retries]
      attempt += 1
      
      begin
        @logger.info("Attempting token fetch (attempt #{attempt}/#{@retry_config[:max_retries]})")
        return fetch_token
        
      rescue StandardError => e
        last_exception = e
        @logger.warn("Token fetch attempt #{attempt} failed: #{e.message}")
        
        # Don't retry on permanent errors
        if permanent_error?(e)
          @logger.error("Permanent error detected, not retrying: #{e.message}")
          record_failure(e)
          raise e
        end
        
        # Calculate delay with exponential backoff
        if attempt < @retry_config[:max_retries]
          delay = calculate_retry_delay(attempt)
          @logger.info("Retrying in #{delay} seconds...")
          sleep(delay)
        end
      end
    end
    
    # All retries exhausted
    record_failure(last_exception)
    raise last_exception || StandardError.new("Token fetch failed after #{@retry_config[:max_retries]} attempts")
  end

  def calculate_retry_delay(attempt)
    # Exponential backoff: base_delay * backoff_multiplier^(attempt-1)
    # Example: 1s, 2s, 4s for base_delay=1, backoff_multiplier=2
    delay = @retry_config[:base_delay] * (@retry_config[:backoff_multiplier] ** (attempt - 1))
    delay = [@retry_config[:max_delay], delay].min
    
    # Add jitter to prevent thundering herd
    # When many concurrent refreshes are happening, this will space them out better
    jitter = delay * 0.1
    delay += rand(-jitter..jitter)
    
    [delay, KustoConstants::Authentication::MIN_RETRY_DELAY].max # Minimum retry delay from constants
  end

  def permanent_error?(exception)
    return false unless exception.respond_to?(:message)
    
    message = exception.message.to_s.downcase
    permanent_patterns = [
      'unauthorized',
      'forbidden',
      'invalid_client',
      'invalid_grant',
      'access_denied'
    ]
    
    permanent_patterns.any? { |pattern| message.include?(pattern) }
  end

  def record_failure(exception)
    @token_state[:consecutive_failures] += 1
    @token_state[:last_failure_time] = Time.now
    @logger.error("Token fetch failed: #{exception&.message || 'Unknown error'}")
  end

  def get_token_expiry_time(expires_in_seconds)
    if expires_in_seconds.nil? || expires_in_seconds.to_i <= 0
      # Default to 55 minutes if expires_in is not provided or invalid
      Time.now + KustoConstants::Authentication::DEFAULT_TOKEN_EXPIRY_SECONDS
    else
      # Use buffer from constants for better safety margin
      Time.now + expires_in_seconds.to_i - KustoConstants::Authentication::TOKEN_EXPIRY_BUFFER_SECONDS
    end
  end

  # Helper method to create HTTP client with consistent timeout configuration
  # This prevents hanging connections and ensures consistent behavior across all token providers
  def create_http_client(uri)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https')
    http.open_timeout = @http_config[:open_timeout]
    http.read_timeout = @http_config[:read_timeout] 
    http.write_timeout = @http_config[:write_timeout]
    http
  end
end
