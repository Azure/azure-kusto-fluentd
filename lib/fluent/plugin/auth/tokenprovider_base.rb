# frozen_string_literal: true

require 'logger'
require 'thread'

# AbstractTokenProvider defines the interface and shared logic for all token providers.
class AbstractTokenProvider
  def initialize(outconfiguration)
    @logger = setup_logger(outconfiguration)
    setup_config(outconfiguration)
    @token_state = { access_token: nil, expiry_time: nil, token_details_mutex: Mutex.new }
  end

  # Abstract method: must be implemented by subclasses to fetch a new token.
  def fetch_token
    raise NotImplementedError, 'Subclasses must implement fetch_token'
  end

  # Public method to get a valid token, refreshing if needed.
  def get_token
    @token_state[:token_details_mutex].synchronize do
      if saved_token_need_refresh?
        @logger.info("Refreshing token. Previous expiry: #{@token_state[:expiry_time]}")
        refresh_saved_token
        @logger.info("New token expiry: #{@token_state[:expiry_time]}")
      end
      @token_state[:access_token]
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
    @token_state[:access_token].nil? || @token_state[:expiry_time].nil? || @token_state[:expiry_time] <= Time.now
  end

  def refresh_saved_token
    token_response = fetch_token
    @token_state[:access_token] = token_response[:access_token]
    @token_state[:expiry_time] = get_token_expiry_time(token_response[:expires_in])
  end

  def get_token_expiry_time(expires_in_seconds)
    if expires_in_seconds.nil? || expires_in_seconds.to_i <= 0
      Time.now + 3540 # Default to 59 minutes if expires_in is not provided or invalid
    else
      Time.now + expires_in_seconds.to_i - 1
    end
  end
end
