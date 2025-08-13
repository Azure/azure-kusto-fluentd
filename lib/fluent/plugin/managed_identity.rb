# frozen_string_literal: true

# ManagedIdentityTokenProvider handles acquiring and refreshing Azure Managed Identity tokens for Kusto ingestion.
#
# Responsibilities:
# - Build and send token requests to Azure IMDS endpoint
# - Cache and refresh tokens as needed
# - Support both system-assigned and user-assigned managed identities

require 'net/http'
require 'uri'
require 'json'
require 'logger'
require_relative 'conffile'

class ManagedIdentityTokenProvider
  def initialize(outconfiguration)
    # Initialize logger, config, and token request parameters
    @logger = setup_logger(outconfiguration)
    setup_config(outconfiguration)
    token_request_params_set(outconfiguration)
    @token_state = { access_token: nil, expiry_time: nil, token_details_mutex: Mutex.new }
  end

  def aad_token_bearer
    # Return a valid access token, refreshing if needed
    @token_state[:token_details_mutex].synchronize do
      if saved_token_need_refresh?
        @logger.info("Refreshing Managed Identity token. Previous expiry: #{@token_state[:expiry_time]}")
        refresh_saved_token
        @logger.info("New token expiry: #{@token_state[:expiry_time]}")
      end
      @token_state[:access_token]
    end
  end

  private

  def setup_config(outconfiguration)
    # Set up resource, logger, and managed identity type
    @resource = outconfiguration.kusto_endpoint
    @logger = outconfiguration.logger || Logger.new($stdout)
    @managed_identity_client_id = outconfiguration.managed_identity_client_id
    val = @managed_identity_client_id.to_s.strip
    @use_system_assigned = (val.upcase == 'SYSTEM')
    @use_user_assigned = (!val.empty? && val.upcase != 'SYSTEM')
  end

  def token_request_params_set(_outconfiguration)
    # Build IMDS endpoint URL for token request
    @imds_base_url = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=#{@resource}"
    if @use_user_assigned
      @imds_base_url += "&client_id=#{@managed_identity_client_id}"
    end
  end

  def saved_token_need_refresh?
    # Check if cached token is missing or expired
    @token_state[:access_token].nil? || @token_state[:expiry_time].nil? || @token_state[:expiry_time] <= Time.now
  end

  def refresh_saved_token
    # Request and cache a new token
    @logger.info('Managed identity token expired - refreshing token.')
    token_response = post_token_request
    @token_state[:access_token] = token_response['access_token']
    @token_state[:expiry_time] = get_token_expiry_time(token_response['expires_in'])
  end

  def get_token_expiry_time(expires_in_seconds)
    # Calculate token expiry time from expires_in value
    if expires_in_seconds.nil? || expires_in_seconds.to_i <= 0
      Time.now + 3540 # Default to 59 minutes if expires_in is not provided or invalid
    else
      Time.now + expires_in_seconds.to_i - 1 # Decrease by 1 second to be on the safe side
    end
  end

  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
  def post_token_request
    # Send HTTP request to IMDS endpoint to acquire token
    headers = { 'Metadata' => 'true' }
    max_retries = 2
    retries = 0
    uri = URI.parse(@imds_base_url)
    while retries < max_retries
      begin
        http = Net::HTTP.new(uri.host, uri.port)
        request = Net::HTTP::Get.new(uri.request_uri, headers)
        response = http.request(request)
        return JSON.parse(response.body) if response.code.to_i == 200

        @logger.error("Failed to get managed identity token: #{response.code} #{response.body}")
      rescue StandardError => e
        @logger.error("Error while requesting managed identity token: #{e.message}")
      end
      retries += 1
      @logger.error(
        "Retrying managed identity token request in 10 seconds. Attempt #{retries}/#{max_retries}"
      )
      sleep 10
    end
    raise "Failed to get managed identity token after #{max_retries} attempts."
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  def setup_logger(outconfiguration)
    # Use provided logger or default to stdout
    outconfiguration.logger || Logger.new($stdout)
  end
end
