# frozen_string_literal: true

# AadTokenProvider handles acquiring and refreshing Azure Active Directory tokens for Kusto ingestion.
#
# Responsibilities:
# - Build and send token requests to Azure AD endpoint
# - Cache and refresh tokens as needed
# - Support client credentials flow for authentication
require_relative 'conffile'
require 'json'
require 'openssl'
require 'base64'
require 'time'
require 'net/http'
require 'uri'
require_relative 'kusto_error_handler'

class AadTokenProvider
  def initialize(outconfiguration)
    # Initialize logger, config, and token request parameters
    @logger = setup_logger(outconfiguration)
    setup_config(outconfiguration)
    token_request_params_set
    @token_state = { access_token: nil, expiry_time: nil, token_details_mutex: Mutex.new }
  end

  # Public method to get a valid AAD token, refreshing if needed
  def aad_token_bearer
    @token_state[:token_details_mutex].synchronize do
      if saved_token_need_refresh?
        @logger.info("Refreshing AAD token. Previous expiry: #{@token_state[:expiry_time]}")
        refresh_saved_token
        @logger.info("New token expiry: #{@token_state[:expiry_time]}")
      end
      return @token_state[:access_token]
    end
  end

  private

  def setup_config(outconfiguration)
    # Set up configuration values for token request
    @client_id = outconfiguration.client_app_id
    @client_secret = outconfiguration.client_app_secret
    @tenant_id = outconfiguration.tenant_id
    @aad_uri = outconfiguration.aad_endpoint
    @resource = outconfiguration.kusto_endpoint
    @database_name = outconfiguration.database_name
    @table_name = outconfiguration.table_name
    @azure_cloud = outconfiguration.azure_cloud
    @managed_identity_client_id = outconfiguration.managed_identity_client_id
  end

  def token_request_params_set
    # Build AAD token request URI and scope
    @token_request_uri = "#{@aad_uri}/#{@tenant_id}/oauth2/v2.0/token"
    @scope = "#{@resource}/.default"
  end

  def saved_token_need_refresh?
    # Check if cached token is missing or expired
    @token_state[:access_token].nil? || @token_state[:expiry_time].nil? || @token_state[:expiry_time] <= Time.now
  end

  def refresh_saved_token
    # Request and cache a new AAD token
    @logger.info('aad token expired - refreshing token.')
    token_response = post_token_request
    @token_state[:access_token] = token_response['access_token']
    @token_state[:expiry_time] = get_token_expiry_time(token_response['expires_in'])
  end

  def get_token_expiry_time(expires_in_seconds)
    # Calculate token expiry time from expires_in value
    if expires_in_seconds.nil? || expires_in_seconds <= 0
      Time.now + 3540 # Default to 59 minutes if expires_in is not provided or invalid
    else
      Time.now + expires_in_seconds - 1 # Decrease by 1 second to be on the safe side
    end
  end

  # rubocop:disable Metrics/MethodLength, Metrics/AbcSize
  def post_token_request
    # Send HTTP request to AAD endpoint to acquire token
    headers = header
    max_retries = 10
    retries = 0
    uri = URI.parse(@token_request_uri)
    form_data = URI.encode_www_form(
      'grant_type' => 'client_credentials',
      'client_id' => @client_id,
      'client_secret' => @client_secret,
      'scope' => @scope
    )
    while retries < max_retries
      begin
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = (uri.scheme == 'https')
        request = Net::HTTP::Post.new(uri.request_uri, headers)
        request.body = form_data

        response = http.request(request)
        return JSON.parse(response.body) if [200, 201].include?(response.code.to_i)

        begin
          error_json = JSON.parse(response.body)
          kusto_error_type = KustoErrorHandler.extract_kusto_error_type(error_json)
          error = KustoErrorHandler.from_kusto_error_type(
            kusto_error_type,
            error_json['error_description'] || error_json['message'] || response.body
          )
          if error.permanent_error?
            @logger.error("Permanent error encountered, not retrying. #{error.message}")
            raise error
          end
        rescue JSON::ParserError
          @logger.error("Failed to parse error response: #{response.body}")
          raise "Permanent error while authenticating with AAD: #{response.body}"
        end
      end
      retries += 1
      @logger.error(
        "Error while authenticating with AAD ('#{@aad_uri}'), retrying in 10 seconds. " \
        "Attempt #{retries}/#{max_retries}"
      )
      sleep 10
    end
    raise "Failed to authenticate with AAD after #{max_retries} attempts."
  end
  # rubocop:enable Metrics/MethodLength, Metrics/AbcSize

  def header
    # Return headers for token request
    {
      'Content-Type' => 'application/x-www-form-urlencoded'
    }
  end

  def setup_logger(outconfiguration)
    # Use provided logger or default to stdout
    outconfiguration.logger || Logger.new($stdout)
  end
end
