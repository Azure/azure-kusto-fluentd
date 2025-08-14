# frozen_string_literal: true

# AadTokenProvider handles acquiring and refreshing Azure Active Directory tokens for Kusto ingestion.
#
# Responsibilities:
# - Build and send token requests to Azure AD endpoint
# - Cache and refresh tokens as needed
# - Support client credentials flow for authentication
require 'json'
require 'openssl'
require 'base64'
require 'time'
require 'net/http'
require 'uri'
require_relative '../kusto_error_handler'
require_relative 'tokenprovider_base'

class AadTokenProvider < AbstractTokenProvider
  def initialize(outconfiguration)
    super(outconfiguration)
    token_request_params_set
  end

  # Use get_token from base class for token retrieval

  private

  def setup_config(outconfiguration)
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
    @token_request_uri = "#{@aad_uri}/#{@tenant_id}/oauth2/v2.0/token"
    @scope = "#{@resource}/.default"
  end

  def fetch_token
    response = post_token_request
    {
      access_token: response['access_token'],
      expires_in: response['expires_in']
    }
  end

  def post_token_request
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

  def header
    {
      'Content-Type' => 'application/x-www-form-urlencoded'
    }
  end
end
