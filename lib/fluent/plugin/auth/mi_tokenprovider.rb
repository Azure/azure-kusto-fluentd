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
require_relative 'tokenprovider_base'

class ManagedIdentityTokenProvider < AbstractTokenProvider
  IMDS_TOKEN_ACQUIRE_URL = 'http://169.254.169.254/metadata/identity/oauth2/token'

  def initialize(outconfiguration)
    super(outconfiguration)
    token_request_params_set(outconfiguration)
  end

  # Use get_token from base class for token retrieval

  private

  def setup_config(outconfiguration)
    @resource = outconfiguration.kusto_endpoint
    @managed_identity_client_id = outconfiguration.managed_identity_client_id
    val = @managed_identity_client_id.to_s.strip
    @use_system_assigned = (val.upcase == 'SYSTEM')
    @use_user_assigned = (!val.empty? && val.upcase != 'SYSTEM')
  end

  def append_header(name, value)
      "#{name}=#{value}"
  end

  def token_request_params_set(_outconfiguration)
      token_acquire_url = IMDS_TOKEN_ACQUIRE_URL.dup + "?" + append_header('resource', ERB::Util.url_encode(outconfiguration.kusto_endpoint)) + '&' + append_header('api-version', '2018-02-01')
      token_acquire_url = (token_acquire_url + '&' + append_header('object_id', ERB::Util.url_encode(@object_id))) unless @object_id.nil?
      token_acquire_url = (token_acquire_url + '&' + append_header('msi_res_id', ERB::Util.url_encode(@msi_res_id))) unless @msi_res_id.nil?
      url = URI.parse(token_acquire_url)
    if @use_user_assigned
      token_acquire_url = (token_acquire_url + '&' + append_header('client_id', ERB::Util.url_encode(@managed_identity_client_id)))      
    end
  end

  def fetch_token
    response = post_token_request
    {
      access_token: response['access_token'],
      expires_in: response['expires_in']
    }
  end

  def post_token_request
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
end
