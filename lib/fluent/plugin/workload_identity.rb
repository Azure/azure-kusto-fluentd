# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require 'thread'
require_relative 'conffile'

class WorkloadIdentity
  DEFAULT_TOKEN_FILE = '/var/run/secrets/azure/tokens/azure-identity-token'
  AZURE_OAUTH2_TOKEN_ENDPOINT = 'https://login.microsoftonline.com/%{tenant_id}/oauth2/v2.0/token'

  def initialize(outconfiguration)
    @client_id = outconfiguration.workload_identity_client_id
    @tenant_id = outconfiguration.workload_identity_tenant_id
    @token_file = outconfiguration.workload_identity_token_file_path || DEFAULT_TOKEN_FILE
    @logger = outconfiguration.logger || Logger.new($stdout)
    @token_state = { access_token: nil, expiry_time: nil, token_details_mutex: Mutex.new }
    @kusto_endpoint = outconfiguration.kusto_endpoint
  end

  def aad_token_bearer
    scope = "#{@kusto_endpoint}/.default"
    @token_state[:token_details_mutex].synchronize do
      if saved_token_need_refresh?
        @logger.info("Refreshing workload identity access token. Previous expiry: #{@token_state[:expiry_time]}")
        refresh_saved_token(scope)
        @logger.info("New token expiry: #{@token_state[:expiry_time]}")
      end
      return @token_state[:access_token]
    end
  end

  private

  def saved_token_need_refresh?
    @token_state[:access_token].nil? || @token_state[:expiry_time].nil? || @token_state[:expiry_time] <= Time.now
  end

  def refresh_saved_token(scope)
    @logger.info('Workload identity access token expired - refreshing token.')
    token_response = acquire_workload_identity_token(scope)
    @token_state[:access_token] = token_response['access_token']
    @token_state[:expiry_time] = get_token_expiry_time(token_response['expires_in'])
  end

  def get_token_expiry_time(expires_in_seconds)
    if expires_in_seconds.nil? || expires_in_seconds <= 0
      Time.now + 3540 # Default to 59 minutes if expires_in is not provided or invalid
    else
      Time.now + expires_in_seconds - 1
    end
  end

  def acquire_workload_identity_token(scope)
    oidc_token = File.read(@token_file).strip
    uri = URI.parse(AZURE_OAUTH2_TOKEN_ENDPOINT % { tenant_id: @tenant_id })
    req = Net::HTTP::Post.new(uri)
    req.set_form_data(
      'grant_type' => 'client_credentials',
      'client_id' => @client_id,
      'scope' => scope,
      'client_assertion_type' => 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
      'client_assertion' => oidc_token
    )
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    res = http.request(req)
    raise "Failed to get access token: #{res.code} #{res.body}" unless res.is_a?(Net::HTTPSuccess)
    JSON.parse(res.body)
  end
end



