# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require_relative 'tokenprovider_base'

class WorkloadIdentity < AbstractTokenProvider
  DEFAULT_TOKEN_FILE = '/var/run/secrets/azure/tokens/azure-identity-token'
  AZURE_OAUTH2_TOKEN_ENDPOINT = 'https://login.microsoftonline.com/%{tenant_id}/oauth2/v2.0/token'

  def initialize(outconfiguration)
    super(outconfiguration)
  end

  # Use get_token from base class for token retrieval

  private

  def setup_config(outconfiguration)
    @client_id = outconfiguration.workload_identity_client_id
    @tenant_id = outconfiguration.workload_identity_tenant_id
    @token_file = outconfiguration.workload_identity_token_file_path || DEFAULT_TOKEN_FILE
    @kusto_endpoint = outconfiguration.kusto_endpoint
    @scope = "#{@kusto_endpoint}/.default"
  end

  def fetch_token
    response = acquire_workload_identity_token
    {
      access_token: response['access_token'],
      expires_in: response['expires_in']
    }
  end

  def acquire_workload_identity_token
    oidc_token = File.read(@token_file).strip
    uri = URI.parse(AZURE_OAUTH2_TOKEN_ENDPOINT % { tenant_id: @tenant_id })
    req = Net::HTTP::Post.new(uri)
    req.set_form_data(
      'grant_type' => 'client_credentials',
      'client_id' => @client_id,
      'scope' => @scope,
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
