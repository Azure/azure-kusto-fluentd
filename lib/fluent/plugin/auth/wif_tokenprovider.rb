# frozen_string_literal: true

require 'net/http'
require 'uri'
require 'json'
require_relative 'tokenprovider_base'

class WorkloadIdentity < AbstractTokenProvider
  DEFAULT_TOKEN_FILE = '/var/run/secrets/azure/tokens/azure-identity-token'
  AZURE_OAUTH2_TOKEN_ENDPOINT = 'https://login.microsoftonline.com/%<tenant_id>s/oauth2/v2.0/token'

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
    oidc_token = read_token_file_safely
    uri = URI.parse(format(AZURE_OAUTH2_TOKEN_ENDPOINT, tenant_id: @tenant_id))
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
    # Add timeouts to prevent hanging connections
    http.open_timeout = 10
    http.read_timeout = 30
    http.write_timeout = 10
    res = http.request(req)
    raise "Failed to get access token: #{res.code} #{res.body}" unless res.is_a?(Net::HTTPSuccess)

    JSON.parse(res.body)
  end

  def read_token_file_safely
    max_attempts = 3
    max_attempts.times do |attempt|
      begin
        # Safe file reading with corruption detection
        token = File.read(@token_file).strip
        raise "Empty or invalid token file" if token.empty? || token.length < 10
        return token
      rescue => e
        @logger.warn("Token file read attempt #{attempt + 1}/#{max_attempts} failed: #{e.message}")
        raise e if attempt == max_attempts - 1
        sleep(0.1 * (2 ** attempt))  # Exponential backoff: 0.1s, 0.2s, 0.4s
      end
    end
  end
end
