# frozen_string_literal: true

require_relative '../helper'
require 'fluent/plugin/auth/wif_tokenprovider'
require 'fluent/plugin/conffile'
require 'mocha/test_unit'
require 'tempfile'
require 'uri'

class WorkloadIdentityTokenProviderTest < Test::Unit::TestCase
  OUTPUT_CONFIGURATION_CLASS = OutputConfiguration

  def setup
    @token_file = Tempfile.new('federated-token')
    @token_file.write('federated-token-for-testing')
    @token_file.flush
  end

  def teardown
    @token_file.close!
  end

  test 'builds the workload identity token endpoint for each supported cloud' do
    endpoints = {
      'AzureCloud' => 'https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token',
      'AzureChinaCloud' => 'https://login.chinacloudapi.cn/test-tenant/oauth2/v2.0/token',
      'AzureUSGovernment' => 'https://login.microsoftonline.us/test-tenant/oauth2/v2.0/token',
      'AzureUSGovernmentCloud' => 'https://login.microsoftonline.us/test-tenant/oauth2/v2.0/token'
    }

    endpoints.each do |cloud, expected_endpoint|
      provider = WorkloadIdentity.new(configuration_for(cloud))

      assert_equal expected_endpoint, provider.instance_variable_get(:@token_request_uri)
    end
  end

  test 'exchanges a workload identity assertion against Azure US Government' do
    provider = WorkloadIdentity.new(configuration_for('AzureUSGovernment'))
    response = Net::HTTPOK.new('1.1', '200', 'OK')
    response.stubs(:body).returns({ access_token: 'government-access-token', expires_in: 3600 }.to_json)
    http = mock('Azure US Government token endpoint')

    provider.expects(:create_http_client).with do |uri|
      assert_equal 'https://login.microsoftonline.us/test-tenant/oauth2/v2.0/token', uri.to_s
      true
    end.returns(http)
    http.expects(:request).with do |request|
      form_data = URI.decode_www_form(request.body).to_h
      assert_equal 'client_credentials', form_data['grant_type']
      assert_equal 'test-client', form_data['client_id']
      assert_equal 'https://test.usgovvirginia.kusto.usgovcloudapi.net/.default', form_data['scope']
      assert_equal 'federated-token-for-testing', form_data['client_assertion']
      true
    end.returns(response)

    assert_equal 'government-access-token', provider.get_token
  end

  private

  def configuration_for(cloud)
    OUTPUT_CONFIGURATION_CLASS.new(
      auth_type: 'workload_identity',
      azure_cloud: cloud,
      workload_identity_client_id: 'test-client',
      workload_identity_tenant_id: 'test-tenant',
      workload_identity_token_file_path: @token_file.path,
      kusto_endpoint: 'https://test.usgovvirginia.kusto.usgovcloudapi.net',
      database_name: 'test-database',
      table_name: 'test-table'
    )
  end
end
