# frozen_string_literal: true

require 'test/unit'
require_relative '../../lib/fluent/plugin/auth/azcli_tokenprovider'

class DummyConfig
  attr_reader :kusto_endpoint

  def initialize(resource)
    @kusto_endpoint = resource
  end

  def logger
    require 'logger'
    Logger.new($stdout)
  end
end

class AzCliTokenProviderIntegrationTest < Test::Unit::TestCase
  def setup
    @resource = 'https://kusto.kusto.windows.net'
    @provider = AzCliTokenProvider.new(DummyConfig.new(@resource))
  end

  def test_get_token_integration
    begin
      az_path = @provider.send(:locate_azure_cli)
    rescue RuntimeError
      omit("Azure CLI not installed, skipping integration test.")
    end

    token = @provider.get_token
    assert_not_nil(token, "Token should not be nil")
    assert_kind_of(String, token)
    assert(token.length > 0, "Token should not be empty")
  end
end
