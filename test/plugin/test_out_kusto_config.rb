# rubocop:disable all
# frozen_string_literal: true

require_relative '../helper'
require 'fluent/plugin/out_kusto'
require 'fluent/plugin/conffile'

class KustoOutputConfigTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @base_conf = {
      'kusto_endpoint' => 'https://example.kusto.windows.net',
      'database_name' => 'testdb',
      'table_name' => 'testtable',
      'buffered' => 'true'
    }
  end

  test 'plugin raises error if buffer section present but buffered is false' do
    conf = "#{config_str(@base_conf.merge('buffered' => 'false'))}\n<buffer>\n  @type memory\n</buffer>\n"
    assert_raise(Fluent::ConfigError) { create_driver(conf) }
  end

  test 'plugin raises error when required param is empty string' do
    %w[kusto_endpoint database_name table_name].each do |param|
      conf_hash = @base_conf.dup
      conf_hash[param] = ''
      conf = config_str(conf_hash)
      assert_raise(Fluent::ConfigError, "Should fail when #{param} is empty") { create_driver(conf) }
    end
  end

  test 'plugin raises error when required param is whitespace only' do
    %w[kusto_endpoint database_name table_name].each do |param|
      conf_hash = @base_conf.dup
      conf_hash[param] = '   '
      conf = config_str(conf_hash)
      assert_raise(Fluent::ConfigError, "Should fail when #{param} is whitespace only") { create_driver(conf) }
    end
  end

  test 'plugin raises error when azure_cloud is invalid' do
    assert_raise(ArgumentError) { OutputConfiguration.new(@base_conf.merge('azure_cloud' => 'InvalidCloud').transform_keys(&:to_sym)).validate_configuration }
  end

  test 'plugin accepts valid managed_identity_client_id' do
    conf = config_str(
      @base_conf
        .merge('auth_type' => 'user_managed_identity')
        .merge('managed_identity_client_id' => 'valid_id')
        .merge('endpoint' => @base_conf['kusto_endpoint'])
        .merge('auth_type' => 'user_managed_identity') # Ensure required field is present
    )
    assert_nothing_raised { create_driver(conf) }
  end

  test 'plugin sets aad_endpoint based on azure_cloud' do
    clouds = {
      'AzureCloud' => 'https://login.microsoftonline.com',
      'AzureChinaCloud' => 'https://login.chinacloudapi.cn',
      'AzureUSGovernment' => 'https://login.microsoftonline.us'
    }
    base_conf_sym = @base_conf.transform_keys(&:to_sym)
    clouds.each do |cloud, aad_url|
      config = OutputConfiguration.new(
        base_conf_sym.merge(
          azure_cloud: cloud,
          client_app_id: 'dummy-client-id',
          client_app_secret: 'dummy-secret',
          tenant_id: 'dummy-tenant'
        )
      )
      assert_equal(aad_url, config.aad_endpoint)
    end
  end

  private

  def config_str(hash)
    hash.map { |k, v| "#{k} #{v}" }.unshift('@type kusto').join("\n")
  end

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(conf)
  end
end
