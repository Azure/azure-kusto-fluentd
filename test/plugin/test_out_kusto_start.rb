# rubocop:disable all

require_relative '../helper'
require 'fluent/plugin/out_kusto'
require 'mocha/test_unit'

# Dummy classes for use in tests
OutputConfigurationDummy = Class.new do
  def initialize(*_args)
    dummy = Object.new
    def dummy.method_missing(name, *_args)
      name == :logger ? Logger.new(nil) : 'dummy_value'
    end

    def dummy.respond_to_missing?(name, include_private = false)
      name == :logger || super
    end
    AadTokenProvider.new(dummy).send(:post_token_request)
  end

  def logger
    Logger.new(nil)
  end

  def table_name
    'testtable'
  end

  def database_name
    'testdb'
  end
end
IngesterDummy = Class.new do
  def initialize(*args); end
end
TestIngesterMock1 = Class.new { def initialize(config); end }
TestIngesterMock2 = Class.new { def initialize(config); end }
TestIngesterMock3 = Class.new do
  @called_with = nil
  def initialize(config)
    self.class.called_with = config
  end

  class << self
    attr_reader :called_with
  end

  class << self
    attr_writer :called_with
  end
end
TestIngesterMock4 = Class.new { def initialize(config); end }
TestIngesterMock5 = Class.new { def initialize(config); end }
TestIngesterMock6 = Class.new { def initialize(config); end }
TestIngesterMock7 = Class.new { def initialize(config); end }

class KustoOutputStartTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      buffered true
      auth_type aad      
    CONF
    @aad_token_stub = { 'access_token' => 'fake', 'expires_in' => 3600 }
    AadTokenProvider.any_instance.stubs(:post_token_request).returns(@aad_token_stub)
  end

  test 'plugin initializes and starts successfully with valid config' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_nothing_raised { driver.instance.start }
    assert_equal output_config_mock.logger, driver.instance.instance_variable_get(:@logger)
  end

  test 'start propagates error if OutputConfiguration.new fails' do
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).raises(StandardError, 'init failed')
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock2)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_raise(StandardError, 'init failed') { driver.instance.start }
  end

  test 'start initializes OutputConfiguration and Ingester with expected arguments' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock3)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    assert_equal output_config_mock, TestIngesterMock3.called_with,
                 'Ingester should be initialized with OutputConfiguration'
  end

  test 'start sets logger from OutputConfiguration' do
    output_config_mock = mock
    logger_mock = mock
    output_config_mock.stubs(:logger).returns(logger_mock)
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    TestIngesterMock4 = Class.new { def initialize(config); end }
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock4)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    assert_equal logger_mock, driver.instance.instance_variable_get(:@logger)
  end

  test 'start is idempotent (multiple calls do not error)' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    TestIngesterMock5 = Class.new { def initialize(config); end }
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock5)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_nothing_raised { driver.instance.start }
    assert_nothing_raised { driver.instance.start }
  end

  test 'start cleans up resources on shutdown' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    output_config_mock.stubs(:kusto_endpoint).returns('https://example.kusto.windows.net')
    output_config_mock.stubs(:access_token).returns('fake-access-token')
    ingester_mock = mock
    ingester_mock.stubs(:access_token).returns('fake-access-token')
    ingester_mock.expects(:shutdown).once
    # Add client and token_provider mocks for shutdown
    client_mock = mock
    token_provider_mock = mock
    token_provider_mock.stubs(:fetch_token).returns('fake-access-token')
    client_mock.stubs(:token_provider).returns(token_provider_mock)
    ingester_mock.stubs(:client).returns(client_mock)
    ingester_mock.stubs(:token_provider).returns(token_provider_mock)
    TestIngesterMock6 = Class.new { def initialize(config); end }
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock6)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    driver.instance.instance_variable_set(:@ingester, ingester_mock)
    driver.instance.shutdown
  end

  test 'start is thread-safe in multi-worker environment' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    TestIngesterMock7 = Class.new { def initialize(config); end }
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock7)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    threads = []
    5.times { threads << Thread.new { assert_nothing_raised { driver.instance.start } } }
    threads.each(&:join)
  end

  test 'start triggers authentication logic and handles failures' do
    AadTokenProvider.any_instance.stubs(:post_token_request).raises(StandardError, 'auth failed')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, OutputConfigurationDummy)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, IngesterDummy)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_raise(StandardError, 'auth failed') { driver.instance.start }
  end

  test 'start passes all config params to OutputConfiguration' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.expects(:new).with(
      has_entries(
        client_app_id: 'dummy-client-id',
        client_app_secret: 'dummy-secret',
        tenant_id: 'dummy-tenant',
        kusto_endpoint: 'https://example.kusto.windows.net',
        database_name: 'testdb',
        table_name: 'testtable',
        azure_cloud: 'AzureCloud',
        managed_identity_client_id: nil
      )
    ).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_nothing_raised { driver.instance.start }
  end

  test 'start raises Fluent::ConfigError if required config missing' do
    bad_conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      # database_name missing
      table_name testtable
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      buffered true
      auth_type aad
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    assert_raise(Fluent::ConfigError) { driver.configure(bad_conf) }
  end

  test 'start sets plugin state variables' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    output_config_mock.stubs(:kusto_endpoint).returns('https://example.kusto.windows.net')
    output_config_mock.stubs(:access_token).returns('fake-access-token')
    # Add stubs for required AAD fields
    output_config_mock.stubs(:client_app_id).returns('dummy-client-id')
    output_config_mock.stubs(:client_app_secret).returns('dummy-secret')
    output_config_mock.stubs(:tenant_id).returns('dummy-tenant')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    assert_not_nil driver.instance.instance_variable_get(:@plugin_start_time)
    assert_equal 0, driver.instance.instance_variable_get(:@total_bytes_ingested)
    assert_equal 'testtable', driver.instance.instance_variable_get(:@table_name)
    assert_equal 'testdb', driver.instance.instance_variable_get(:@database_name)
  end

  test 'start raises error for invalid azure_cloud value' do
    conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      buffered true
      azure_cloud InvalidCloud
      auth_type aad      
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    driver.configure(conf)
    assert_raise(ArgumentError) { driver.instance.start }
  end

  test 'start handles missing logger in OutputConfiguration' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(nil)
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_nothing_raised { driver.instance.start }
    assert_nil driver.instance.instance_variable_get(:@logger)
  end

  test 'start raises error if OutputConfiguration returns nil for required fields' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns(nil)
    output_config_mock.stubs(:database_name).returns(nil)
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    assert_nil driver.instance.instance_variable_get(:@table_name)
    assert_nil driver.instance.instance_variable_get(:@database_name)
  end

  test 'start raises error if Ingester initialization fails' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    failing_ingester = Class.new { def initialize(_); raise 'ingester failed'; end }
    Object.const_set(:Ingester, failing_ingester)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_raise(RuntimeError, 'ingester failed') { driver.instance.start }
  end

  test 'start with minimal required config succeeds' do
    minimal_conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      auth_type aad
      table_name testtable
      buffered true
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    assert_nothing_raised { driver.configure(minimal_conf); driver.instance.start }
  end

  test 'start with managed identity only and no AAD params' do
    mi_conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      buffered true
      auth_type user_managed_identity 
      managed_identity_client_id test-mi-id
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    assert_nothing_raised { driver.configure(mi_conf); driver.instance.start }
  end

  test 'start sets correct default values for optional config params' do
    conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      auth_type system_managed_identity
      buffered true
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    driver.configure(conf)
    assert_equal false, driver.instance.delayed
    assert_equal 'AzureCloud', driver.instance.azure_cloud
  end

  test 'start with extra unknown config params does not error' do
    conf = <<-CONF
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      buffered true
      auth_type aad
      unknown_param some_value
    CONF
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput)
    assert_nothing_raised { driver.configure(conf); driver.instance.start }
  end

  test 'start after shutdown does not reinitialize resources' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    output_config_mock.stubs(:kusto_endpoint).returns('https://example.kusto.windows.net')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    driver.instance.start
    driver.instance.shutdown
    assert_nothing_raised { driver.instance.start }
  end

  test 'start raises error if OutputConfiguration is nil' do
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(nil)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    Object.const_set(:Ingester, TestIngesterMock1)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_raise(NoMethodError) { driver.instance.start }
  end

  test 'start raises error if Ingester class is missing' do
    output_config_mock = mock
    output_config_mock.stubs(:logger).returns(Logger.new(nil))
    output_config_mock.stubs(:table_name).returns('testtable')
    output_config_mock.stubs(:database_name).returns('testdb')
    Object.send(:remove_const, :OutputConfiguration) if Object.const_defined?(:OutputConfiguration)
    Object.const_set(:OutputConfiguration, Class.new)
    OutputConfiguration.stubs(:new).returns(output_config_mock)
    Object.send(:remove_const, :Ingester) if Object.const_defined?(:Ingester)
    driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    assert_raise(NameError) { driver.instance.start }
  end
end
