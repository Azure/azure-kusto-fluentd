# rubocop:disable all
# frozen_string_literal: true

require 'ostruct'
require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_kusto'
require 'mocha/test_unit'

class FakeKustoError < StandardError
  def initialize(msg, permanent)
    super(msg)
    @permanent = permanent
  end

  def permanent?
    @permanent
  end

  def is_permanent?
    permanent?
  end

  def failure_code
    nil
  end

  def failure_sub_code
    nil
  end
end

class KustoOutputTryWriteTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(<<-CONF)
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      auth_type aad
      buffered true
      delayed true
    CONF
    @driver.instance.stubs(:commit_write)
  end

  def logger_stub
    m = mock
    m.stubs(:debug)
    m.stubs(:error)
    m.stubs(:info)
    m.stubs(:warn)
    m
  end

  def ingester_stub
    m = mock
    m.stubs(:upload_data_to_blob_and_queue)
    m
  end

  def set_mocks(ingester: nil, logger: nil)
    @driver.instance.instance_variable_set(:@ingester, ingester) if ingester
    @driver.instance.instance_variable_set(:@logger, logger) if logger
  end

  def chunk_stub(data: 'testdata', tag: 'test.tag', unique_id: 'uniqueid'.b, metadata: nil)
    c = mock
    c.stubs(:read).returns(data)
    c.stubs(:metadata).returns(metadata || OpenStruct.new(tag: tag))
    c.stubs(:unique_id).returns(unique_id)
    c
  end

  test 'try_write uploads compressed data to blob and queue with deferred commit' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2 # Give thread time to run
  end

  test 'try_write handles permanent Kusto error by dropping chunk' do
    ingester_mock = ingester_stub
    kusto_error = FakeKustoError.new('permanent fail', true)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 0.2
  end

  test 'try_write raises error on non-permanent Kusto error (triggers retry)' do
    ingester_mock = ingester_stub
    kusto_error = FakeKustoError.new('transient fail', false)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:transient)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(FakeKustoError) { @driver.instance.try_write(chunk) }
  end

  test 'try_write raises error on unknown error' do
    ingester_mock = ingester_stub
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(nil)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(IOError, 'io fail')
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(IOError) { @driver.instance.try_write(chunk) }
  end

  test 'try_write handles chunk metadata being nil' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub(metadata: nil)
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk metadata without tag method' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    metadata_obj = Object.new
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(metadata_obj)
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk unique_id being nil' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub(unique_id: nil)
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk.read returning nil' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub(data: nil)
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk.read returning empty string' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub(data: '')
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk.read raising error' do
    ingester_mock = mock
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    chunk = mock
    chunk.stubs(:read).raises(StandardError, 'read fail')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    assert_raise(StandardError) { @driver.instance.try_write(chunk) }
  end

  test 'try_write handles error in deferred commit thread' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).with(regexp_matches(/Error in deferred commit thread/)).at_least_once
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    # Simulate check_data_on_server raising error in thread
    @driver.instance.stubs(:check_data_on_server).raises(StandardError, 'thread fail')
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    @driver.instance.stubs(:commit_write)
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write handles chunk with very large data' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub(data: 'x' * 10_000_000)
    @driver.instance.expects(:commit_write).with(chunk.unique_id).once
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  test 'try_write is thread safe with concurrent calls' do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = chunk_stub
    @driver.instance.stubs(:commit_write)
    threads = 5.times.map do
      Thread.new { assert_nothing_raised { @driver.instance.try_write(chunk) } }
    end
    threads.each(&:join)
  end

  # Removed test cases: unique_id as integer, unique_id as array, tag as empty string, unique_id with special characters, ignores return value

  test 'try_write handles check_data_on_server always false (thread keeps running)' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(false)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    # We can't join the thread, but we can at least ensure no exception is raised
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2
  end

  # Test that logger.debug and logger.error are called on success and error
  test 'try_write logs debug and error messages appropriately' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).never
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    @driver.instance.stubs(:check_data_on_server).returns(true)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    @driver.instance.stubs(:commit_write)
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 1.2

    # Now test error logging
    ingester_mock = mock
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).at_least_once
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(FakeKustoError.new('permanent fail', true))
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 0.2
  end

  # Test that commit_write is not called if upload_data_to_blob_and_queue fails
  test 'try_write does not call commit_write if upload_data_to_blob_and_queue fails' do
    ingester_mock = mock
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    @driver.instance.instance_variable_set(:@ingester, ingester_mock)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    @driver.instance.expects(:commit_write).never
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(FakeKustoError.new('permanent fail', true))
    assert_nothing_raised { @driver.instance.try_write(chunk) }
    sleep 0.2
  end

  # Test behavior when @ingester is nil
  test 'try_write raises error if @ingester is nil' do
    @driver.instance.instance_variable_set(:@ingester, nil)
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    @driver.instance.instance_variable_set(:@logger, logger_mock)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    assert_raise(NoMethodError) { @driver.instance.try_write(chunk) }
  end

  # Test for a configuration option affecting try_write (example: delayed false disables deferred commit)
  test 'try_write commits immediately if delayed is false' do
    driver2 = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(<<-CONF)
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name testtable
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      buffered true
      delayed false
      auth_type aad
    CONF
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    driver2.instance.instance_variable_set(:@ingester, ingester_mock)
    driver2.instance.instance_variable_set(:@logger, logger_mock)
    driver2.instance.stubs(:check_data_on_server).returns(true)
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(OpenStruct.new(tag: 'test.tag'))
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    driver2.instance.expects(:commit_write).with(chunk.unique_id).once
    # Patch Thread.new to run inline for this test to avoid background thread issues
    orig_thread_new = Thread.method(:new)
    Thread.singleton_class.class_eval do
      define_method(:new) do |*_args, &block|
        block.call
      end
    end
    begin
      assert_nothing_raised { driver2.instance.try_write(chunk) }
    ensure
      Thread.singleton_class.class_eval do
        define_method(:new, orig_thread_new)
      end
    end
  end
end