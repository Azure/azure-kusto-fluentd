# rubocop:disable all
# frozen_string_literal: true

require 'ostruct'
require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_kusto'
require 'mocha/test_unit'

class KustoOutputWriteTest < Test::Unit::TestCase
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
      buffered true
      auth_type aad
    CONF
  end

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

  test 'write uploads compressed data to blob and queue' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, blob_name, _db, _table|
      assert_match(/fluentd_event_worker\d+_test\.tag_[0-9a-f]+\.json\.gz/, blob_name)
      true
    end
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write raises error on permanent Kusto error' do
    ingester_mock = mock
    kusto_error = FakeKustoError.new('permanent fail', true)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(FakeKustoError) { @driver.instance.write(chunk) }
  end

  test 'write raises error on non-permanent Kusto error (triggers retry)' do
    ingester_mock = mock
    kusto_error = FakeKustoError.new('transient fail', false)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:transient)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(FakeKustoError) { @driver.instance.write(chunk) }
  end

  test 'write raises error on unknown error' do
    ingester_mock = mock
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(nil)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(IOError, 'io fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(IOError) { @driver.instance.write(chunk) }
  end

  test 'write calls logger.info on success' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    logger_mock.stubs(:info)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write calls logger.error with correct message on permanent error' do
    ingester_mock = mock
    kusto_error = FakeKustoError.new('permanent fail', true)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    logger_mock.expects(:error).with(regexp_matches(/Dropping chunk .* due to permanent Kusto error/)).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_raise(FakeKustoError) { @driver.instance.write(chunk) }
  end

  test 'write works with different chunk metadata' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(tag: 'other.tag', unique_id: 'otherid'.b)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles empty chunk data' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(data: '')
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk.read raising error' do
    ingester_mock = ingester_stub
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    chunk.stubs(:read).raises(IOError, 'read fail')
    assert_raise(IOError) { @driver.instance.write(chunk) }
  end

  test 'write retries on non-permanent Kusto error up to buffer retry_max_times' do
    # Simulate Fluentd's retry mechanism by calling write multiple times
    ingester_mock = mock
    kusto_error = FakeKustoError.new('transient fail', false)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:transient)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    # Simulate retry_max_times = 3
    3.times do
      assert_raise(FakeKustoError) { @driver.instance.write(chunk) }
    end
  end

  test 'write stops retrying if permanent error occurs after retries' do
    ingester_mock = mock
    # First 2 attempts: non-permanent error, 3rd attempt: permanent error
    kusto_error_transient = FakeKustoError.new('transient fail', false)
    kusto_error_permanent = FakeKustoError.new('permanent fail', true)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:transient, :transient, :permanent)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error_transient, kusto_error_transient,
                                                            kusto_error_permanent)
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail')
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    2.times { assert_raise(FakeKustoError) { @driver.instance.write(chunk) } }
    assert_raise(FakeKustoError) { @driver.instance.write(chunk) } # Should be permanent error
  end

  test 'write succeeds after retries if error goes away' do
    ingester_mock = mock
    kusto_error = FakeKustoError.new('transient fail', false)
    KustoErrorHandler.stubs(:extract_kusto_error_type).returns(:transient, :transient, nil)
    KustoErrorHandler.stubs(:from_kusto_error_type).returns(kusto_error, kusto_error)
    # First 2 attempts raise, 3rd attempt succeeds
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, 'fail').then.raises(StandardError,
                                                                                                  'fail').then.returns(true)
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    2.times { assert_raise(FakeKustoError) { @driver.instance.write(chunk) } }
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk with nil metadata' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(metadata: nil)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk with nil unique_id' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(unique_id: nil)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk with nil tag' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(tag: nil)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk with very large data' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(data: 'x' * 10_000_000)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles logger that only responds to error' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    def logger_mock.debug(*)
      raise NoMethodError
    end
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles logger.error raising exception' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:debug)
    def logger_mock.error(*)
      raise 'logger error'
    end
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write is thread safe with concurrent calls' do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    chunk = chunk_stub
    threads = 5.times.map do
      Thread.new { assert_nothing_raised { @driver.instance.write(chunk) } }
    end
    threads.each(&:join)
  end

  test 'write handles logger with info but not error' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:info)
    def logger_mock.error(*)
      raise NoMethodError
    end
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk.metadata without tag method' do
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = mock
    logger_mock.stubs(:info)
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    metadata_obj = Object.new
    chunk = mock
    chunk.stubs(:read).returns('testdata')
    chunk.stubs(:metadata).returns(metadata_obj)
    chunk.stubs(:unique_id).returns('uniqueid'.b)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles unique_id as integer' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(unique_id: 12_345)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles unique_id as array' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(unique_id: [1, 2, 3])
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles chunk.read returning nil' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(data: nil)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end

  test 'write handles tag and unique_id with unicode and invalid bytes' do
    ingester_mock = ingester_stub
    ingester_mock.expects(:upload_data_to_blob_and_queue).once
    logger_mock = logger_stub
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    chunk = chunk_stub(tag: "t\u2603\xFF", unique_id: "\xFF\xFE\xFD".b)
    assert_nothing_raised { @driver.instance.write(chunk) }
  end
end
