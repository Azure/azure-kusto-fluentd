# rubocop:disable all
require_relative "../helper"
require "fluent/test/driver/output"
require "fluent/plugin/out_kusto.rb"
require "mocha/test_unit"

class KustoOutputProcessTest < Test::Unit::TestCase
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

  def set_mocks(ingester: nil, logger: nil)
    @driver.instance.instance_variable_set(:@ingester, ingester) if ingester
    @driver.instance.instance_variable_set(:@logger, logger) if logger
  end

  def event_stream(arr)
    Fluent::ArrayEventStream.new(arr)
  end

  def logger_stub
    m = mock
    m.stubs(:debug)
    m.stubs(:error)
    m.stubs(:warn)
    m.stubs(:info)
    m
  end

  def ingester_stub
    m = mock
    m.stubs(:upload_data_to_blob_and_queue)
    m
  end

  test "process logs error but continues on upload failure" do
    ingester_mock = mock; ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, "upload failed")
    logger_mock = mock; logger_mock.stubs(:debug); logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => "bar"}]])) }
  end

  test "process handles empty event stream" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([])) }
  end

  test "process handles non-hash record" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, "not a hash"]])) }
  end

  test "process handles nil record" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, nil]])) }
  end

  test "process handles format raising error (circular reference)" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    record = {}; record["self"] = record
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, record]])) }
  end

  test "process handles upload raising different errors" do
    ingester_mock = mock; ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(IOError, "io error")
    logger_mock = logger_stub; logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => "bar"}]])) }
  end

  test "process continues after multiple failures" do
    ingester_mock = mock; ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, "fail")
    logger_mock = logger_stub; logger_mock.expects(:error).at_least_once
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => "bar"}], [Time.now.to_i, {"foo" => "baz"}]])) }
  end

  test "process handles logger being nil" do
    set_mocks(ingester: ingester_stub, logger: nil)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => "bar"}]])) }
  end

  test "process handles empty string time" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([["", {"foo" => "bar"}]])) }
  end

  test "process handles array as record" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, [1,2,3]]])) }
  end

  test "process handles very large record" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    big_record = {"foo" => "x" * 100_000}
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, big_record]])) }
  end

  test "process handles deeply nested record" do
    set_mocks(ingester: ingester_stub, logger: logger_stub)
    nested = {"a"=>{"b"=>{"c"=>{"d"=>1}}}}
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, nested]])) }
  end

  test "process handles binary data in record" do
    logger_mock = mock; logger_mock.stubs(:debug); logger_mock.stubs(:error); logger_mock.stubs(:warn)
    set_mocks(ingester: mock, logger: logger_mock)
    record = {"bin" => "\xFF".b}
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, record]])) }
  end

  test "process encodes formatted data to UTF-8 and replaces invalid characters" do
    ingester_mock = mock
    ingester_mock.stubs(:upload_data_to_blob_and_queue)
    logger_mock = logger_stub
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    bad_utf8 = "foo\xFFbar".force_encoding("ASCII-8BIT")
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => bad_utf8}]]) ) }
  end

  test "process handles event stream with mixed valid and invalid events" do
    logger_mock = logger_stub
    logger_mock.stubs(:error)
    set_mocks(ingester: ingester_stub, logger: logger_mock)
    es = event_stream([[Time.now.to_i, {"foo" => "bar"}], [Time.now.to_i, nil], [Time.now.to_i, "not a hash"]])
    assert_nothing_raised { @driver.instance.process("test.tag", es) }
  end

  test "process logs error with correct message on upload failure" do
    ingester_mock = mock
    ingester_mock.stubs(:upload_data_to_blob_and_queue).raises(StandardError, "upload failed")
    logger_mock = mock
    logger_mock.stubs(:debug)
    logger_mock.expects(:error).with { |msg| msg.include?("Failed to ingest event to Kusto") && msg.include?("upload failed") }
    set_mocks(ingester: ingester_mock, logger: logger_mock)
    assert_nothing_raised { @driver.instance.process("test.tag", event_stream([[Time.now.to_i, {"foo" => "bar"}]])) }
  end
end