# rubocop:disable all
# frozen_string_literal: true

require_relative '../helper'
require 'fluent/plugin/out_kusto'

class KustoOutputFormatTest < Test::Unit::TestCase
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

  test 'format includes tag, timestamp, and record fields' do
    tag = 'mytag'
    time = Time.utc(2024, 1, 1, 12, 0, 0).to_i
    record = { 'foo' => 'bar', 'baz' => 1 }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 'mytag', parsed['tag']
    assert_equal Time.at(time).utc.iso8601, parsed['timestamp']
    assert_equal({ 'foo' => 'bar', 'baz' => 1 }, parsed['record'])
  end

  test "format prefers record['tag'] over argument tag" do
    tag = 'outertag'
    time = Time.now.to_i
    record = { 'tag' => 'innertag', 'foo' => 'bar' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 'innertag', parsed['tag']
  end

  test "format uses record['tag'] over all fallbacks" do
    tag = 'outer'
    time = 1
    record = { 'tag' => 'inner', 'host' => 'hostval', 'user' => 'userval', 'message' => '1.2.3.4' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 'inner', parsed['tag']
  end

  test "format uses record['time'] over record['timestamp'] and time param" do
    tag = 't'
    time = 123
    record = { 'time' => 't1', 'timestamp' => 't2' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 't1', parsed['timestamp']
  end

  test "format uses record['timestamp'] over time param" do
    tag = 't'
    time = 123
    record = { 'timestamp' => 't2' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 't2', parsed['timestamp']
  end

  test 'format falls back to default_tag if no tag or fallback fields' do
    tag = nil
    time = nil
    record = { 'foo' => 'bar' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 'default_tag', parsed['tag']
  end

  test 'format removes tag and time from record in output' do
    tag = 't'
    time = 123
    record = { 'foo' => 1, 'tag' => 't', 'time' => 'sometime' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_not_include parsed['record'].keys, 'tag'
    assert_not_include parsed['record'].keys, 'time'
  end

  test 'format falls back to host, user, or IP in message if tag is missing' do
    tag = nil
    time = 0
    record = { 'host' => 'hostval', 'foo' => 1 }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 'hostval', parsed['tag']
    record2 = { 'user' => 'userval', 'foo' => 2 }
    json2 = @driver.instance.format(tag, time, record2)
    parsed2 = JSON.parse(json2)
    assert_equal 'userval', parsed2['tag']
    record3 = { 'message' => '192.168.1.1 something happened', 'foo' => 3 }
    json3 = @driver.instance.format(tag, time, record3)
    parsed3 = JSON.parse(json3)
    assert_equal '192.168.1.1', parsed3['tag']
  end

  test 'format falls back to date/time in log message if timestamp missing' do
    tag = 't'
    time = nil
    record = { 'foo' => 'bar', 'msg' => '[01/Jan/2024:12:00:00 +0000] log entry' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal '[01/Jan/2024:12:00:00 +0000] log entry', parsed['timestamp']
  end

  test 'format sets timestamp to empty string if no time info' do
    tag = 't'
    time = nil
    record = { 'foo' => 'bar' }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal '', parsed['timestamp']
  end

  test 'format uses first date/time pattern found in record' do
    tag = 't'
    time = nil
    record = {
      'foo' => 'no date here',
      'bar' => '[01/Jan/2024:12:00:00 +0000] log entry',
      'baz' => '[02/Feb/2025:13:00:00 +0000] another'
    }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal '[01/Jan/2024:12:00:00 +0000] log entry', parsed['timestamp']
  end

  test 'format output is valid JSON and ends with newline' do
    tag = 't'
    time = 1
    record = { 'foo' => 'bar' }
    json = @driver.instance.format(tag, time, record)
    assert_nothing_raised { JSON.parse(json) }
    assert_match(/\n\z/, json)
  end

  test 'format raises error on circular reference' do
    tag = 't'
    time = 1
    record = {}
    record['self'] = record
    assert_raise_with_message(RuntimeError, 'Circular reference detected in record') do
      @driver.instance.format(tag, time, record)
    end
  end

  test "format uses any key containing 'time' or 'date' for timestamp" do
    tag = 't'
    time = 123
    record = { 'event_time' => '2024-01-01T12:00:00Z', 'foo' => 1 }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal '2024-01-01T12:00:00Z', parsed['timestamp']

    record2 = { 'logdate' => '2023-12-31', 'foo' => 2 }
    json2 = @driver.instance.format(tag, time, record2)
    parsed2 = JSON.parse(json2)
    assert_equal '2023-12-31', parsed2['timestamp']

    record3 = { 'sometime' => '', 'foo' => 3 }
    json3 = @driver.instance.format(tag, time, record3)
    parsed3 = JSON.parse(json3)
    # Falls back to time param if key exists but is empty
    assert_equal Time.at(time).utc.iso8601, parsed3['timestamp']
  end

  test 'format handles record with JSON string value' do
    tag = 't'
    time = 1
    json_str = '{"foo": "bar", "baz": 1}'
    record = { 'data' => json_str }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal json_str, parsed['record']['data']
  end

  test 'format handles record with CSV string value' do
    tag = 't'
    time = 1
    csv_str = "foo,bar,baz\n1,2,3"
    record = { 'csv_data' => csv_str }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal csv_str, parsed['record']['csv_data']
  end

  test 'format handles record with nested hash (parsed JSON)' do
    tag = 't'
    time = 1
    nested = { 'foo' => 'bar', 'baz' => 1 }
    record = { 'nested' => nested }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal nested, parsed['record']['nested']
  end

  test 'format handles nil record gracefully' do
    tag = 't'
    time = 1
    record = nil
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 't', parsed['tag']
    assert_equal Time.at(time).utc.iso8601, parsed['timestamp']
    assert_equal({}, parsed['record'])
  end

  test 'format handles array record' do
    tag = 't'
    time = 1
    record = [1, 2, 3]
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 't', parsed['tag']
    assert_equal Time.at(time).utc.iso8601, parsed['timestamp']
    assert_equal [1, 2, 3], parsed['record']
  end

  test 'format handles deeply nested hash and array' do
    tag = 't'
    time = 1
    record = { 'a' => { 'b' => [1, { 'c' => 2 }] } }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal({ 'a' => { 'b' => [1, { 'c' => 2 }] } }, parsed['record'])
  end

  test 'format handles special characters in keys and values' do
    tag = 't'
    time = 1
    record = { "uni\u2603" => "snowman\u2603", "newline" => "line1\nline2" }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal "snowman\u2603", parsed['record']["uni\u2603"]
    assert_equal "line1\nline2", parsed['record']['newline']
  end

  test 'format handles very large record' do
    tag = 't'
    time = 1
    record = {}
    1000.times { |i| record["key#{i}"] = i }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal 1000, parsed['record'].size
  end

  test 'format handles boolean, nil, and float values' do
    tag = 't'
    time = 1
    record = { 'bool' => true, 'nilval' => nil, 'float' => 1.23 }
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal true, parsed['record']['bool']
    assert_nil parsed['record']['nilval']
    assert_in_delta 1.23, parsed['record']['float'], 0.0001
  end

  test 'format handles empty hash and array' do
    tag = 't'
    time = 1
    record = {}
    json = @driver.instance.format(tag, time, record)
    parsed = JSON.parse(json)
    assert_equal({}, parsed['record'])
    record2 = []
    json2 = @driver.instance.format(tag, time, record2)
    parsed2 = JSON.parse(json2)
    assert_equal [], parsed2['record']
  end
end
