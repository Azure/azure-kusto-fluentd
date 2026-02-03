# rubocop:disable all
# frozen_string_literal: true

require 'ostruct'
require_relative '../helper'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_kusto'
require 'mocha/test_unit'

class KustoOutputResolveTableNameTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  def create_driver(table_name = 'testtable')
    Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(<<-CONF)
      @type kusto
      endpoint https://example.kusto.windows.net
      database_name testdb
      table_name #{table_name}
      client_id dummy-client-id
      client_secret dummy-secret
      tenant_id dummy-tenant
      buffered true
      auth_type aad
    CONF
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

  def set_mocks(driver, ingester: nil, logger: nil)
    driver.instance.instance_variable_set(:@ingester, ingester) if ingester
    driver.instance.instance_variable_set(:@logger, logger) if logger
  end

  def chunk_stub(data: 'testdata', tag: 'test.tag', unique_id: 'uniqueid'.b, metadata: nil)
    c = mock
    c.stubs(:read).returns(data)
    c.stubs(:metadata).returns(metadata || OpenStruct.new(tag: tag))
    c.stubs(:unique_id).returns(unique_id)
    c
  end

  # ==========================================
  # resolve_table_name method tests
  # ==========================================

  test 'resolve_table_name returns static table name when no placeholders' do
    driver = create_driver('MyStaticTable')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'MyStaticTable', result
  end

  test 'resolve_table_name replaces ${tag} with full tag' do
    driver = create_driver('${tag}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'app_orders_created', result  # dots replaced with underscores
  end

  test 'resolve_table_name replaces ${tag_parts[0]} with first part' do
    driver = create_driver('${tag_parts[0]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'app', result
  end

  test 'resolve_table_name replaces ${tag_parts[1]} with second part' do
    driver = create_driver('${tag_parts[1]}')
    result = driver.instance.send(:resolve_table_name, 'custom.orders.created')
    assert_equal 'orders', result
  end

  test 'resolve_table_name replaces ${tag_parts[2]} with third part' do
    driver = create_driver('${tag_parts[2]}')
    result = driver.instance.send(:resolve_table_name, 'custom.orders.created')
    assert_equal 'created', result
  end

  test 'resolve_table_name returns empty string for out-of-bounds tag_parts index' do
    driver = create_driver('${tag_parts[5]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders')
    assert_equal '', result
  end

  test 'resolve_table_name replaces ${tag_prefix[1]} with first part' do
    driver = create_driver('${tag_prefix[1]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'app', result
  end

  test 'resolve_table_name replaces ${tag_prefix[2]} with first two parts' do
    driver = create_driver('${tag_prefix[2]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'app_orders', result  # dot replaced with underscore
  end

  test 'resolve_table_name replaces ${tag_suffix[1]} with last part' do
    driver = create_driver('${tag_suffix[1]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'created', result
  end

  test 'resolve_table_name replaces ${tag_suffix[2]} with last two parts' do
    driver = create_driver('${tag_suffix[2]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders.created')
    assert_equal 'orders_created', result  # dot replaced with underscore
  end

  test 'resolve_table_name handles mixed static and placeholder' do
    driver = create_driver('prefix_${tag_parts[1]}_suffix')
    result = driver.instance.send(:resolve_table_name, 'custom.orders.events')
    assert_equal 'prefix_orders_suffix', result
  end

  test 'resolve_table_name handles multiple placeholders' do
    driver = create_driver('${tag_parts[0]}_${tag_parts[1]}')
    result = driver.instance.send(:resolve_table_name, 'env.production.logs')
    assert_equal 'env_production', result
  end

  test 'resolve_table_name sanitizes special characters' do
    driver = create_driver('${tag}')
    result = driver.instance.send(:resolve_table_name, 'app-name.service:type.logs')
    assert_equal 'app_name_service_type_logs', result  # special chars replaced with underscores
  end

  test 'resolve_table_name handles single-part tag' do
    driver = create_driver('${tag_parts[0]}')
    result = driver.instance.send(:resolve_table_name, 'singletag')
    assert_equal 'singletag', result
  end

  test 'resolve_table_name returns empty for tag_parts on single-part tag with index > 0' do
    driver = create_driver('${tag_parts[1]}')
    result = driver.instance.send(:resolve_table_name, 'singletag')
    assert_equal '', result
  end

  test 'resolve_table_name handles empty tag' do
    driver = create_driver('${tag}')
    result = driver.instance.send(:resolve_table_name, '')
    assert_equal '', result
  end

  test 'resolve_table_name handles nil tag' do
    driver = create_driver('${tag}')
    result = driver.instance.send(:resolve_table_name, nil)
    assert_equal '', result
  end

  # ==========================================
  # Integration tests with write method
  # ==========================================

  test 'write uses resolved table name with tag_parts placeholder' do
    driver = create_driver('${tag_parts[1]}')
    ingester_mock = mock
    # Verify the table name passed to ingester is 'orders' (from tag custom.orders.events)
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'testdb', db
      assert_equal 'orders', table
      true
    end
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    chunk = chunk_stub(tag: 'custom.orders.events')
    assert_nothing_raised { driver.instance.write(chunk) }
  end

  test 'write uses resolved table name with tag placeholder' do
    driver = create_driver('${tag}')
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'testdb', db
      assert_equal 'app_service_logs', table  # dots converted to underscores
      true
    end
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    chunk = chunk_stub(tag: 'app.service.logs')
    assert_nothing_raised { driver.instance.write(chunk) }
  end

  test 'write uses static table name when no placeholders' do
    driver = create_driver('MyStaticTable')
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'testdb', db
      assert_equal 'MyStaticTable', table
      true
    end
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    chunk = chunk_stub(tag: 'any.tag.here')
    assert_nothing_raised { driver.instance.write(chunk) }
  end

  test 'write routes different tags to different tables' do
    driver = create_driver('${tag_parts[1]}')
    ingester_mock = mock
    
    # First call should use 'orders' table
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'orders', table
      true
    end
    
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    chunk1 = chunk_stub(tag: 'custom.orders.created')
    assert_nothing_raised { driver.instance.write(chunk1) }
    
    # Second call should use 'users' table
    ingester_mock2 = mock
    ingester_mock2.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'users', table
      true
    end
    set_mocks(driver, ingester: ingester_mock2, logger: logger_stub)
    chunk2 = chunk_stub(tag: 'custom.users.signup')
    assert_nothing_raised { driver.instance.write(chunk2) }
  end

  # ==========================================
  # Edge cases and special characters
  # ==========================================

  test 'resolve_table_name handles tag with numbers' do
    driver = create_driver('${tag_parts[1]}')
    result = driver.instance.send(:resolve_table_name, 'app.table123.logs')
    assert_equal 'table123', result
  end

  test 'resolve_table_name handles tag with underscores' do
    driver = create_driver('${tag_parts[1]}')
    result = driver.instance.send(:resolve_table_name, 'app.my_table.logs')
    assert_equal 'my_table', result
  end

  test 'resolve_table_name preserves underscores' do
    driver = create_driver('my_${tag_parts[1]}_table')
    result = driver.instance.send(:resolve_table_name, 'app.orders.logs')
    assert_equal 'my_orders_table', result
  end

  test 'resolve_table_name handles deeply nested tags' do
    driver = create_driver('${tag_parts[3]}')
    result = driver.instance.send(:resolve_table_name, 'level1.level2.level3.level4.level5')
    assert_equal 'level4', result
  end

  test 'resolve_table_name with tag_prefix handles more parts than exist' do
    driver = create_driver('${tag_prefix[10]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders')
    assert_equal 'app_orders', result  # Returns all parts when count exceeds available
  end

  test 'resolve_table_name with tag_suffix handles more parts than exist' do
    driver = create_driver('${tag_suffix[10]}')
    result = driver.instance.send(:resolve_table_name, 'app.orders')
    assert_equal 'app_orders', result  # Returns all parts when count exceeds available
  end

  # ==========================================
  # Tests for try_write with dynamic table names
  # ==========================================

  test 'try_write uses resolved table name' do
    driver = create_driver('${tag_parts[1]}')
    driver.instance.instance_variable_set(:@delayed, false)
    driver.instance.instance_variable_set(:@shutdown_called, false)
    
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'orders', table
      true
    end
    
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    
    # Need to stub commit_write since we're in non-delayed mode
    driver.instance.stubs(:commit_write)
    
    chunk = chunk_stub(
      data: '{"tag":"custom.orders","timestamp":"2024-01-01","record":{"key":"value"}}',
      tag: 'custom.orders.events'
    )
    assert_nothing_raised { driver.instance.try_write(chunk) }
  end

  # ==========================================
  # Tests for process with dynamic table names
  # ==========================================

  test 'process uses resolved table name' do
    driver = create_driver('${tag_parts[1]}')
    
    ingester_mock = mock
    ingester_mock.expects(:upload_data_to_blob_and_queue).once.with do |_data, _blob_name, db, table, _compression, _mapping|
      assert_equal 'orders', table
      true
    end
    
    set_mocks(driver, ingester: ingester_mock, logger: logger_stub)
    
    # Create a simple event stream
    es = mock
    es.stubs(:each).yields(Time.now.to_i, {'message' => 'test'})
    
    assert_nothing_raised { driver.instance.process('custom.orders.events', es) }
  end
end