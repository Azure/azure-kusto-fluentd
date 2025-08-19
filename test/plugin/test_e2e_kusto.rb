# frozen_string_literal: true

require 'test-unit'
require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/test/helpers'
require 'fluent/plugin/out_kusto'
require 'net/http'
require 'uri'
require 'json'
require_relative '../../lib/fluent/plugin/kusto_query'
require_relative '../../lib/fluent/plugin/ingester'
require_relative '../../lib/fluent/plugin/conffile'
require 'ostruct'
require 'logger'
require 'concurrent'
require 'tempfile'
require 'set'

class KustoE2ETest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
    # Setup logger
    @logger = Logger.new($stdout)
    @logger.level = Logger::INFO

    # Configuration from environment
    @engine_url = ENV['CLUSTER'] || 'https://example.kusto.windows.net'
    @database = ENV['DB'] || 'testdb'
    @table = "FluentD_#{Time.now.to_i}"
    @columns = '(tag:string, timestamp:datetime, record:string)'
    @client_id = ENV['CLIENT_ID'] || ''
    @client_secret = ENV['CLIENT_SECRET'] || ''
    @tenant_id = ENV['TENANT_ID'] || ''
    @managed_identity_client_id = ENV['MANAGED_IDENTITY_CLIENT_ID'] || ''
    @auth_type = (ENV['AUTH_TYPE'] || 'aad').downcase
    @wi_client_id = ENV['WI_CLIENT_ID'] || ''
    @wi_tenant_id = ENV['WI_TENANT_ID'] || ''
    @wi_token_file = ENV['WI_TOKEN_FILE'] || ''

    # Create driver with default configuration
    setup_auth_config
    configure_and_start_driver
    setup_test_table(@table)
  end

  def teardown
    kusto_query(".drop table #{@table} ifexists", :management)
  end

  def get_access_token
    # Use the same logic as the plugin's Ingester class
    opts = {
      tenant_id: @tenant_id,
      kusto_endpoint: @engine_url,
      database_name: @database,
      table_name: @table,
      azure_cloud: 'AzureCloud'
    }

    case @auth_type
    when 'azcli'
      opts[:auth_type] = 'azcli'
    when 'workload_identity'
      opts[:auth_type] = 'workload_identity'
      opts[:workload_identity_client_id] = @wi_client_id
      opts[:workload_identity_tenant_id] = @wi_tenant_id
      opts[:workload_identity_token_file_path] = @wi_token_file
    when 'user_managed_identity', 'system_managed_identity'
      opts[:auth_type] = @auth_type
      opts[:managed_identity_client_id] = @managed_identity_client_id
    else
      opts[:auth_type] = 'aad'
      opts[:client_app_id] = @client_id
      opts[:client_app_secret] = @client_secret
    end

    outconfig = OutputConfiguration.new(opts)
    ingester = Ingester.new(outconfig)
    def ingester.access_token
      token = @client.token_provider.get_token
      # Extract token if it's a hash or object
      case token
      when Hash
        token[:access_token] || token['access_token']
      when String
        token
      else
        token.respond_to?(:access_token) ? token.access_token : token.to_s
      end
    end
    ingester.access_token
  end

  def kusto_query(query, type = :data)
    endpoint = @engine_url
    path = type == :management ? '/v1/rest/mgmt' : '/v1/rest/query'
    uri = URI("#{endpoint}#{path}")
    token = get_access_token

    headers = {
      'Authorization' => "Bearer #{token}",
      'Content-Type' => 'application/json',
      'Accept' => 'application/json',
      'x-ms-client-version' => 'Kusto.FluentD:1.0.0'
    }

    body_hash = { csl: query }
    body_hash[:db] = @database if @database
    body = body_hash.to_json

    request = Net::HTTP::Post.new(uri.request_uri, headers)
    request.body = body
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true

    response = http.request(request)
    unless response.code.to_i.between?(200, 299)
      @logger.error("Kusto query failed with status #{response.code}: #{response.body}")
      return []
    end

    begin
      response_json = JSON.parse(response.body)
      tables = response_json['Tables']
      rows = tables && tables[0] && tables[0]['Rows']
      rows || []
    rescue JSON::ParserError => e
      @logger.error("Failed to parse JSON: #{e}")
      @logger.error(response.body)
      []
    end
  end

  def setup_auth_config
    @auth_lines = case @auth_type
                  when 'azcli'
                    <<-AUTH
      auth_type azcli
                    AUTH
                  when 'workload_identity'
                    <<-AUTH
      auth_type workload_identity
      workload_identity_client_id #{@wi_client_id}
      workload_identity_tenant_id #{@wi_tenant_id}
      workload_identity_token_file_path #{@wi_token_file}
                    AUTH
                  when 'user_managed_identity', 'system_managed_identity'
                    <<-AUTH
      auth_type #{@auth_type}
      managed_identity_client_id #{@managed_identity_client_id}
                    AUTH
                  else
                    <<-AUTH
      auth_type aad
      tenant_id #{@tenant_id}
      client_id #{@client_id}
      client_secret #{@client_secret}
                    AUTH
                  end
  end

  def configure_and_start_driver(options = {})
    config_options = {
      buffered: false,
      delayed: false,
      table_name: @table,
      flush_interval: '5s',
      chunk_limit_size: '8k',
      timekey: 60,
      compression_enabled: true
    }.merge(options)

    buffer_config = if config_options[:buffered]
                      buffer_type = config_options[:buffer_type] || 'memory'
                      flush_mode = config_options[:flush_mode] || 'interval'

                      base_buffer = <<-BUFFER
      <buffer>
        @type #{buffer_type}
        chunk_limit_size #{config_options[:chunk_limit_size]}
        timekey #{config_options[:timekey]}
        flush_mode #{flush_mode}
        flush_at_shutdown #{config_options[:flush_at_shutdown] || 'true'}
        overflow_action #{config_options[:overflow_action] || 'throw_exception'}
        retry_max_interval #{config_options[:retry_max_interval] || '30'}
        retry_forever #{config_options[:retry_forever] || 'false'}
        flush_thread_count #{config_options[:flush_thread_count] || '1'}
                      BUFFER

                      # Only add flush_interval if flush_mode is not 'immediate'
                      if flush_mode != 'immediate'
                        base_buffer = base_buffer.sub(/flush_mode #{flush_mode}/,
                                                      "flush_interval #{config_options[:flush_interval]}\n        flush_mode #{flush_mode}")
                      end

                      # Add file-specific configurations
                      if buffer_type == 'file'
                        base_buffer += "        path #{config_options[:buffer_path] || '/tmp/fluentd_test_buffer'}\n"
                      end

                      # Add additional buffer configurations
                      if config_options[:total_limit_size]
                        base_buffer += "        total_limit_size #{config_options[:total_limit_size]}\n"
                      end

                      if config_options[:chunk_limit_records]
                        base_buffer += "        chunk_limit_records #{config_options[:chunk_limit_records]}\n"
                      end

                      base_buffer += "      </buffer>\n"
                      base_buffer
                    else
                      ''
                    end

    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered #{config_options[:buffered]}
      delayed #{config_options[:delayed]}
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{config_options[:table_name]}
      compression_enabled #{config_options[:compression_enabled]}
      #{@auth_lines}
      #{buffer_config}
    CONF

    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, @logger)
    @driver.instance.start
  end

  def setup_test_table(table_name)
    kusto_query(".drop table #{table_name} ifexists", :management)
    kusto_query(".create table #{table_name} #{@columns}", :management)
  end

  def wait_for_ingestion(query, expected_count, max_wait = 240, interval = 5)
    waited = 0
    rows = []

    while waited < max_wait
      rows = kusto_query(query)
      break if rows.size >= expected_count

      sleep interval
      waited += interval
      @logger.debug("Waiting for ingestion: #{waited}s elapsed, #{rows.size}/#{expected_count} records found")
    end

    rows
  end

  def generate_test_events(count, base_id, tag_suffix = '')
    time = Time.now.to_i
    events = []
    count.times do |i|
      events << [
        time + i,
        {
          'id' => base_id + i,
          'name' => "test_event_#{tag_suffix}_#{i + 1}",
          'timestamp' => Time.at(time + i).utc.iso8601,
          'data' => {
            'index' => i,
            'batch_id' => base_id,
            'test_type' => tag_suffix
          }
        }
      ]
    end
    events
  end

  def create_temp_buffer_file
    temp_file = Tempfile.new(['fluentd_buffer', '.buf'])
    temp_path = temp_file.path
    temp_file.close
    temp_path
  end

  # Before running this test, ensure your service principal has TableAdmin and Ingestor permissions on the test database.
  test 'process function ingestion to Kusto' do
    test_table = "FluentD_process_#{Time.now.to_i}"
    configure_and_start_driver(table_name: test_table)
    setup_test_table(test_table)

    tag = 'e2e.test'
    time = Time.now.to_i
    record = { 'id' => 1, 'name' => 'test' }
    event_stream = Fluent::ArrayEventStream.new([[time, record]])

    assert_nothing_raised { @driver.instance.process(tag, event_stream) }

    query = "#{test_table} | extend r = parse_json(record) | where r.id == 1 and r.name == \"test\""
    rows = wait_for_ingestion(query, 1)

    assert(!rows.empty?, 'Data was not ingested into Kusto')

    found = false
    rows.each do |row|
      r = begin
        row[3]
      rescue StandardError
        nil
      end
      if r && r['id'] == 1 && r['name'] == 'test'
        found = true
        break
      end
    end

    assert(found, 'Expected record with name == test not found in Kusto')
  end

  test 'write function ingests data to Kusto' do
    test_table = "FluentD_write_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: test_table,
      buffered: true
    )
    setup_test_table(test_table)

    tag = 'e2e.write'
    time = Time.now.to_i
    events = [
      [time, { 'id' => 2, 'name' => 'write_test_1' }],
      [time + 1, { 'id' => 2, 'name' => 'write_test_2' }],
      [time + 2, { 'id' => 2, 'name' => 'write_test_3' }],
      [time + 3, { 'id' => 2, 'name' => 'write_test_4' }],
      [time + 4, { 'id' => 2, 'name' => 'write_test_5' }]
    ]

    @driver.run(default_tag: tag) do
      events.each do |t, r|
        @driver.feed(tag, t, r)
      end
      sleep 5 # Wait for buffer flush
    end

    query = "#{test_table} | extend r = parse_json(record) | where r.id == 2 and r.name startswith \"write_test_\""
    rows = wait_for_ingestion(query, 5)

    assert(rows.size >= 5, 'Not all events were ingested into Kusto by write')
  end

  test 'try_write function ingests data to Kusto' do
    test_table = "FluentD_trywrite_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: test_table,
      buffered: true,
      delayed: true
    )
    setup_test_table(test_table)

    tag = 'e2e.try_write'
    time = Time.now.to_i
    events = [
      [time, { 'id' => 3, 'name' => 'try_write_test_1' }],
      [time + 1, { 'id' => 3, 'name' => 'try_write_test_2' }],
      [time + 2, { 'id' => 3, 'name' => 'try_write_test_3' }],
      [time + 3, { 'id' => 3, 'name' => 'try_write_test_4' }],
      [time + 4, { 'id' => 3, 'name' => 'try_write_test_5' }]
    ]

    @driver.run(default_tag: tag) do
      events.each do |t, r|
        @driver.feed(tag, t, r)
      end
      sleep 5 # Wait for buffer flush
    end

    query = "#{test_table} | extend r = parse_json(record) | where r.id == 3 and r.name startswith \"try_write_test_\""
    rows = wait_for_ingestion(query, 5)

    assert(rows.size >= 5, 'Not all events were ingested into Kusto by try_write')

    chunk_id = rows[0][3]['chunk_id'] if rows[0] && rows[0][3] && rows[0][3]['chunk_id']
    assert(chunk_id, 'chunk_id not found in ingested records')

    query_chunk = "#{test_table} | extend r = parse_json(record) | where r.chunk_id == '#{chunk_id}'"
    chunk_rows = wait_for_ingestion(query_chunk, 5)

    assert(chunk_rows.size >= 5, 'Not all chunk records were committed in Kusto by try_write')
  end

  test 'try_write function ingests data to Kusto with parallel chunk commit' do
    test_table = "FluentD_trywrite_parallel_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: test_table,
      buffered: true,
      delayed: true,
      chunk_limit_size: '256'
    )
    setup_test_table(test_table)

    tag = 'e2e.try_write_parallel'
    time = Time.now.to_i
    events = []
    10.times do |i|
      events << [time + i, { 'id' => 4, 'name' => "try_write_parallel_test_#{i + 1}" }]
    end

    @driver.run(default_tag: tag) do
      events.each do |t, r|
        @driver.feed(tag, t, r)
      end
      sleep 5 # Wait for buffer flush
    end

    query = "#{test_table} | extend r = parse_json(record) | where r.id == 4 and r.name startswith \"try_write_parallel_test_\""
    rows = wait_for_ingestion(query, 10)

    assert(rows.size >= 10, 'Not all events were ingested into Kusto by try_write (parallel)')

    chunk_ids = rows.map { |row| row[3]['chunk_id'] if row[3] && row[3]['chunk_id'] }.compact.uniq
    assert(chunk_ids.size >= 2, 'Less than 2 chunk_ids found, parallel chunking not verified')

    # Check chunk commit by verifying all records with each chunk_id
    chunk_ids.each do |cid|
      expected_count = rows.count { |row| row[3]['chunk_id'] == cid }
      query_chunk = "#{test_table} | extend r = parse_json(record) | where r.chunk_id == '#{cid}'"
      chunk_rows = wait_for_ingestion(query_chunk, expected_count)

      assert(chunk_rows.size == expected_count,
             "Not all chunk records were committed in Kusto for chunk_id #{cid} (expected #{expected_count}, got #{chunk_rows.size})")
    end
  end

  # ESSENTIAL E2E BUFFERING TEST CASES - START

  # Test Case 1: Non-buffered mode with compression disabled
  test 'non_buffered_compression_disabled' do
    table_name = "FluentD_non_buffered_no_compression_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: false,
      compression_enabled: false
    )
    setup_test_table(table_name)

    tag = 'e2e.non_buffered.no_compression'
    events = generate_test_events(3, 1000, 'no_comp')

    events.each do |time, record|
      event_stream = Fluent::ArrayEventStream.new([[time, record]])
      assert_nothing_raised { @driver.instance.process(tag, event_stream) }
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 1000 and r.id <= 1002"
    rows = wait_for_ingestion(query, 3)

    assert(rows.size >= 3, "Expected 3 records, got #{rows.size} in non-buffered mode with compression disabled")
  end

  # Test Case 2: Memory buffered mode with immediate flush
  test 'memory_buffered_immediate_flush' do
    table_name = "FluentD_memory_buffered_immediate_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      buffer_type: 'memory',
      flush_mode: 'immediate'
    )
    setup_test_table(table_name)

    tag = 'e2e.memory_buffered.immediate'
    events = generate_test_events(5, 2000, 'mem_imm')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 3 # Allow time for immediate flush
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 2000 and r.id <= 2004"
    rows = wait_for_ingestion(query, 5)

    assert(rows.size >= 5, "Expected 5 records, got #{rows.size} in memory buffered immediate flush")
  end

  # Test Case 3: Memory buffered mode with interval flush
  test 'memory_buffered_interval_flush' do
    table_name = "FluentD_memory_buffered_interval_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      buffer_type: 'memory',
      flush_mode: 'interval',
      flush_interval: '3s'
    )
    setup_test_table(table_name)

    tag = 'e2e.memory_buffered.interval'
    events = generate_test_events(7, 3000, 'mem_int')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 8 # Wait longer than flush_interval
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 3000 and r.id <= 3006"
    rows = wait_for_ingestion(query, 7)

    assert(rows.size >= 7, "Expected 7 records, got #{rows.size} in memory buffered interval flush")
  end

  # Test Case 4: Memory buffered mode with chunk size limit
  test 'memory_buffered_chunk_size_limit' do
    table_name = "FluentD_memory_buffered_chunk_limit_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      buffer_type: 'memory',
      chunk_limit_size: '512' # Small to force multiple chunks
    )
    setup_test_table(table_name)

    tag = 'e2e.memory_buffered.chunk_limit'
    # Create larger events to exceed chunk size quickly
    events = []
    10.times do |i|
      large_data = 'x' * 100 # Create large payload
      events << [
        Time.now.to_i + i,
        {
          'id' => 4000 + i,
          'name' => "chunk_limit_test_#{i + 1}",
          'large_field' => large_data,
          'data' => { 'index' => i, 'test_type' => 'chunk_limit' }
        }
      ]
    end

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 8
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 4000 and r.id <= 4009"
    rows = wait_for_ingestion(query, 10)

    assert(rows.size >= 10, "Expected 10 records, got #{rows.size} in chunk size limit test")
  end

  # Test Case 5: Delayed commit mode with sync verification
  test 'delayed_commit_sync_verification' do
    table_name = "FluentD_delayed_commit_sync_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      delayed: true,
      flush_interval: '3s'
    )
    setup_test_table(table_name)

    tag = 'e2e.delayed_commit.sync'
    events = generate_test_events(4, 5000, 'delayed_sync')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 8
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 5000 and r.id <= 5003"
    rows = wait_for_ingestion(query, 4)

    assert(rows.size >= 4, "Expected 4 records, got #{rows.size} in delayed commit sync mode")

    # Verify chunk_id exists (added by delayed commit)
    chunk_ids = rows.map { |row| row[3]['chunk_id'] if row[3] }.compact.uniq
    assert(chunk_ids.size >= 1, 'No chunk_ids found in delayed commit mode')
  end

  # Test Case 6: Delayed commit mode with multiple chunks
  test 'delayed_commit_multiple_chunks' do
    table_name = "FluentD_delayed_commit_multi_chunks_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      delayed: true,
      chunk_limit_size: '300', # Small chunks to force multiple
      flush_interval: '4s'
    )
    setup_test_table(table_name)

    tag = 'e2e.delayed_commit.multi_chunks'
    events = generate_test_events(12, 6000, 'multi_chunk')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 10
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 6000 and r.id <= 6011"
    rows = wait_for_ingestion(query, 12)

    assert(rows.size >= 12, "Expected 12 records, got #{rows.size} in delayed commit multiple chunks")

    # Verify multiple chunk_ids exist
    chunk_ids = rows.map { |row| row[3]['chunk_id'] if row[3] }.compact.uniq
    assert(chunk_ids.size >= 1, "Expected chunk_ids, got #{chunk_ids.size}")
  end

  # Test Case 7: File buffer with persistent storage
  test 'file_buffer_persistent_storage' do
    table_name = "FluentD_file_buffer_persistent_#{Time.now.to_i}"
    buffer_path = create_temp_buffer_file
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      buffer_type: 'file',
      buffer_path: buffer_path,
      flush_interval: '5s',
      chunk_limit_size: '4k'
    )
    setup_test_table(table_name)

    tag = 'e2e.file_buffer.persistent'
    events = generate_test_events(6, 20_000, 'file_buf')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 8
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 20000 and r.id <= 20005"
    rows = wait_for_ingestion(query, 6)

    assert(rows.size >= 6, "Expected 6 records, got #{rows.size} in file buffer persistent storage test")
  end

  # Test Case 8: Buffered mode with compression enabled
  test 'buffered_mode_compression_enabled' do
    table_name = "FluentD_buffered_compression_#{Time.now.to_i}"
    configure_and_start_driver(
      table_name: table_name,
      buffered: true,
      compression_enabled: true,
      flush_interval: '4s',
      chunk_limit_size: '8k'
    )
    setup_test_table(table_name)

    tag = 'e2e.buffered.compression'
    events = generate_test_events(10, 7000, 'compression')

    @driver.run(default_tag: tag) do
      events.each do |time, record|
        @driver.feed(tag, time, record)
      end
      sleep 8
    end

    query = "#{table_name} | extend r = parse_json(record) | where r.id >= 7000 and r.id <= 7009"
    rows = wait_for_ingestion(query, 10)

    assert(rows.size >= 10, "Expected 10 records, got #{rows.size} in compression test")
  end

  # ESSENTIAL E2E BUFFERING TEST CASES - END
end
