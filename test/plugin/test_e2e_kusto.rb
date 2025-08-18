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
require 'ostruct'

class KustoE2ETest < Test::Unit::TestCase
  include Fluent::Test::Helpers

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
      @client.token_provider.aad_token_bearer
    end
    ingester.access_token
  end

  def kusto_mgmt_query(query)
    # Send management commands to /v1/rest/mgmt on the data endpoint
    endpoint = @engine_url
    path = '/v1/rest/mgmt'
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
      puts "Kusto query failed with status #{response.code}:"
      puts response.body
      return []
    end
    begin
      response_json = JSON.parse(response.body)
      tables = response_json['Tables']
      rows = tables && tables[0] && tables[0]['Rows']
      rows || []
    rescue JSON::ParserError => e
      puts "Failed to parse JSON: #{e}"
      puts response.body
      []
    end
  end

  def kusto_data_query(query)
    # Send data queries to /v1/rest/query on the data endpoint
    endpoint = @engine_url
    path = '/v1/rest/query'
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
      puts "Kusto query failed with status #{response.code}:"
      puts response.body
      return []
    end
    begin
      response_json = JSON.parse(response.body)
      tables = response_json['Tables']
      rows = tables && tables[0] && tables[0]['Rows']
      rows || []
    rescue JSON::ParserError => e
      puts "Failed to parse JSON: #{e}"
      puts response.body
      []
    end
  end

  def setup
    Fluent::Test.setup
    @engine_url = ENV['CLUSTER'] || 'https://example.kusto.windows.net'
    @database = ENV['DB'] || 'testdb'
    @table = "FluentD_#{Time.now.to_i}"
    @columns = "(tag:string, timestamp:datetime, record:string)"
    @client_id = ENV['CLIENT_ID'] || ''
    @client_secret = ENV['CLIENT_SECRET'] || ''
    @tenant_id = ENV['TENANT_ID'] || ''
    @managed_identity_client_id = ENV['MANAGED_IDENTITY_CLIENT_ID'] || ''
    @auth_type = (ENV['AUTH_TYPE'] || 'aad').downcase

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
    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered false
      delayed false
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{@table}
      #{@auth_lines}
    CONF
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, Logger.new($stdout))
    @driver.instance.start
    drop_resp = kusto_mgmt_query(".drop table #{@table} ifexists")
    create_resp = kusto_mgmt_query(".create table #{@table} #{@columns}")
  end

  def teardown
    kusto_mgmt_query(".drop table #{@table} ifexists")
  end

  def create_test_table(table_name)
    kusto_mgmt_query(".drop table #{table_name} ifexists")
    create_resp = kusto_mgmt_query(".create table #{table_name} #{@columns}")
  end

  # Before running this test, ensure your service principal has TableAdmin and Ingestor permissions on the test database.
  # Example Kusto command:
  # .add database ['Fluentd-test-db'] TableAdmin ("aadapp=#{ENV['CLIENT_ID']};#{ENV['TENANT_ID']}")
  # .add database ['Fluentd-test-db'] Ingestor ("aadapp=#{ENV['CLIENT_ID']};#{ENV['TENANT_ID']}")
  # Update your .env and test config to use Fluentd-test-db as the database.

  test 'process function ingestion to Kusto' do
    @table = "FluentD_process_#{Time.now.to_i}"
    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered false
      delayed false
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{@table}
      #{@auth_lines}
    CONF
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, Logger.new($stdout))
    @driver.instance.start
    create_test_table(@table)
    tag = 'e2e.test'
    time = Time.now.to_i
    record = { 'id' => 1, 'name' => 'test' }
    event_stream = Fluent::ArrayEventStream.new([[time, record]])
    # puts "Calling process"
    assert_nothing_raised { @driver.instance.process(tag, event_stream) }
    sleep 30 # Wait for ingestion
    # puts "All data in table: #{kusto_data_query(@table).inspect}"
    query = "#{@table} | extend r = parse_json(record) | where r.id == 1 and r.name == \"test\""
    rows = kusto_data_query(query)
    assert(!rows.empty?, 'Data was not ingested into Kusto')
    # puts "Query result rows: #{rows.inspect}"
    found = false
    rows.each do |row|
      r = row[3] rescue nil
      if r && r['id'] == 1 && r['name'] == 'test'
        found = true
        break
      end
    end
    assert(found, 'Expected record with name == test not found in Kusto')
    # puts "Plugin logs (if any):"
    begin
      log_path = File.join(File.dirname(__FILE__), '../../lib/fluent/plugin/plugin_*.log')
      Dir.glob(log_path).each do |logfile|
        # puts "--- #{logfile} ---"
        # puts File.read(logfile)
      end
    rescue => e
      puts "Error reading plugin logs: #{e}"
    end
  end

  test 'write function ingests data to Kusto' do
    @table = "FluentD_write_#{Time.now.to_i}"
    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered true
      delayed false
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{@table}
      #{@auth_lines}
      <buffer>
        @type memory
        flush_interval 1s
        chunk_limit_size 8k
      </buffer>
    CONF
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, Logger.new($stdout))
    @driver.instance.start
    create_test_table(@table)
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
    sleep 10 # Wait for ingestion
    query = "#{@table} | extend r = parse_json(record) | where r.id == 2 and r.name startswith \"write_test_\""
    max_wait = 240
    interval = 5
    waited = 0
    rows = []
    while waited < max_wait
      rows = kusto_data_query(query)
      break if rows.size >= 5
      sleep interval
      waited += interval
    end
    assert(rows.size >= 5, 'Not all events were ingested into Kusto by write')
    # puts "Write Query result rows: #{rows.inspect}"
  end

  test 'try_write function ingests data to Kusto' do
    @table = "FluentD_trywrite_#{Time.now.to_i}"
    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered true
      delayed true
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{@table}
      #{@auth_lines}
      <buffer>
        @type memory
        flush_interval 1s
        chunk_limit_size 8k
      </buffer>
    CONF
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, Logger.new($stdout))
    @driver.instance.start
    create_test_table(@table)
    tag = 'e2e.try_write'
    time = Time.now.to_i
    events = [
      [time, { 'id' => 3, 'name' => 'try_write_test_1' }],
      [time + 1, { 'id' => 3, 'name' => 'try_write_test_2' }],
      [time + 2, { 'id' => 3, 'name' => 'try_write_test_3' }],
      [time + 3, { 'id' => 3, 'name' => 'try_write_test_4' }],
      [time + 4, { 'id' => 3, 'name' => 'try_write_test_5' }]
    ]
    chunk_id = nil
    @driver.run(default_tag: tag) do
      events.each do |t, r|
        @driver.feed(tag, t, r)
      end
      sleep 5 # Wait for buffer flush
    end
    # Wait and retry for ingestion, extract chunk_id from Kusto records
    query = "#{@table} | extend r = parse_json(record) | where r.id == 3 and r.name startswith \"try_write_test_\""
    max_wait = 240
    interval = 5
    waited = 0
    rows = []
    while waited < max_wait
      rows = kusto_data_query(query)
      break if rows.size >= 5
      sleep interval
      waited += interval
    end
    assert(rows.size >= 5, 'Not all events were ingested into Kusto by try_write')
    chunk_id = rows[0][3]['chunk_id'] if rows[0] && rows[0][3] && rows[0][3]['chunk_id']
    # puts "TryWrite Query result rows: #{rows.inspect}"
    assert(chunk_id, 'chunk_id not found in ingested records')
    # Now check chunk commit by verifying all records with chunk_id
    query_chunk = "#{@table} | extend r = parse_json(record) | where r.chunk_id == '#{chunk_id}'"
    waited = 0
    chunk_rows = []
    while waited < max_wait
      chunk_rows = kusto_data_query(query_chunk)
      break if chunk_rows.size >= 5
      sleep interval
      waited += interval
    end
    assert(chunk_rows.size >= 5, 'Not all chunk records were committed in Kusto by try_write')
    # puts "TryWrite chunk_id records: #{chunk_rows.inspect}"
  end

  test 'try_write function ingests data to Kusto with parallel chunk commit' do
    @table = "FluentD_trywrite_parallel_#{Time.now.to_i}"
    @conf = <<-CONF
      @type kusto
      @log_level debug
      buffered true
      delayed true
      endpoint #{@engine_url}
      database_name #{@database}
      table_name #{@table}
      #{@auth_lines}
      <buffer>
        @type memory
        flush_interval 1s
        chunk_limit_size 256
      </buffer>
    CONF
    @driver = Fluent::Test::Driver::Output.new(Fluent::Plugin::KustoOutput).configure(@conf)
    @driver.instance.instance_variable_set(:@logger, Logger.new($stdout))
    @driver.instance.start
    create_test_table(@table)
    tag = 'e2e.try_write_parallel'
    time = Time.now.to_i
    events = []
    10.times do |i|
      events << [time + i, { 'id' => 4, 'name' => "try_write_parallel_test_#{i+1}" }]
    end
    @driver.run(default_tag: tag) do
      events.each do |t, r|
        @driver.feed(tag, t, r)
      end
      sleep 5 # Wait for buffer flush
    end
    # Wait and retry for ingestion, extract chunk_ids from Kusto records
    query = "#{@table} | extend r = parse_json(record) | where r.id == 4 and r.name startswith \"try_write_parallel_test_\""
    max_wait = 240
    interval = 5
    waited = 0
    rows = []
    while waited < max_wait
      rows = kusto_data_query(query)
      break if rows.size >= 10
      sleep interval
      waited += interval
    end
    assert(rows.size >= 10, 'Not all events were ingested into Kusto by try_write (parallel)')
    chunk_ids = rows.map { |row| row[3]['chunk_id'] if row[3] && row[3]['chunk_id'] }.compact.uniq
    # puts "TryWrite Parallel Query result rows: #{rows.inspect}"
    assert(chunk_ids.size >= 2, 'Less than 2 chunk_ids found, parallel chunking not verified')
    # Now check chunk commit by verifying all records with each chunk_id
    chunk_ids.each do |cid|
      expected_count = rows.count { |row| row[3]['chunk_id'] == cid }
      waited = 0
      chunk_rows = []
      query_chunk = "#{@table} | extend r = parse_json(record) | where r.chunk_id == '#{cid}'"
      while waited < max_wait
        chunk_rows = kusto_data_query(query_chunk)
        break if chunk_rows.size >= expected_count
        sleep interval
        waited += interval
      end
      assert(chunk_rows.size == expected_count, "Not all chunk records were committed in Kusto for chunk_id #{cid} (expected #{expected_count}, got #{chunk_rows.size})")
      # puts "TryWrite chunk_id #{cid} records: #{chunk_rows.inspect}"
    end
  end
end
