# frozen_string_literal: true

# KustoOutput is a Fluentd output plugin for ingesting logs into Azure Data Explorer (Kusto).
# Supports managed identity, AAD authentication, multi-worker, buffer, and delayed commit.
require 'fluent/plugin/output'
require_relative 'ingester'
require 'time'
require_relative 'kusto_error_handler'
require 'logger'
require 'json'
require 'stringio'
require 'zlib'
require 'set'

module Fluent
  module Plugin
    class KustoOutput < Output
      # Register plugin and define configuration parameters
      Fluent::Plugin.register_output('kusto', self)
      helpers :compat_parameters, :inject

      config_param :endpoint, :string, default: nil, secret: true
      config_param :database_name, :string, default: nil
      config_param :table_name, :string, default: nil
      config_param :tenant_id, :string, default: nil
      config_param :client_id, :string, default: nil
      config_param :client_secret, :string, default: nil, secret: true
      config_param :buffered, :bool, default: true
      config_param :delayed, :bool, default: false
      config_param :managed_identity_client_id, :string, default: nil, secret: true
      config_param :azure_cloud, :string, default: 'AzureCloud'
      config_param :compression_enabled, :bool, default: true
      config_param :logger_path, :string, default: nil
      config_param :auth_type, :string, default: 'aad', desc: 'Authentication type to use for Kusto. Options: "aad", "user_managed_identity", "system_managed_identity", "workload_identity".'
      config_param :workload_identity_client_id, :string, default: nil, secret: true, desc: 'Client ID for workload identity authentication.'
      config_param :workload_identity_tenant_id, :string, default: nil, secret: true, desc: 'Tenant ID for workload identity authentication.'
      config_param :workload_identity_token_file_path, :string, default: nil, secret: true, desc: 'File path for workload identity token.'

      config_section :buffer do
        config_set_default :chunk_keys, ['time']
        config_set_default :timekey, (60 * 60 * 24)
      end

      def multi_workers_ready?
        # Enable multi-worker support
        true
      end

      def configure(conf)
        # Configure plugin and validate parameters
        compat_parameters_convert(conf, :buffer)
        super
        validate_buffer_config(conf)
        validate_delayed_config
        validate_required_params
      end

      def start
        # Initialize output configuration and ingester
        super
        setup_outconfiguration
        setup_ingester_and_logger
        @table_name = @outconfiguration&.table_name
        @database_name = @outconfiguration&.database_name
        @shutdown_called = false
        @deferred_threads = []
        @plugin_start_time = Time.now
        @total_bytes_ingested = 0
      end

      def format(tag, time, record)
        # Format a record for ingestion
        tag_val = extract_tag(record, tag)
        timestamp = extract_timestamp(record, time)
        safe_record = sanitize_record_for_json(record)
        "#{format_record_json(tag_val, timestamp, safe_record)}\n"
      end

      def extract_tag(record, tag)
        # Extract tag from record or fallback to defaults
        return tag if !record.is_a?(Hash) || record.nil?
        return record['tag'] if record['tag']
        return tag if tag
        return record['host'] if record['host']
        return record['user'] if record['user']
        return ::Regexp.last_match(1) if record['message'] && record['message'] =~ /(\d{1,3}(?:\.\d{1,3}){3})/
        'default_tag'
      end

      def extract_timestamp(record, time)
        # Extract datetime from record or fallback to time
        timestamp = find_time_or_date_key(record)
        return timestamp if timestamp && !timestamp.to_s.empty?
        timestamp = (time ? Time.at(time).utc.iso8601 : '')
        return timestamp unless timestamp.to_s.empty?
        timestamp = find_timestamp_by_regex(record)
        timestamp ||= ''
        timestamp
      end

      def find_time_or_date_key(record)
        # Find time/date key in record
        return nil unless record.is_a?(Hash)
        record.each do |k, v|
          return v if k.to_s.downcase.include?('time') || k.to_s.downcase.include?('date')
        end
        nil
      end

      def find_timestamp_by_regex(record)
        # Find datetime by regex in record values
        record.each_value do |v|
          next unless v.is_a?(String)
          return ::Regexp.last_match(1) if v =~ %r{(\[\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4}\].*)}
        end
        nil
      end

      def format_record_json(tag_val, timestamp, safe_record)
        # Format record as JSON for ingestion
        record_value = safe_record.is_a?(Hash) ? safe_record.reject { |k, _| %w[tag time].include?(k) } : (safe_record || {})
        { 'tag' => tag_val, 'timestamp' => timestamp, 'record' => record_value }.to_json
      end

      def dump_unique_id_hex(unique_id)
        # Convert unique_id to hex string
        return 'noid' if unique_id.nil?
        return unique_id.unpack1('H*') if unique_id.respond_to?(:unpack1)
        unique_id.to_s
      end

      def compress_data(data)
        # Compress data using gzip
        sio = StringIO.new
        gz = Zlib::GzipWriter.new(sio)
        gz.write(data)
        gz.close
        sio.string
      end

      def process(tag, es)
        es.each do |time, record|
          formatted = format(tag, time, record).encode('UTF-8', invalid: :replace, undef: :replace, replace: '_')
          safe_tag = tag.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '_').gsub(/[^0-9A-Za-z.-]/, '_')
          blob_name = "fluentd_event_#{safe_tag}.json"
          @ingester.upload_data_to_blob_and_queue(formatted, blob_name, database_name, table_name, compression_enabled)
        rescue StandardError => e
          @logger&.error("Failed to ingest event to Kusto: #{e}\nEvent skipped: #{record.inspect}\n#{e.backtrace.join("\n")}")
          next
        end
      end

      def write(chunk)
        # Write a chunk of events to Kusto
        worker_id = Fluent::Engine.worker_id
        raw_data = chunk.read
        tag = extract_tag_from_metadata(chunk.metadata)
        safe_tag = tag.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '_').gsub(/[^0-9A-Za-z.-]/, '_')
        unique_id = chunk.unique_id
        ext = compression_enabled ? '.json.gz' : '.json'
        blob_name = "fluentd_event_worker#{worker_id}_#{safe_tag}_#{dump_unique_id_hex(unique_id)}#{ext}"
        data_to_upload = compression_enabled ? compress_data(raw_data) : raw_data
        begin
          @ingester.upload_data_to_blob_and_queue(data_to_upload, blob_name, @database_name, @table_name, compression_enabled)
        rescue StandardError => e
          handle_kusto_error(e, unique_id)
        end
      end

      def extract_tag_from_metadata(metadata)
        # Extract tag from chunk metadata
        return 'default_tag' if metadata.nil?
        return metadata.tag || 'default_tag' if metadata.respond_to?(:tag)
        'default_tag'
      end

      def handle_kusto_error(e, unique_id)
        # Handle and log Kusto errors
        KustoErrorHandler.handle_kusto_error(@logger, e, dump_unique_id_hex(unique_id))
      end

      def try_write(chunk)
        @deferred_threads ||= []
        tag = extract_tag_from_metadata(chunk.metadata)
        safe_tag = tag.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '_').gsub(/[^0-9A-Za-z.-]/, '_')
        chunk_id = dump_unique_id_hex(chunk.unique_id)
        ext = compression_enabled ? '.json.gz' : '.json'
        blob_name = "fluentd_event_#{safe_tag}_#{chunk_id}#{ext}"
        raw_data = chunk.read || ""
        records = raw_data.split("\n").map do |line|
          begin
            rec = JSON.parse(line)
            if rec.is_a?(Hash) && rec['record'].is_a?(Hash)
              rec['record']['chunk_id'] = chunk_id
            end
            rec.to_json
          rescue
            line
          end
        end
        updated_raw_data = records.join("\n")
        row_count = records.size
        data_to_upload = compression_enabled ? compress_data(updated_raw_data) : updated_raw_data
        begin
          @ingester.upload_data_to_blob_and_queue(data_to_upload, blob_name, database_name, table_name, compression_enabled)
          if @shutdown_called
            commit_write(chunk.unique_id)
            @logger.info("Immediate commit for chunk_id=#{chunk_id} due to shutdown") if @logger
          else
            thread = start_deferred_commit_thread(chunk_id, chunk, row_count)
            @deferred_threads << thread if thread
          end
        rescue StandardError => e
          KustoErrorHandler.handle_try_write_error(@logger, e, chunk_id)
        end
      end

      def start_deferred_commit_thread(chunk_id, chunk, row_count)
        # Start a thread to commit chunk after verifying ingestion
        return nil if @shutdown_called
        Thread.new do
          loop do
            sleep 1
            if check_data_on_server(chunk_id, row_count)
              commit_write(chunk.unique_id)
              break
            end
          end
        rescue StandardError => e
          @logger.error("Error in deferred commit thread: #{e}")
        end
      end

      def check_data_on_server(chunk_id, row_count)
        # Query Kusto to verify chunk ingestion
        begin
          query = "#{@table_name} | extend record_dynamic = parse_json(record) | where record_dynamic.chunk_id == '#{chunk_id}' | count"
          result = run_kusto_api_query(query, @endpoint, @ingester.token_provider, use_ingest_endpoint: false, database_name: @database_name)
          if result.is_a?(Array) && result[0].is_a?(Array)
            count_val = result[0][0].to_i
            return count_val == row_count
          else
            @logger.error("Kusto query failed or returned unexpected result: #{result.inspect}") if @logger && @logger.respond_to?(:error)
          end
        rescue => e
          @logger.error("Failed to get chunk_id count: #{e}") if @logger && @logger.respond_to?(:error)
        end
        false
      end

      def shutdown
        # Handle plugin shutdown and cleanup threads
        @shutdown_called = true
        if @deferred_threads
          @deferred_threads.each do |t|
            if t.alive?
              t.kill
              @logger.info("delayed commit for buffer chunks was cancelled in shutdown chunk_id=unknown") if @logger
            end
          end
        end
        @ingester.shutdown if @ingester && @ingester.respond_to?(:shutdown)
        super
      end

      private

      def validate_buffer_config(conf)
        # Validate buffer configuration
        return unless !@buffered && conf.elements('buffer').any?
        raise Fluent::ConfigError, 'Buffer section present but buffered is false'
      end

      def validate_delayed_config
        # Validate delayed commit configuration
        return unless !@buffered && @delayed
        raise Fluent::ConfigError, 'Delayed commit is only supported in buffered mode (buffered must be true if delayed is true)'
      end

      def validate_required_params
        # Ensure required parameters are present
        required_params = %w[endpoint database_name table_name]
        missing_params = required_params.select do |param|
          value = send(param)
          value.nil? || value.strip.empty?
        end
        return if missing_params.empty?
        raise Fluent::ConfigError, "Missing required parameters: #{missing_params.join(', ')}"
      end

      def prefer_buffered_processing
        @buffered
      end

      def prefer_delayed_commit
        @delayed
      end

      def setup_outconfiguration
        # Build OutputConfiguration for Kusto
        @outconfiguration = OutputConfiguration.new(
          client_app_id: client_id,
          client_app_secret: client_secret,
          tenant_id: tenant_id,
          kusto_endpoint: endpoint,
          database_name: database_name,
          table_name: table_name,
          azure_cloud: azure_cloud,
          managed_identity_client_id: managed_identity_client_id,
          logger_path: logger_path,
          auth_type: auth_type,
          workload_identity_client_id: workload_identity_client_id,
          workload_identity_tenant_id: workload_identity_tenant_id,
          workload_identity_token_file_path: workload_identity_token_file_path
        )
      end

      def setup_ingester_and_logger
        # Initialize Ingester and logger
        @ingester = Ingester.new(@outconfiguration)
        @logger = @outconfiguration.logger
      end

      def sanitize_record_for_json(obj, seen = Set.new)
        # Recursively sanitize record for JSON serialization
        return obj unless obj.is_a?(Hash) || obj.is_a?(Array)
        raise 'Circular reference detected in record' if seen.include?(obj.object_id)
        seen.add(obj.object_id)
        obj.is_a?(Hash) ? sanitize_hash(obj, seen) : sanitize_array(obj, seen)
      end

      def sanitize_hash(obj, seen)
        obj.each_with_object({}) do |(k, v), h|
          h[k.to_s] = sanitize_record_for_json(v, seen)
        end
      end

      def sanitize_array(obj, seen)
        obj.map { |v| sanitize_record_for_json(v, seen) }
      end
    end
  end
end
