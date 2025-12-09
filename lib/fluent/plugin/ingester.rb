# frozen_string_literal: true

# Ingester handles uploading data to Azure Blob Storage and sending ingestion messages to Azure Queue for Kusto ingestion.
#
# Responsibilities:
# - Upload data to blob storage
# - Prepare and send ingestion messages to queue
# - Handle errors during upload and ingestion

require 'uri'
require 'json'
require 'securerandom'
require 'base64'
require_relative 'client'
require_relative 'kusto_error_handler'
require 'logger'
require 'net/http'
class Ingester
  # Use a class instance variable instead of a class variable for client cache
  @client_cache = nil
  class << self
    attr_accessor :client_cache
  end

  def initialize(outconfiguration)
    # Initialize Ingester with configuration and resources
    @client = self.class.client(outconfiguration)
    @logger = begin
      outconfiguration.logger
    rescue StandardError
      Logger.new($stdout)
    end
  end

  def self.client(outconfiguration)
    # Thread-safe singleton client cache with basic validation
    return self.client_cache if self.client_cache

    # Double-checked locking pattern for thread safety
    @client_mutex ||= Mutex.new
    @client_mutex.synchronize do
      self.client_cache ||= Client.new(outconfiguration)
    end
  end

  # CRITICAL FIX: Dynamic resource access instead of stale cached reference
  def resources
    @client.resources
  end

  def build_uri(container_sas_uri, name)
    # Build a blob URI with SAS token
    base_uri, sas_token = container_sas_uri.split('?', 2)
    base_uri = base_uri.chomp('/')
    "#{base_uri}/#{name}?#{sas_token}"
  end

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  def upload_to_blob(blob_uri, raw_data, blob_name)
    # Upload raw data to Azure Blob Storage
    uri_str = build_uri(blob_uri, blob_name)
    uri = URI.parse(uri_str)
    blob_size = raw_data.bytesize
    request = Net::HTTP::Put.new(uri)
    request.body = raw_data
    request['x-ms-blob-type'] = 'BlockBlob'
    request['Content-Length'] = blob_size.to_s

    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https',
                               open_timeout: 10, read_timeout: 30, write_timeout: 10) do |http|
      http.request(request)
    end

    unless response.code.to_i.between?(200, 299)
      begin
        error_handler = KustoErrorHandler.new(response.body)
        if error_handler.permanent_error?
          @logger.error("Permanent error while uploading blob: #{error_handler.message}.Blob name: #{blob_name}")
        end
      rescue StandardError => e
        @logger.error("Failed to parse error response with KustoErrorHandler: #{e.message}. Blob name: #{blob_name}")
      end
      raise "Blob upload failed: #{response.code} #{response.message} - #{response.body}"
    end

    [uri.to_s, blob_size]
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

  # rubocop:disable Metrics/MethodLength
  def prepare_ingestion_message2(db, table, data_uri, blob_size_bytes, identity_token, compression_enabled = true, mapping_reference = nil)
    # Prepare the ingestion message for Azure Queue
    additional_props = {
      'authorizationContext' => identity_token,
      'format' => 'multijson'
    }
    additional_props['CompressionType'] = 'gzip' if compression_enabled
    additional_props['ingestionMappingReference'] = mapping_reference if mapping_reference && !mapping_reference.empty?
    
    {
      'Id' => SecureRandom.uuid,
      'BlobPath' => data_uri,
      'RawDataSize' => blob_size_bytes,
      'DatabaseName' => db,
      'TableName' => table,
      'RetainBlobOnSuccess' => true,
      'FlushImmediately' => true,
      'ReportLevel' => 2,  # Report both failures and successes
      'ReportMethod' => 0, # Use Azure Queue for reporting
      'AdditionalProperties' => additional_props
    }.to_json
  end
  # rubocop:enable Metrics/MethodLength

  # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
  def post_message_to_queue_http(queue_uri_with_sas, message)
    # Post the ingestion message to Azure Queue
    base_uri, sas_token = queue_uri_with_sas.split('?', 2)
    base_uri = base_uri.chomp('/')
    post_uri = URI("#{base_uri}/messages?#{sas_token}")
    encoded_message = Base64.strict_encode64(message)
    request = Net::HTTP::Post.new(post_uri)
    request['Content-Type'] = 'application/xml'
    request.body = "<QueueMessage><MessageText>#{encoded_message}</MessageText></QueueMessage>"
    response = Net::HTTP.start(post_uri.hostname, post_uri.port, use_ssl: post_uri.scheme == 'https',
                               open_timeout: 10, read_timeout: 30, write_timeout: 10) do |http|
      http.request(request)
    end
    {
      code: response.code,
      message: response.message,
      body: response.body
    }
  end
  # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

  def upload_data_to_blob_and_queue(raw_data, blob_name, db, table_name, compression_enabled = true, mapping_reference = nil)
    # Upload data to blob and send ingestion message to queue
    # Use dynamic resources method instead of stale cached reference
    current_resources = resources
    blob_uri, blob_size_bytes = upload_to_blob(current_resources[:blob_sas_uri], raw_data, blob_name)
    message = prepare_ingestion_message2(db, table_name, blob_uri, blob_size_bytes, current_resources[:identity_token],
                                         compression_enabled, mapping_reference)
    post_message_to_queue_http(current_resources[:queue_sas_uri], message)
    { blob_uri: blob_uri, blob_size_bytes: blob_size_bytes }
  end

  def token_provider
    # Return the token provider from the client
    @client.token_provider
  end
end
