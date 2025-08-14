# frozen_string_literal: true

# Client handles authentication and resource fetching for Azure Data Explorer (Kusto) ingestion.
# It supports both Managed Identity and AAD Client Credentials authentication methods.
#
# The Client class is responsible for:
# - Authenticating using Managed Identity or AAD Client Credentials
# - Fetching and caching Kusto ingestion resources
# - Providing access to blob SAS URI, queue SAS URI, and identity token
require_relative 'auth/aad_tokenprovider'
require_relative 'auth/mi_tokenprovider'
require_relative 'kusto_query'
require 'logger'
class Client
  def initialize(outconfiguration)
    # Set up queries for resource fetching
    @user_query_blob_container = '.get ingestion resources'
    @user_query_aad_token = '.get kusto identity token'
    # Use provided logger or default to stdout
    @logger = initialize_logger(outconfiguration)
    @data_endpoint = outconfiguration.kusto_endpoint
    @cached_resources = nil
    @resources_expiry_time = nil
    @outconfiguration = outconfiguration
    @token_provider = create_token_provider(outconfiguration)
  end

  def resources
    # Return cached resources if valid, otherwise fetch and cache
    return @cached_resources if resources_cached?
    fetch_and_cache_resources
    @cached_resources
  end

  attr_reader :blob_sas_uri, :queue_sas_uri, :identity_token, :logger, :blob_rows, :data_endpoint, :token_provider

  private

  def initialize_logger(outconfiguration)
    # Prefer logger from configuration, fallback to stdout
    outconfiguration.logger
  rescue StandardError
    Logger.new($stdout)
  end

  def resources_cached?
    # Check if resources are cached and not expired
    @cached_resources && @resources_expiry_time && @resources_expiry_time > Time.now
  end

  def fetch_and_cache_resources
    # Fetch resources from Kusto and cache them
    @logger.info('Fetching resources from Kusto...')
    blob_rows, aad_token_rows = fetch_kusto_resources
    return unless blob_rows && aad_token_rows
    blob_sas_uri, queue_sas_uri, identity_token = extract_resource_uris(blob_rows, aad_token_rows)
    return unless validate_resource_uris(blob_sas_uri, queue_sas_uri, identity_token)
    assign_and_cache_resources(blob_sas_uri, queue_sas_uri, identity_token)
  end

  
  def fetch_kusto_resources
    # Fetch resource rows and validate them
    blob_rows, aad_token_rows = fetch_kusto_rows_with_error_handling
    validate_kusto_resource_rows(blob_rows, aad_token_rows)
  end

  def fetch_kusto_rows_with_error_handling
    # Fetch blob and AAD token rows with error handling
    blob_rows = fetch_blob_rows
    aad_token_rows = fetch_aad_token_rows
    [blob_rows, aad_token_rows]
  end

  def fetch_blob_rows
    # Run Kusto query for blob resources
    run_kusto_api_query(@user_query_blob_container, @data_endpoint, @token_provider,
                        use_ingest_endpoint: true)
  rescue StandardError => e
    @logger.error("Failed to fetch blob resources from Kusto: #{e.message}")
    nil
  end

  def fetch_aad_token_rows
    # Run Kusto query for AAD token resources
    run_kusto_api_query(@user_query_aad_token, @data_endpoint, @token_provider,
                        use_ingest_endpoint: true, database_name: nil)
  rescue StandardError => e
    @logger.error("Failed to fetch AAD token resources from Kusto: #{e.message}")
    nil
  end

  def create_token_provider(outconfiguration)
    case outconfiguration.auth_type&.downcase
    when 'aad'
      AadTokenProvider.new(outconfiguration)
    when 'workload_identity'
      require_relative 'auth/wif_tokenprovider'
      WorkloadIdentity.new(outconfiguration)
    when 'user_managed_identity', 'system_managed_identity'
      ManagedIdentityTokenProvider.new(outconfiguration)
    else
      raise "Unknown auth_type: #{outconfiguration.auth_type}"
    end
  end

  def extract_resource_uris(blob_rows, aad_token_rows)
    # Extract URIs from resource rows
    blob_sas_uri = blob_rows.find { |row| row[0] == 'TempStorage' }&.[](1)
    queue_sas_uri = blob_rows.find { |row| row[0] == 'SecuredReadyForAggregationQueue' }&.[](1)
    identity_token = aad_token_rows[0][0] if aad_token_rows.any?
    [blob_sas_uri, queue_sas_uri, identity_token]
  end

  def validate_resource_uris(blob_sas_uri, queue_sas_uri, identity_token)
    # Ensure all required URIs are present
    if blob_sas_uri.nil? || queue_sas_uri.nil? || identity_token.nil?
      @logger.error('Failed to retrieve all required resources: blob_sas_uri, queue_s_uri, or identity_token is nil.')
      return false
    end
    true
  end

  def assign_and_cache_resources(blob_sas_uri, queue_sas_uri, identity_token)
    # Assign and cache resource URIs
    @blob_sas_uri = blob_sas_uri
    @queue_sas_uri = queue_sas_uri
    @identity_token = identity_token
    @cached_resources = {
      blob_sas_uri: blob_sas_uri,
      queue_sas_uri: queue_sas_uri,
      identity_token: identity_token
    }
    @resources_expiry_time = Time.now + 21_600 # Cache for 6 hours
  end

  def validate_kusto_resource_rows(blob_rows, aad_token_rows)
    # Validate resource rows are present
    if blob_rows.nil? || blob_rows.empty?
      @logger.error('No blob rows found in the response.')
      return [nil, nil]
    end
    if aad_token_rows.nil? || aad_token_rows.empty?
      @logger.error('No AAD token rows found in the response.')
      return [nil, nil]
    end
    [blob_rows, aad_token_rows]
  end
end
