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
require_relative 'auth/azcli_tokenprovider'
require_relative 'auth/wif_tokenprovider'
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
    
    # Minimal state tracking for 12-hour reset
    @client_state = {
      creation_time: Time.now,
      resource_fetch_count: 0,
      last_successful_fetch: nil
    }
    
    # Simplified health configuration
    @health_config = {
      max_client_age: 43_200, # 12 hours - force reset after this time
      max_fetch_cycles: 200 # Force reset after too many fetch cycles
    }
  end

  def resources
    # Return cached resources if valid, otherwise fetch and cache
    return @cached_resources if resources_cached?

    fetch_and_cache_resources
    @cached_resources
  end

  # Minimal health status for operational visibility
  def health_status
    {
      resources_cached: !@cached_resources.nil?,
      cache_expires_at: @resources_expiry_time,
      fetch_cycles: @client_state[:resource_fetch_count],
      pod_age_hours: (Time.now - @client_state[:creation_time]) / 3600,
      last_successful_fetch: @client_state[:last_successful_fetch]
    }
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
    # Check for long-running pod health issues first
    if long_running_pod_health_check_needed?
      @logger.warn("Long-running pod health issue detected, forcing resource refresh")
      return false
    end
    
    # Check if resources are cached and not expired
    @cached_resources && @resources_expiry_time && @resources_expiry_time > Time.now
  end

  def long_running_pod_health_check_needed?
    current_time = Time.now
    
    # Check if client is too old (12+ hours) - force reset to prevent staleness
    if @client_state[:creation_time] && 
       (current_time - @client_state[:creation_time]) > @health_config[:max_client_age]
      @logger.warn("Client is #{(current_time - @client_state[:creation_time]) / 3600} hours old, forcing reset")
      reset_client_state_for_long_running_pod
      return true
    end
    
    # Check if too many fetch cycles (potential state corruption)
    if @client_state[:resource_fetch_count] > @health_config[:max_fetch_cycles]
      @logger.warn("Client has #{@client_state[:resource_fetch_count]} fetch cycles, resetting state")
      reset_client_state_for_long_running_pod
      return true
    end
    
    # Check if no successful fetch for too long (6 hours)
    if @client_state[:last_successful_fetch] &&
       (current_time - @client_state[:last_successful_fetch]) > 21_600
      @logger.warn("No successful resource fetch for #{(current_time - @client_state[:last_successful_fetch]) / 3600} hours")
      return true
    end
    
    false
  end

  def reset_client_state_for_long_running_pod
    @logger.info("Resetting client state for long-running pod health")
    
    @cached_resources = nil
    @resources_expiry_time = nil
    @client_state[:creation_time] = Time.now
    @client_state[:resource_fetch_count] = 0
    @client_state[:last_successful_fetch] = nil
    @client_state[:consecutive_failures] = 0
    
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
    when 'azcli'
      AzCliTokenProvider.new(outconfiguration)
    when 'workload_identity'
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
    
    # Add jitter (±30 minutes) to prevent thundering herd
    base_ttl = 21_600 # 6 hours
    jitter = rand(-1800..1800) # ±30 minutes
    @resources_expiry_time = Time.now + base_ttl + jitter
    
    # Update client state tracking
    @client_state[:resource_fetch_count] += 1
    @client_state[:last_successful_fetch] = Time.now
    @client_state[:consecutive_failures] = 0
    
    @logger.info("Resources cached with jitter: #{jitter / 60} minutes (expires at #{@resources_expiry_time}) - fetch cycle #{@client_state[:resource_fetch_count]}")
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
