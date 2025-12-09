# frozen_string_literal: true

# Provides helper functions for interacting with Kusto ingestion endpoints and running Kusto API queries.
# Includes endpoint transformation and query execution logic.

require 'net/http'
require 'uri'
require 'json'
require 'securerandom'
require 'base64'
require_relative 'kusto_version'

def to_ingest_endpoint(data_endpoint)
  # Convert a Kusto data endpoint to its corresponding ingest endpoint
  data_endpoint.sub(%r{^https://}, 'https://ingest-')
end

# Runs a Kusto API query against the specified endpoint.
# Handles both management and query endpoints, builds request, and parses response.
def run_kusto_api_query(query, data_endpoint, token_provider, use_ingest_endpoint: false, database_name: nil)
  access_token = token_provider.get_token
  endpoint = use_ingest_endpoint ? to_ingest_endpoint(data_endpoint) : data_endpoint
  path = use_ingest_endpoint ? '/v1/rest/mgmt' : '/v1/rest/query'
  uri = URI("#{endpoint}#{path}")

  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  # Add timeouts to prevent hanging connections
  http.open_timeout = 10
  http.read_timeout = 30
  http.write_timeout = 10

  headers = {
    'Authorization' => "Bearer #{access_token}",
    'Content-Type' => 'application/json',
    'Accept' => 'application/json',
    'x-ms-client-version' => "Kusto.FluentD:#{Fluent::Plugin::Kusto::VERSION}",
    'x-ms-app' => 'Kusto.FluentD',
    'x-ms-user' => 'Kusto.FluentD'
  }

  body_hash = { csl: query }
  body_hash[:db] = database_name if database_name
  body = body_hash.to_json

  request = Net::HTTP::Post.new(uri.request_uri, headers)
  request.body = body

  response = http.request(request)
  unless response.code.to_i.between?(200, 299)
    # Print error details if query fails
    puts "Kusto query failed with status #{response.code}:"
    puts response.body
    begin
      error_handler = defined?(KustoErrorHandler) ? KustoErrorHandler.new(response.body) : nil
      puts "Permanent Kusto error: #{error_handler.message}" if error_handler&.permanent_error?
    rescue StandardError => e
      puts "Failed to parse error response with KustoErrorHandler: #{e.message}"
    end
    return response
  end

  begin
    # Parse and return rows from response JSON
    response_json = JSON.parse(response.body)
    tables = response_json['Tables']
    rows = tables && tables[0] && tables[0]['Rows']
    rows || []
  rescue JSON::ParserError => e
    puts "Failed to parse JSON: #{e}"
    puts response.body
    response
  end
end
