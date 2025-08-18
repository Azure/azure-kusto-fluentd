# frozen_string_literal: true

# KustoErrorHandler parses and classifies errors returned from Azure Data Explorer (Kusto) API responses.
# Provides methods for error extraction, classification, and logging.
class KustoErrorHandler
  attr_reader :message

  def initialize(error_response)
    # Parse error response and extract message
    @error = parse_error(error_response)
    @message = @error['Message'] || @error['message'] || error_response
  end

  def permanent_error?
    # Check if error is marked as permanent
    @error && [true, 'true'].include?(@error['@permanent'])
  end

  # rubocop:disable Metrics/MethodLength
  # Extracts the Kusto error type (code) from an exception or error response
  def self.extract_kusto_error_type(error)
    error_json = parse_error_json(error)
    return error_json['error']['code'] if error_json && error_json['error'] && error_json['error']['code']

    nil
  end

  def self.parse_error_json(error)
    # Parse error JSON from string or exception
    if error.is_a?(String)
      begin
        JSON.parse(error)
      rescue StandardError
        nil
      end
    elsif error.respond_to?(:message)
      begin
        JSON.parse(error.message)
      rescue StandardError
        nil
      end
    end
  end
  # rubocop:enable Metrics/MethodLength

  # Factory method to create a KustoErrorHandler from error type and message
  def self.from_kusto_error_type(error_type, message)
    # You can expand this logic to map error_type to more structured fields if needed
    error_response = { 'error' => { 'code' => error_type, 'message' => message } }.to_json
    new(error_response)
  end

  # Handles Kusto errors and logs them appropriately
  def self.handle_kusto_error(logger, e, unique_id)
    kusto_error_type = extract_kusto_error_type(e)
    if kusto_error_type
      kusto_error = from_kusto_error_type(kusto_error_type, e.message)
      log_kusto_data_error(logger, kusto_error)
      log_kusto_drop_chunk(logger, kusto_error, unique_id) if kusto_error.is_permanent?
      # Always raise the custom error if present
      raise kusto_error if kusto_error.is_a?(StandardError)
      raise kusto_error unless kusto_error.is_permanent?

      nil
    else
      log_failed_ingest(logger, unique_id, e)
      raise
    end
  end

  # Handles errors during try_write and logs them appropriately
  def self.handle_try_write_error(logger, e, chunk_id)
    kusto_error_type = extract_kusto_error_type(e)
    if kusto_error_type
      kusto_error = from_kusto_error_type(kusto_error_type, e.message)
      log_kusto_data_error(logger, kusto_error)
      if kusto_error.is_permanent?
        log_kusto_drop_chunk(logger, kusto_error, chunk_id)
        return nil
      end
      # Always raise the custom error if present
      raise kusto_error if kusto_error.is_a?(StandardError)
      raise kusto_error unless kusto_error.is_permanent?

      nil
    else
      logger.error(
        "Failed to ingest chunk #{chunk_id}: #{e.full_message}"
      )
      raise
    end
  end

  # Log details of a Kusto data error
  def self.log_kusto_data_error(logger, kusto_error)
    logger.error(
      "KustoDataError: #{kusto_error.message} " \
      "(Code: #{kusto_error.failure_code}, Reason: #{kusto_error.failure_sub_code}, " \
      "Permanent: #{kusto_error.is_permanent?})"
    )
  end

  # Log when a chunk is dropped due to a permanent error
  def self.log_kusto_drop_chunk(logger, kusto_error, chunk_id)
    logger.error(
      "Dropping chunk #{chunk_id} due to permanent Kusto error: #{kusto_error.message}"
    )
  end

  # Log failed ingestion event
  def self.log_failed_ingest(logger, unique_id, e)
    logger.error(
      "Failed to ingest event to Kusto : #{unique_id}\n" \
      "#{e.full_message}"
    )
  end

  private

  def parse_error(error_response)
    # Parse error response JSON
    JSON.parse(error_response)
  rescue StandardError
    {}
  end
end
