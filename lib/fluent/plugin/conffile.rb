# frozen_string_literal: true

# OutputConfiguration holds and validates configuration for the Kusto output plugin.
# It supports both Managed Identity and AAD Client Credentials authentication methods.
#
# Responsibilities:
# - Store configuration options
# - Validate configuration for Managed Identity or AAD
# - Provide Azure AD endpoint based on cloud

require 'logger'

AZURE_CLOUDS = {
  'AzureCloud' => { 'aad' => 'https://login.microsoftonline.com' },
  'AzureChinaCloud' => { 'aad' => 'https://login.chinacloudapi.cn' },
  'AzureUSGovernment' => { 'aad' => 'https://login.microsoftonline.us' }
}.freeze

class OutputConfiguration
  def initialize(opts = {})
    # Initialize configuration options and logger
    @logger_path = opts[:logger_path]
    @logger = initialize_logger
    @client_app_id = opts[:client_app_id]
    @client_app_secret = opts[:client_app_secret]
    @tenant_id = opts[:tenant_id]
    @kusto_endpoint = opts[:kusto_endpoint]
    @database_name = opts[:database_name]
    @table_name = opts[:table_name]
    @azure_cloud = opts[:azure_cloud] || 'AzureCloud'
    @managed_identity_client_id = opts[:managed_identity_client_id]
    @azure_clouds = AZURE_CLOUDS
    @auth_type = opts[:auth_type] || 'aad'
    @workload_identity_client_id = opts[:workload_identity_client_id]
    @workload_identity_tenant_id = opts[:workload_identity_tenant_id]
    @workload_identity_token_file_path = opts[:workload_identity_token_file_path]
    validate_configuration
  end

  def validate_configuration
    # Validate configuration based on authentication method
    case @auth_type&.downcase
    when 'aad'
      validate_aad_config
    when 'user_managed_identity', 'system_managed_identity', 'azcli'
      validate_base_config
    when 'workload_identity'
      validate_workload_identity_config
    else
      raise ArgumentError, "Unknown auth_type: #{@auth_type}"
    end
    validate_azure_cloud
    true
  end

  def print_missing_parameter_message_and_raise(param_name)
    # Print error and raise if a required parameter is missing
    @logger.error(
      "Missing a required setting for the Kusto output plugin configuration:\n" \
      "output {\nkusto {\n#{param_name} => # SETTING MISSING\n ...\n}\n}\n"
    )
    raise ArgumentError, "The setting #{param_name} is required for Kusto configuration."
  end

  attr_reader :logger, :client_app_id, :client_app_secret, :tenant_id, :kusto_endpoint, :database_name, :table_name,
              :managed_identity_client_id, :azure_cloud, :auth_type, :workload_identity_client_id,
              :workload_identity_tenant_id, :workload_identity_token_file_path

  def aad_endpoint
    # Return Azure AD endpoint for selected cloud
    @azure_clouds[@azure_cloud]['aad']
  end

  private

  def initialize_logger
    # Use logger_path if provided, otherwise log to stdout
    logger = if @logger_path && !@logger_path.strip.empty?
               Logger.new(@logger_path, 'daily')
             else
               Logger.new($stdout)
             end
    logger.level = Logger::DEBUG
    logger
  end

  def using_managed_identity?
    val = @managed_identity_client_id.to_s.strip
    !val.empty?
  end

  def validate_base_config
    # Validate required configs for Managed Identity
    required = {
      'kusto_endpoint' => @kusto_endpoint,
      'database_name' => @database_name,
      'table_name' => @table_name
    }
    check_required_configs(required, %w[kusto_endpoint database_name table_name])
    # No further validation needed for SYSTEM or GUID
  end

  def validate_aad_config
    # Validate required configs for AAD
    required = aad_required_hash
    check_required_configs(
      required,
      %w[client_app_id client_app_secret tenant_id kusto_endpoint database_name table_name]
    )
  end

  def validate_workload_identity_config
    # Validate required configs for Workload Identity
    required = {
      'workload_identity_client_id' => @workload_identity_client_id,
      'workload_identity_tenant_id' => @workload_identity_tenant_id,
      'kusto_endpoint' => @kusto_endpoint,
      'database_name' => @database_name,
      'table_name' => @table_name
    }
    check_required_configs(required, %w[client_app_id tenant_id kusto_endpoint database_name table_name])
  end

  def aad_required_hash
    # Return required config hash for AAD
    {
      'client_app_id' => @client_app_id,
      'client_app_secret' => @client_app_secret,
      'tenant_id' => @tenant_id,
      'kusto_endpoint' => @kusto_endpoint,
      'database_name' => @database_name,
      'table_name' => @table_name
    }
  end

  def check_required_configs(required_configs, names)
    # Check for missing or empty required configs
    required_configs.each do |name, conf|
      print_missing_parameter_message_and_raise(name) if conf.nil?
    end
    return unless required_configs.values.any? { |conf| conf.to_s.strip.empty? }

    raise ArgumentError,
          "Malformed configuration, the following arguments can not be null or empty. [#{names.join(', ')}]"
  end

  def validate_azure_cloud
    # Validate that the selected Azure cloud is supported
    return if @azure_clouds.key?(@azure_cloud)

    raise ArgumentError,
          "The specified Azure cloud #{@azure_cloud} is not supported. Supported clouds are: " \
          "#{@azure_clouds.keys.join(', ')}."
  end
end
