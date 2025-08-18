# frozen_string_literal: true

require 'json'
require 'time'
require 'open3'
require 'shellwords'
require_relative 'tokenprovider_base'

class AzCliTokenProvider < AbstractTokenProvider
  def initialize(outconfiguration)
    super(outconfiguration)
    @resource = outconfiguration.kusto_endpoint
  end

  # Use get_token from base class for token retrieval

  def fetch_token
    token = acquire_token(@resource)
    raise "No valid Azure CLI token found for resource: #{@resource}" unless token

    {
      access_token: token['accessToken'],
      expires_in: (Time.parse(token['expiresOn']) - Time.now).to_i
    }
  end

  def acquire_token(resource)
    az_cli = locate_azure_cli
    cmd = "#{az_cli} account get-access-token --resource #{Shellwords.escape(resource)} --output json"
    stdout, stderr, status = Open3.capture3(cmd)
    unless status.success?
      raise "Failed to acquire Azure CLI token: #{stderr.strip}"
    end
    JSON.parse(stdout)
  rescue Errno::ENOENT
    raise "Azure CLI not found. Please install Azure CLI and run 'az login'."
  end

  def locate_azure_cli
    exts = ENV['PATHEXT'] ? ENV['PATHEXT'].split(';') : ['']
    ENV['PATH'].split(File::PATH_SEPARATOR).each do |path|
      exts.each do |ext|
        exe = File.join(path, "az#{ext}")
        return exe if File.executable?(exe) && !File.directory?(exe)
      end
    end
    raise "Azure CLI executable 'az' not found in PATH."
  end
end
