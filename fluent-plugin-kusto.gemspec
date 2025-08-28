# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require_relative 'lib/fluent/plugin/kusto_version'

Gem::Specification.new do |spec|
  spec.name          = 'fluent-plugin-kusto'
  spec.version       = Fluent::Plugin::Kusto::VERSION
  spec.authors       = ['Komal Rani', 'Kusto OSS IDC Team']
  spec.email         = ['t-komalrani+microsoft@microsoft.com', 'kustoossidc@microsoft.com']

  spec.summary       = 'A custom Fluentd output plugin for Azure Kusto ingestion.'
  spec.description   = 'Fluentd output plugin to ingest logs into Azure Data Explorer (Kusto), ' \
                      'supporting managed identity and AAD authentication, with multi-worker and buffer support.'
  spec.homepage      = 'https://github.com/Azure/azure-kusto-fluentd'
  spec.license       = 'Apache-2.0'

  spec.required_ruby_version = '>= 2.7.0'

  spec.files         = Dir['lib/**/*.rb', 'test/**/*', 'README.md', 'LICENSE', 'Gemfile']
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']
  spec.metadata      = { 'fluentd_plugin' => 'true', 'fluentd_group' => 'output' }

  spec.add_development_dependency 'bundler', '>= 2.0'
  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'test-unit', '~> 3.0'
  spec.add_development_dependency 'fluentd', '>= 1.0', '< 2'
  spec.add_runtime_dependency 'fluentd', '>= 1.0', '< 2'
  spec.add_development_dependency 'rubocop', '~> 1.0'
  spec.add_dependency 'dotenv', '~> 2.0'
end
