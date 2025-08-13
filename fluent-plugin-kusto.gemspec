# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'azure-kusto-fluentd'
  spec.version       = '0.1.0'
  spec.authors       = ['Komal Rani']
  spec.email         = ['t-komalrani+microsoft@microsoft.com']

  spec.summary       = 'A custom Fluentd output plugin for Azure Kusto ingestion.'
  spec.description   = 'Fluentd output plugin to ingest logs into Azure Data Explorer (Kusto), ' \
                      'supporting managed identity and AAD authentication, with multi-worker and buffer support.'
  spec.homepage      = 'https://github.com/yourusername/azure-kusto-fluentd'
  spec.license       = 'Apache-2.0'

  spec.required_ruby_version = '>= 2.5'

  spec.files         = Dir["lib/**/*.rb", "tests/**/*", "README.md", "LICENSE", "Gemfile"]
  spec.test_files    = spec.files.grep(%r{^(test|tests|spec|features)/})
  spec.require_paths = ['lib']
  spec.metadata      = { "fluentd_plugin" => "true", "fluentd_group" => "output" }

  spec.add_development_dependency 'bundler', '~> 2.6.9'
  spec.add_development_dependency 'rake', '~> 13.2.1'
  spec.add_development_dependency 'test-unit', '~> 3.6.7'
  spec.add_runtime_dependency 'fluentd', '>= 1.0', '< 2'
  spec.add_dependency 'azure-storage-blob'
  spec.add_dependency 'azure-storage-queue'
  spec.add_dependency 'azure-storage-table'
  spec.add_dependency 'dotenv'
end
