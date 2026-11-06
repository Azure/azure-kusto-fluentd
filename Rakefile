# frozen_string_literal: true

require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs.push('lib', 'test')
  t.test_files = FileList['test/**/test_*.rb']
  t.verbose = true
  t.warning = true
  if ENV['JUNIT_XML_OUTPUT']
    t.ruby_opts << '-rtest/unit/runner/junitxml'
    t.options = "--runner=junitxml --junitxml-output-file=#{ENV.fetch('JUNIT_XML_OUTPUT')}"
  end
end

task default: [:test]
