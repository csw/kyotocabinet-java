# encoding: utf-8

require 'rubygems'
require 'bundler'
begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end
require 'rake'
require 'rake/clean'

require 'jeweler'
Jeweler::Tasks.new do |gem|
  # gem is a Gem::Specification... see http://docs.rubygems.org/read/chapter/20 for more options
  gem.name = "kyotocabinet-java"
  gem.homepage = "http://github.com/csw/kyotocabinet-java"
  gem.license = "GPL"
  gem.summary = %Q{Kyoto Cabinet Java library for JRuby}
  gem.description = %Q{Wrapper for Kyoto Cabinet's Java library for use with JRuby, to provide the same interface as the native kyotocabinet-ruby gem for MRI.}
  gem.email = "cswh@umich.edu"
  gem.authors = ["FAL Labs", "Clayton Wheeler"]

  gem.platform = 'java'
  gem.extensions = ["ext/kyotocabinet-java/configure"]
  # dependencies defined in Gemfile
end
Jeweler::RubygemsDotOrgTasks.new

file "ext/kyotocabinet-java/configure" => ["ext/kyotocabinet-java/configure.in"] do
  sh "cd ext/kyotocabinet-java; autoconf"
end
task :build => "ext/kyotocabinet-java/configure"

file "test_ext/kyotocabinet.jar" => ["ext/kyotocabinet-java/configure"] do
  sh "cd ext/kyotocabinet-java; ./configure --prefix=#{File.expand_path('../test_ext', __FILE__)} && make && make install && make clean"
end

CLEAN.include("ext/kyotocabinet-java/Makefile")
CLEAN.include("test_ext/*")

CLOBBER.include("ext/kyotocabinet-java/config.status")
CLOBBER.include("ext/kyotocabinet-java/config.tmp")

require 'rspec/core'
require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end
task :spec => "test_ext/kyotocabinet.jar"

# XXX: using simplecov instead
# require 'rcov/rcovtask'
# Rcov::RcovTask.new do |test|
#   test.libs << 'test'
#   test.pattern = 'test/**/test_*.rb'
#   test.verbose = true
#   test.rcov_opts << '--exclude "gems/*"'
# end

task :default => :test

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "kyotocabinet-java #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
