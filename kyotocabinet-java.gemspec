# -*- encoding: utf-8 -*-
require File.expand_path('../lib/kyotocabinet-java/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Clayton Wheeler"]
  gem.email         = ["cswh@umich.edu"]
  gem.description   = %q{Kyoto Cabinet Java library for JRuby}
  gem.summary       = %q{Wrapper for Kyoto Cabinet's Java library for use with JRuby, to provide the same interface as the native kyotocabinet-ruby gem for MRI.'}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.extensions    = ["ext/kyotocabinet-java/configure"]
  gem.name          = "kyotocabinet-java"
  gem.require_paths = ["lib"]
  gem.version       = KyotoCabinet::Java::VERSION
end
