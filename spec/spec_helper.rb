require 'rspec'
if RUBY_PLATFORM == 'java'
  require 'bundler/setup'

  $LOAD_PATH << File.expand_path('../../test_ext', __FILE__)
else
  # enable running this against the kyotocabinet-ruby library for
  # compatibility testing.
  $LOAD_PATH.delete_if { |e| e =~ %r{kyotocabinet-java/lib} }
  $stderr.puts $LOAD_PATH.join("\n")
end

require 'kyotocabinet'
