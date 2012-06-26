# kyotocabinet-java

This is a Ruby gem providing a JRuby interface for the
[Kyoto Cabinet][] database library. It aims to provide an identical
interface to the official [Ruby library][]. However, that library uses
Ruby's C extension API and thus does not work under JRuby. This is
instead implemented atop Kyoto Cabinet's [Java API][], with a minimal
Ruby adaptation layer to compensate for interface differences.

[Kyoto Cabinet]: http://fallabs.com/kyotocabinet/
[Ruby library]: http://fallabs.com/kyotocabinet/
[Java API]: http://fallabs.com/kyotocabinet/javadoc/

## Prerequisites

Installation of this gem requires that Kyoto Cabinet (>= 1.2.76)
already be installed. It also requires a C++ compiler and JDK, since
it contains a patched copy of the [kyotocabinet-java][] library, which
will be compiled upon installation. It does not attempt to install
anything outside the RubyGems directory.

[kyotocabinet-java]: http://fallabs.com/kyotocabinet/javapkg/

## Installation

```
$ gem install kyotocabinet-java
```

Or, if you're using [Bundler][] and want your Ruby code to be able to
transparently use Kyoto Cabinet under MRI or JRuby, you might want to
add something like this to your Gemfile:

[Bundler]: http://gembundler.com/

```ruby
gem "kyotocabinet-ruby", "~> 1.27.1", :platforms => [:mri, :rbx]
gem "kyotocabinet-java", "~> 0.2.0", :platforms => :jruby
```

## Usage

This is designed to be a drop-in replacement for `kyotocabinet-ruby`,
and so usage should be identical:

```ruby
require 'kyotocabinet'
db = KyotoCabinet::DB.new
db.open("%", KyotoCabinet::DB::OWRITER | KyotoCabinet::DB::OCREATE)
db["hello"] = "world"
```

## Status

This library was written to maintain indexes for a
[BioRuby MAF parser][]. It works for that purpose but has not been
exhaustively tested. Other parts of the Kyoto Cabinet API might need
some work. The `bioruby-maf` code relies on storing binary data in
Kyoto Cabinet; this works with the Java API methods accepting or
returning `byte[]`, but not their `String` counterparts. Some of these
methods have been wrapped to explicitly convert arguments and return
values to byte arrays, but not all. See [kyotocabinet.rb][] for
examples. Feel free to submit pull requests.

[BioRuby MAF parser]: https://github.com/csw/bioruby-maf
[kyotocabinet.rb]: https://github.com/csw/kyotocabinet-java/blob/master/lib/kyotocabinet.rb

Installation has been tested on Mac OS X 10.7.4 and Debian wheezy, and
should work on Ubuntu and Fedora as well. Please report build problems
on other Unix platforms; it should be fairly easy to resolve them.

It's not completely clear what it would take to make this work on
Windows; shipping a pre-built JAR and the binary DLL provided by FAL
Labs might do it. Patches would be welcome.

## Contributing to kyotocabinet-java
 
* Check out the latest master to make sure the feature hasn't been implemented or the bug hasn't been fixed yet.
* Check out the issue tracker to make sure someone already hasn't requested it and/or contributed it.
* Fork the project.
* Start a feature/bugfix branch.
* Commit and push until you are happy with your contribution.
* Make sure to add tests for it. This is important so I don't break it in a future version unintentionally.
* Please try not to mess with the Rakefile, version, or history. If you want to have your own version, or is otherwise necessary, that is fine, but please isolate to its own commit so I can cherry-pick around it.

## Copyright

Copyright (c) 2012 Clayton Wheeler. See LICENSE.txt for
further details.

