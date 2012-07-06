##
## JRuby adaptation layer for Kyoto Cabinet Java interface.
##
## This requires the kyotocabinet-java library to be installed. This
## library builds a jar file, which should be on the Ruby $LOAD_PATH,
## the Java CLASSPATH, or in /usr/local/lib/ where it is placed by the
## default 'make install' target in kyotocabinet-java.
##
## kyotocabinet-java also builds a necessary JNI library,
## libjkyotocabinet.{jnilib,dylib}. This cannot be added to
## java.library.path at runtime, so either its installation path must
## be added to java.library.path with a JVM option like
## -Djava.library.path=/some/dir, or it must be copied to somewhere
## already on java.library.path.
##

unless RUBY_PLATFORM == 'java'
  raise "This library requires JRuby!"
end

require 'java'

unless Java.const_defined? :Kyotocabinet
  # need to require kyotocabinet.jar
  soname = java.lang.System.mapLibraryName("jkyotocabinet")
  jdir = $LOAD_PATH.find { |d| File.exist? "#{d}/kyotocabinet.jar" }
  raise "Could not find kyotocabinet.jar on $LOAD_PATH (#{$LOAD_PATH.join(':')})!" unless jdir
  sopath = "#{jdir}/#{soname}"
  unless File.exist? sopath
    raise "JNI library #{soname} not installed in #{jdir}!"
  end
  java.lang.System.setProperty("kyotocabinet.lib", sopath)
  require 'kyotocabinet.jar'
  begin
    Java::Kyotocabinet::Loader.load
  rescue NameError
    raise "Kyoto Cabinet classes could not be loaded!"
  end
end

module KyotoCabinet
  module Adaptation
    BYTE_ARRAY = [1].to_java(:byte).java_class

    def conv_string_array(a)
      ba = Java::byte[a.size, 0].new
      a.each_with_index do |k, i|
        ba[i] = k.to_java_bytes
      end
      ba
    end

    def to_string_array(ba)
      ba.collect { |e| String.from_java_bytes(e) }
    end

    def self.included(mod)
      super(mod)
      mod.instance_eval do
        def with_java_method(mname, args)
          mh = self.java_method(mname, args)
          yield mh
        end
        def convert_args(mname, args)
          indices = []
          args.each_with_index do |a, i|
            if a == BYTE_ARRAY
              indices << i
            end
          end
          mh = self.java_method(mname, args)
          self.send(:define_method, mname) do |*argv|
            indices.each { |i| argv[i] = argv[i].to_java_bytes }
            mh.bind(self).call(*argv)
          end
        end

        def convert_args_ret(mname, args)
          indices = []
          args.each_with_index do |a, i|
            if a == BYTE_ARRAY
              indices << i
            end
          end
          mh = self.java_method(mname, args)
          self.send(:define_method, mname) do |*argv|
            indices.each { |i| argv[i] = argv[i].to_java_bytes }
            rv = mh.bind(self).call(*argv)
            if rv
              String.from_java_bytes(rv)
            end
          end
        end
      end
    end

    def ret_bytes(v)
      if v
        String.from_java_bytes(v)
      end
    end
  end

  class VisitorProxy
    include Java::Kyotocabinet::Visitor
    
    def initialize(v)
      @v = v
    end

    def visit_empty(key)
      rv = @v.visit_empty(String.from_java_bytes(key))
      rv ? rv.to_java_bytes : nil
    end

    def visit_full(key, value)
      rv = @v.visit_full(String.from_java_bytes(key),
                         String.from_java_bytes(value))
      rv ? rv.to_java_bytes : nil
    end
  end

  class BlockVisitor

    def self.wrap(proc)
      VisitorProxy.new(self.new(proc))
    end

    def initialize(proc)
      @proc = proc
    end

    def visit_empty(key)
      @proc.call(key)
    end

    def visit_full(key, value)
      @proc.call(key, value)
    end
    
  end

end

module Java::Kyotocabinet
  VisitorProxy = KyotoCabinet::VisitorProxy
  BlockVisitor = KyotoCabinet::BlockVisitor

  class Cursor
    include KyotoCabinet::Adaptation

    alias_method :_get, :get
    def get(step=false)
      r = self._get(step)
      if r
        return [String.from_java_bytes(r[0]),
                String.from_java_bytes(r[1])]
      else
        return nil
      end
    end

    alias_method :_get_key, :get_key
    def get_key(step=false)
      r = self._get_key(step)
      if r
        return String.from_java_bytes(r)
      else
        return nil
      end
    end

    alias_method :_jump, :jump
    def jump(key)
      self._jump(key.to_java_bytes)
    end
  end # class Cursor

  class DB
    include KyotoCabinet::Adaptation

    # The Java API is thread-safe so this can be a no-op
    GCONCURRENT = 0

    alias_method :_accept, :accept
    def accept(key, visitor=nil, writable=true)
      self._accept(key.to_java_bytes,
                   VisitorProxy.new(visitor),
                   writable)
    end

    alias_method :_accept_bulk, :accept_bulk
    def accept_bulk(keys, visitor=nil, writable=true)
      self._accept_bulk(conv_string_array(keys),
                        VisitorProxy.new(visitor),
                        writable)
    end

    convert_args :add, [BYTE_ARRAY, BYTE_ARRAY]
    convert_args :append, [BYTE_ARRAY, BYTE_ARRAY]
    convert_args :cas, [BYTE_ARRAY, BYTE_ARRAY, BYTE_ARRAY]
    convert_args :check, [BYTE_ARRAY]

    alias_method :_match_prefix, :match_prefix
    def match_prefix(prefix, limit=-1)
      self._match_prefix(prefix, limit)
    end

    alias_method :_get, :get
    def get(key)
      ret_bytes(self._get(key.to_java_bytes))
    end
    alias_method :[], :get

    alias_method :_get_bulk, :get_bulk
    def get_bulk(keys, atomic=true)
      ra = _get_bulk(conv_string_array(keys), atomic)
      h = {}
      i = 0
      while i < ra.size
        h[String.from_java_bytes(ra[i])] = String.from_java_bytes(ra[i + 1])
        i += 2
      end
      h
    end

    alias_method :_increment, :increment
    def increment(key, num=0, orig=0)
      self._increment(key.to_java_bytes, num, orig)
    end

    alias_method :_increment_double, :increment_double
    def increment_double(key, num=0, orig=0)
      _increment_double(key.to_java_bytes, num, orig)
    end

    alias_method :_iterate, :iterate
    def iterate(visitor=nil, writable=true, &blk)
      if visitor
        _iterate(VisitorProxy.new(visitor),
                 writable)
      else
        _iterate(BlockVisitor.wrap(blk),
                 writable)
      end
    end


    alias_method :_match_prefix, :match_prefix
    def match_prefix(prefix, max=-1)
      _match_prefix(prefix, max)
    end

    alias_method :_match_regex, :match_regex
    def match_regex(regex, max=-1)
      _match_regex(regex, max)
    end

    alias_method :_match_similar, :match_similar
    def match_similar(origin, range=1, utf=false, max=-1)
      _match_similar(origin, range, utf, max)
    end

    alias_method :_merge, :merge
    def merge(srcary, mode=DB::MSET)
      _merge(srcary.to_java(DB), mode)
    end

    alias_method :_occupy, :occupy
    def occupy(writable=false, proc=nil)
      _occupy(writable, proc)
    end

    alias_method :_open, :open
    def open(path=':', mode=DB::OWRITER|DB::OCREATE)
      _open(path, mode)
    end

    convert_args :remove, [BYTE_ARRAY]

    alias_method :_remove_bulk, :remove_bulk
    def remove_bulk(keys, atomic=true)
      _remove_bulk(conv_string_array(keys), atomic)
    end

    convert_args :replace, [BYTE_ARRAY, BYTE_ARRAY]

    convert_args_ret :seize, [BYTE_ARRAY]

    alias_method :_set, :set
    def set(k, v)
      self._set(k.to_java_bytes, v.to_s.to_java_bytes)
    end
    alias_method :[]=, :set
    alias_method :store, :set

    alias_method :_set_bulk, :set_bulk
    def set_bulk(rec_h, atomic)
      ba = Java::byte[rec_h.size * 2, 0].new
      i = 0
      rec_h.each_pair do |k, v|
        ba[i]   = k.to_java_bytes
        ba[i+1] = v.to_java_bytes
        i += 2
      end
      self._set_bulk(ba, atomic)
    end

    ## TODO: store?

    alias_method :_synchronize, :synchronize
    def synchronize(hard=false, proc=nil)
      self._synchronize(hard, proc)
    end

    def transaction(hard=false)
      begin_transaction(hard)
      commit = false
      begin
        commit = yield
      ensure
        end_transaction(commit)
      end
      commit
    end


    def cursor_process
      cur = self.cursor()
      begin
        yield cur
      ensure
        cur.disable()
      end
    end
  end # class DB
end

module KyotoCabinet
  java_import Java::Kyotocabinet::Cursor
  java_import Java::Kyotocabinet::DB
  java_import Java::Kyotocabinet::Error
  java_import Java::Kyotocabinet::FileProcessor
  java_import Java::Kyotocabinet::Visitor
end
