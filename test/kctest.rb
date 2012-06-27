#! /usr/bin/ruby -w
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------------------------------
# The test cases of the Ruby binding
#                                                                Copyright (C) 2009-2010 FAL Labs
# This file is part of Kyoto Cabinet.
# This program is free software: you can redistribute it and/or modify it under the terms of
# the GNU General Public License as published by the Free Software Foundation, either version
# 3 of the License, or any later version.
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
# You should have received a copy of the GNU General Public License along with this program.
# If not, see <http://www.gnu.org/licenses/>.
#-------------------------------------------------------------------------------------------------


require 'kyotocabinet'
require 'fileutils'
include KyotoCabinet


# main routine
def main
  ARGV.length >= 1 || usage
  if ARGV[0] == "order"
    rv = runorder
  elsif ARGV[0] == "wicked"
    rv = runwicked
  elsif ARGV[0] == "misc"
    rv = runmisc
  else
    usage
  end
  GC.start
  return rv
end


# print the usage and exit
def usage
  STDERR.printf("%s: test cases of the Ruby binding\n", $progname)
  STDERR.printf("\n")
  STDERR.printf("usage:\n")
  STDERR.printf("  %s order [-cc] [-th num] [-rnd] [-etc] path rnum\n", $progname)
  STDERR.printf("  %s wicked [-cc] [-th num] [-it num] path rnum\n", $progname)
  STDERR.printf("  %s misc path\n", $progname)
  STDERR.printf("\n")
  exit(1)
end


# print the error message of the database
def dberrprint(db, func)
  err = db.error
  printf("%s: %s: %d: %s: %s\n", $progname, func, err.code, err.name, err.message)
end


# print members of a database
def dbmetaprint(db, verbose)
  if verbose
    status = db.status
    if status
      status.each { |name, value|
        printf("%s: %s\n", name, value)
      }
    end
  else
    printf("count: %d\n", db.count)
    printf("size: %d\n", db.size)
  end
end


# parse arguments of order command
def runorder
  path = nil
  rnum = nil
  gopts = 0
  thnum = 1
  rnd = false
  etc = false
  i = 1
  while i < ARGV.length
    arg = ARGV[i]
    if !path && arg =~ /^-/
      if arg == "-cc"
        gopts |= DB::GCONCURRENT
      elsif arg == "-th"
        i += 1
        usage if i >= ARGV.length
        thnum = ARGV[i].to_i
      elsif arg == "-rnd"
        rnd = true
      elsif arg == "-etc"
        etc = true
      else
        usage
      end
    elsif !path
      path = arg
    elsif !rnum
      rnum = arg.to_i
    else
      usage
    end
    i += 1
  end
  usage if !path || !rnum || rnum < 1 || thnum < 1
  rv = procorder(path, rnum, gopts, thnum, rnd, etc)
  return rv
end


# parse arguments of wicked command
def runwicked
  path = nil
  rnum = nil
  gopts = 0
  thnum = 1
  itnum = 1
  i = 1
  while i < ARGV.length
    arg = ARGV[i]
    if !path && arg =~ /^-/
      if arg == "-cc"
        gopts |= DB::GCONCURRENT
      elsif arg == "-th"
        i += 1
        usage if i >= ARGV.length
        thnum = ARGV[i].to_i
      elsif arg == "-it"
        i += 1
        usage if i >= ARGV.length
        itnum = ARGV[i].to_i
      else
        usage
      end
    elsif !path
      path = arg
    elsif !rnum
      rnum = arg.to_i
    else
      usage
    end
    i += 1
  end
  usage if !path || !rnum || rnum < 1 || thnum < 1 || itnum < 1
  rv = procwicked(path, rnum, gopts, thnum, itnum)
  return rv
end


# parse arguments of wicked command
def runmisc
  path = nil
  i = 1
  while i < ARGV.length
    arg = ARGV[i]
    if !path && arg =~ /^-/
      usage
    elsif !path
      path = arg
    else
      usage
    end
    i += 1
  end
  usage if !path
  rv = procmisc(path)
  return rv
end


# perform order command
def procorder(path, rnum, gopts, thnum, rnd, etc)
  printf("<In-order Test>\n  path=%s  rnum=%d  gopts=%d  thnum=%d  rnd=%s  etc=%s\n\n",
         path, rnum, gopts, thnum, rnd, etc)
  err = false
  db = DB::new(gopts)
  db.tune_exception_rule([ Error::SUCCESS, Error::NOIMPL, Error::MISC ])
  db.tune_encoding("utf-8")
  printf("opening the database:\n")
  stime = Time::now
  if !db.open(path, DB::OWRITER | DB::OCREATE | DB::OTRUNCATE)
    dberrprint(db, "DB::open")
    err = true
  end
  etime = Time::now
  printf("time: %.3f\n", etime - stime)
  printf("setting records:\n")
  stime = Time::now
  threads = Array::new
  for thid in 0...thnum
    th = Thread::new(thid) { |id|
      base = id * rnum
      range = rnum * thnum
      for i in 1..rnum
        break if err
        key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
        if !db.set(key, key)
          dberrprint(db, "DB::set")
          err = true
        end
        if id < 1 && rnum > 250 && i % (rnum / 250) == 0
          print(".")
          if i == rnum || i % (rnum / 10) == 0
            printf(" (%08d)\n", i)
          end
        end
      end
    }
    threads.push(th)
  end
  threads.each { |jth|
    jth.join
  }
  etime = Time::now
  dbmetaprint(db, false)
  printf("time: %.3f\n", etime - stime)
  if etc
    printf("adding records:\n")
    stime = Time::now
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        base = id * rnum
        range = rnum * thnum
        for i in 1..rnum
          break if err
          key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
          if !db.add(key, key) && db.error != Error::DUPREC
            dberrprint(db, "DB::add")
            err = true
          end
          if id < 1 && rnum > 250 && i % (rnum / 250) == 0
            print(".")
            if i == rnum || i % (rnum / 10) == 0
              printf(" (%08d)\n", i)
            end
          end
        end
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    etime = Time::now
    dbmetaprint(db, false)
    printf("time: %.3f\n", etime - stime)
  end
  if etc
    printf("appending records:\n")
    stime = Time::now
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        base = id * rnum
        range = rnum * thnum
        for i in 1..rnum
          break if err
          key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
          if !db.append(key, key)
            dberrprint(db, "DB::append")
            err = true
          end
          if id < 1 && rnum > 250 && i % (rnum / 250) == 0
            print(".")
            if i == rnum || i % (rnum / 10) == 0
              printf(" (%08d)\n", i)
            end
          end
        end
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    etime = Time::now
    dbmetaprint(db, false)
    printf("time: %.3f\n", etime - stime)
  end
  if etc && !(gopts & DB::GCONCURRENT)
    printf("accepting visitors:\n")
    stime = Time::now
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        me = Thread::current
        me[:rnd] = rnd
        me[:cnt] = 0
        def me.visit_full(key, value)
          self[:cnt] += 1
          Thread::pass if self[:cnt] % 100 == 0
          rv = Visitor::NOP
          if self[:rnd]
            case rand(7)
            when 0
              rv = self[:cnt]
            when 1
              rv = Visitor::REMOVE
            end
          end
          return rv
        end
        def me.visit_empty(key)
          return visit_full(key, key)
        end
        base = id * rnum
        range = rnum * thnum
        for i in 1..rnum
          break if err
          key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
          if !db.accept(key, me, rnd)
            dberrprint(db, "DB::accept")
            err = true
          end
          if id < 1 && rnum > 250 && i % (rnum / 250) == 0
            print(".")
            if i == rnum || i % (rnum / 10) == 0
              printf(" (%08d)\n", i)
            end
          end
        end
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    etime = Time::now
    dbmetaprint(db, false)
    printf("time: %.3f\n", etime - stime)
  end
  printf("getting records:\n")
  stime = Time::now
  threads = Array::new
  for thid in 0...thnum
    th = Thread::new(thid) { |id|
      base = id * rnum
      range = rnum * thnum
      for i in 1..rnum
        break if err
        key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
        if !db.get(key) && db.error != Error::NOREC
          dberrprint(db, "DB::get")
          err = true
        end
        if id < 1 && rnum > 250 && i % (rnum / 250) == 0
          print(".")
          if i == rnum || i % (rnum / 10) == 0
            printf(" (%08d)\n", i)
          end
        end
      end
    }
    threads.push(th)
  end
  threads.each { |jth|
    jth.join
  }
  etime = Time::now
  dbmetaprint(db, false)
  printf("time: %.3f\n", etime - stime)
  if etc && !(gopts & DB::GCONCURRENT)
    printf("traversing the database by the inner iterator:\n")
    stime = Time::now
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        me = Thread::current
        me[:id] = id
        me[:rnum] = rnum
        me[:rnd] = rnd
        me[:cnt] = 0
        def me.visit_full(key, value)
          self[:cnt] += 1
          Thread::pass if self[:cnt] % 100 == 0
          rv = Visitor::NOP
          if self[:rnd]
            case rand(7)
            when 0
              rv = self[:cnt].to_s * 2
            when 1
              rv = Visitor::REMOVE
            end
          end
          if self[:id] < 1 && self[:rnum] > 250 && self[:cnt] % (self[:rnum] / 250) == 0
            print(".")
            if self[:cnt] == self[:rnum] || self[:cnt] % (self[:rnum] / 10) == 0
              printf(" (%08d)\n", self[:cnt])
            end
          end
          return rv
        end
        def me.visit_empty(key)
          return Visitor::NOP
        end
        if !db.iterate(me, rnd)
          dberrprint(db, "DB::iterate")
          err = true
        end
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    printf(" (end)\n") if rnd
    etime = Time::now
    dbmetaprint(db, false)
    printf("time: %.3f\n", etime - stime)
  end
  if etc && !(gopts & DB::GCONCURRENT)
    printf("traversing the database by the outer cursor:\n")
    stime = Time::now
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        me = Thread::current
        me[:id] = id
        me[:rnum] = rnum
        me[:rnd] = rnd
        me[:cnt] = 0
        def me.visit_full(key, value)
          self[:cnt] += 1
          Thread::pass if self[:cnt] % 100 == 0
          rv = Visitor::NOP
          if self[:rnd]
            case rand(7)
            when 0
              rv = self[:cnt].to_s * 2
            when 1
              rv = Visitor::REMOVE
            end
          end
          if self[:id] < 1 && self[:rnum] > 250 && self[:cnt] % (self[:rnum] / 250) == 0
            print(".")
            if self[:cnt] == self[:rnum] || self[:cnt] % (self[:rnum] / 10) == 0
              printf(" (%08d)\n", self[:cnt])
            end
          end
          return rv
        end
        def me.visit_empty(key)
          return Visitor::NOP
        end
        cur = db.cursor
        if !cur.jump && db.error != Error::NOREC
          dberrprint(db, "Cursor::jump")
          err = true
        end
        while cur.accept(me, rnd, false)
          if !cur.step && db.error != Error::NOREC
            dberrprint(db, "Cursor::step")
            err = true
          end
        end
        if db.error != Error::NOREC
          dberrprint(db, "Cursor::accept")
          err = true
        end
        cur.disable if !rnd || rand(2) == 0
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    printf(" (end)\n") if rnd
    etime = Time::now
    dbmetaprint(db, false)
    printf("time: %.3f\n", etime - stime)
  end
  printf("removing records:\n")
  stime = Time::now
  threads = Array::new
  for thid in 0...thnum
    th = Thread::new(thid) { |id|
      base = id * rnum
      range = rnum * thnum
      for i in 1..rnum
        break if err
        key = sprintf("%08d", rnd ? rand(range) + 1 : base + i)
        if !db.remove(key) && db.error != Error::NOREC
          dberrprint(db, "DB::remove")
          err = true
        end
        if id < 1 && rnum > 250 && i % (rnum / 250) == 0
          print(".")
          if i == rnum || i % (rnum / 10) == 0
            printf(" (%08d)\n", i)
          end
        end
      end
    }
    threads.push(th)
  end
  threads.each { |jth|
    jth.join
  }
  etime = Time::now
  dbmetaprint(db, true)
  printf("time: %.3f\n", etime - stime)
  printf("closing the database:\n")
  stime = Time::now
  if !db.close
    dberrprint(db, "DB::close")
    err = true
  end
  etime = Time::now
  printf("time: %.3f\n", etime - stime)
  printf("%s\n\n", err ? "error" : "ok")
  return err ? 1 : 0
end


# perform wicked command
def procwicked(path, rnum, gopts, thnum, itnum)
  printf("<Wicked Test>\n  path=%s  rnum=%d  gopts=%d  thnum=%d  itnum=%d\n\n",
         path, rnum, gopts, thnum, itnum)
  err = false
  db = DB::new(gopts)
  db.tune_exception_rule([ Error::SUCCESS, Error::NOIMPL, Error::MISC ])
  db.tune_encoding("utf-8") if rand(2) == 0
  for itcnt in 1..itnum
    printf("iteration %d:\n", itcnt) if itnum > 1
    stime = Time::now
    omode = DB::OWRITER | DB::OCREATE
    omode |= DB::OTRUNCATE if itcnt == 1
    if !db.open(path, omode)
      dberrprint(db, "DB::open")
      err = true
    end
    threads = Array::new
    for thid in 0...thnum
      th = Thread::new(thid) { |id|
        me = Thread::current
        me[:cnt] = 0
        def me.visit_full(key, value)
          self[:cnt] += 1
          Thread::pass if self[:cnt] % 100 == 0
          rv = Visitor::NOP
          if self[:rnd]
            case rand(7)
            when 0
              rv = self[:cnt]
            when 1
              rv = Visitor::REMOVE
            end
          end
          return rv
        end
        def me.visit_empty(key)
          return visit_full(key, key)
        end
        cur = db.cursor
        range = rnum * thnum
        for i in 1..rnum
          break if err
          tran = rand(100) == 0
          if tran && !db.begin_transaction(rand(rnum) == 0)
            dberrprint(db, "DB::begin_transaction")
            tran = false
            err = true
          end
          key = sprintf("%08d", rand(range) + 1)
          case rand(12)
          when 0
            if !db.set(key, key)
              dberrprint(db, "DB::set")
              err = true
            end
          when 1
            if !db.add(key, key) && db.error != Error::DUPREC
              dberrprint(db, "DB::add")
              err = true
            end
          when 2
            if !db.replace(key, key) && db.error != Error::NOREC
              dberrprint(db, "DB::replace")
              err = true
            end
          when 3
            if !db.append(key, key)
              dberrprint(db, "DB::append")
              err = true
            end
          when 4
            if rand(2) == 0
              if !db.increment(key, rand(10)) && db.error != Error::LOGIC
                dberrprint(db, "DB::increment")
                err = true
              end
            else
              if !db.increment_double(key, rand() * 10) && db.error != Error::LOGIC
                dberrprint(db, "DB::increment_double")
                err = true
              end
            end
          when 5
            if !db.cas(key, key, key) && db.error != Error::LOGIC
              dberrprint(db, "DB::cas")
              err = true
            end
          when 6
            if !db.remove(key) && db.error != Error::NOREC
              dberrprint(db, "DB::remove")
              err = true
            end
          when 7
            if !db.accept(key, me, true) &&
                (!(gopts & DB::GCONCURRENT) || db.error != Error::INVALID)
              dberrprint(db, "DB::accept")
              err = true
            end
          when 8
            if rand(10) == 0
              if rand(4) == 0
                begin
                  if !cur.jump_back(key) && db.error != Error::NOREC
                    dberrprint(db, "Cursor::jump_back")
                    err = true
                  end
                rescue Error::XNOIMPL
                end
              else
                if !cur.jump(key) && db.error != Error::NOREC
                  dberrprint(db, "Cursor::jump")
                  err = true
                end
              end
            else
              case rand(6)
              when 0
                if !cur.get_key && db.error != Error::NOREC
                  dberrprint(db, "Cursor::get_key")
                  err = true
                end
              when 1
                if !cur.get_value && db.error != Error::NOREC
                  dberrprint(db, "Cursor::get_value")
                  err = true
                end
              when 2
                if !cur.get && db.error != Error::NOREC
                  dberrprint(db, "Cursor::get")
                  err = true
                end
              when 3
                if !cur.remove && db.error != Error::NOREC
                  dberrprint(db, "Cursor::remove")
                  err = true
                end
              else
                if !cur.accept(me, true, rand(2) == 0) && db.error != Error::NOREC &&
                    (!(gopts & DB::GCONCURRENT) || db.error != Error::INVALID)
                  dberrprint(db, "Cursor::accept")
                  err = true
                end
              end
            end
            if rand(2) == 0
              if !cur.step && db.error != Error::NOREC
                dberrprint(db, "Cursor::step")
                err = true
              end
            end
            if rand(rnum / 50 + 1) == 0
              prefix = key[0,key.length-1]
              if !db.match_prefix(prefix, rand(10))
                dberrprint(db, "DB::match_prefix")
                err = true
              end
            end
            if rand(rnum / 50 + 1) == 0
              regex = key[0,key.length-1]
              if !db.match_regex(regex, rand(10))
                dberrprint(db, "DB::match_regex")
                err = true
              end
            end
            if rand(rnum / 50 + 1) == 0
              origin = key[0,key.length-1]
              if !db.match_similar(origin, 3, rand(10))
                dberrprint(db, "DB::match_similar")
                err = true
              end
            end
            if rand(10) == 0
              paracur = db.cursor
              paracur.jump(key)
              if !paracur.accept(me, true, rand(2) == 0) && db.error != Error::NOREC &&
                  (!(gopts & DB::GCONCURRENT) || db.error != Error::INVALID)
                dberrprint(db, "Cursor::accept")
                err = true
              end
              paracur.disable
            end
          else
            if !db.get(key) && db.error != Error::NOREC
              dberrprint(db, "DB::get")
              err = true
            end
          end
          if tran
            Thread::pass if rand(10) == 0
            if !db.end_transaction(rand(10) > 0)
              dberrprint(db, "DB::end_transaction")
              err = true
            end
          end
          if id < 1 && rnum > 250 && i % (rnum / 250) == 0
            print(".")
            if i == rnum || i % (rnum / 10) == 0
              printf(" (%08d)\n", i)
            end
          end
        end
        cur.disable if rand(2) == 0
      }
      threads.push(th)
    end
    threads.each { |jth|
      jth.join
    }
    dbmetaprint(db, itcnt == itnum)
    if !db.close
      dberrprint(db, "DB::close")
      err = true
    end
    etime = Time::now
    printf("time: %.3f\n", etime - stime)
  end
  printf("%s\n\n", err ? "error" : "ok")
  return err ? 1 : 0
end


# perform misc command
def procmisc(path)
  printf("<Miscellaneous Test>\n  path=%s\n\n", path)
  err = false
  if conv_str(:mikio) != "mikio" || conv_str(123.45) != "123.45"
    printf("%s: conv_str: error\n", $progname)
    err = true
  end
  printf("calling utility functions:\n")
  atoi("123.456mikio")
  atoix("123.456mikio")
  atof("123.456mikio")
  hash_murmur(path)
  hash_fnv(path)
  levdist(path, "casket")
  dcurs = Array::new
  printf("opening the database by iterator:\n")
  dberr = DB::process(path, DB::OWRITER | DB::OCREATE | DB::OTRUNCATE) { |db|
    db.tune_exception_rule([ Error::SUCCESS, Error::NOIMPL, Error::MISC ])
    db.tune_encoding("utf-8")
    db.to_s
    db.inspect
    rnum = 10000
    printf("setting records:\n")
    for i in 0...rnum
      db[i] = i
    end
    if db.count != rnum
      dberrprint(db, "DB::count")
      err = true
    end
    printf("deploying cursors:\n")
    for i in 1..100
      cur = db.cursor
      if !cur.jump(i)
        dberrprint(db, "Cursor::jump")
        err = true
      end
      case i % 3
      when 0
        dcurs.push(cur)
      when 1
        cur.disable
      end
      cur.to_s
      cur.inspect
    end
    printf("getting records:\n")
    dcurs.each { |tcur|
      if !tcur.get_key
        dberrprint(db, "Cursor::get_key")
        err = true
      end
    }
    printf("accepting visitor:\n")
    def db.visit_full(key, value)
      rv = Visitor::NOP
      case key.to_i % 3
      when 0
        rv = sprintf("full:%s", key)
      when 1
        rv = Visitor::REMOVE
      end
      return rv
    end
    def db.visit_empty(key)
      rv = Visitor::NOP
      case key.to_i % 3
      when 0
        rv = sprintf("empty:%s", key)
      when 1
        rv = Visitor::REMOVE
      end
      return rv
    end
    for i in 0...(rnum * 2)
      if !db.accept(i, db, true)
        dberrprint(db, "DB::access")
        err = true
      end
    end
    for i in 0...(rnum * 2)
      if !db.accept(i) { |key, value|
          rv = Visitor::NOP
          case key.to_i % 5
          when 0
            rv = sprintf("block:%s", key)
          when 1
            rv = Visitor::REMOVE
          end
          rv
        }
        dberrprint(db, "DB::access")
        err = true
      end
    end
    printf("accepting visitor by iterator:\n")
    def dcurs.visit_full(key, value)
      Visitor::NOP
    end
    if !db.iterate(dcurs, false)
      dberrprint(db, "DB::iterate")
      err = true
    end
    if !db.iterate { |key, value|
        value.upcase
      }
      dberrprint(db, "DB::iterate")
      err = true
    end
    printf("accepting visitor with a cursor:\n")
    cur = db.cursor
    def cur.visit_full(key, value)
      rv = Visitor::NOP
      case key.to_i % 7
      when 0
        rv = sprintf("cur:full:%s", key)
      when 1
        rv = Visitor::REMOVE
      end
      return rv
    end
    begin
      if !cur.jump_back
        dberrprint(db, "Cursor::jump_back")
        err = true
      end
      while cur.accept(cur, true)
        cur.step_back
      end
    rescue Error::XNOIMPL
      if !cur.jump
        dberrprint(db, "Cursor::jump")
        err = true
      end
      while cur.accept(cur, true)
        cur.step
      end
    end
    cur.jump
    while cur.accept { |key, value|
        rv = Visitor::NOP
        case key.to_i % 11
        when 0
          rv = sprintf("cur:block:%s", key)
        when 1
          rv = Visitor::REMOVE
        end
        rv
      }
      cur.step
    end
    printf("accepting visitor in bulk:\n")
    keys = []
    for i in 1..10
      keys.push(i)
    end
    if not db.accept_bulk(keys, db, true)
      dberrprint(db, "DB::accept_bulk")
      err = true
    end
    recs = {}
    for i in 1..10
      recs[i] = sprintf("[%d]", i)
    end
    if db.set_bulk(recs) < 0
      dberrprint(db, "DB::set_bulk")
      err = true
    end
    if not db.get_bulk(keys)
      dberrprint(db, "DB::get_bulk")
      err = true
    end
    if db.remove_bulk(keys) < 0
      dberrprint(db, "DB::remove_bulk")
      err = true
    end
    printf("synchronizing the database:\n")
    def db.process(path, count, size)
      true
    end
    if !db.synchronize(false, db)
      dberrprint(db, "DB::synchronize")
      err = true
    end
    if !db.synchronize { |tpath, count, size|
        true
      }
      dberrprint(db, "DB::synchronize")
      err = true
    end
    if !db.occupy(false, db)
      dberrprint(db, "DB::occupy")
      err = true
    end
    if !db.occupy { |tpath, count, size|
        true
      }
      dberrprint(db, "DB::occupy")
      err = true
    end
    printf("performing transaction:\n")
    if !db.transaction {
        db["tako"] = "ika"
        true
      }
      dberrprint(db, "DB::transaction")
      err = true
    end
    if db["tako"] != "ika"
      dberrprint(db, "DB::transaction")
      err = true
    end
    db.delete("tako")
    cnt = db.count
    if !db.transaction {
        db["tako"] = "ika"
        db["kani"] = "ebi"
        false
      }
      dberrprint(db, "DB::transaction")
      err = true
    end
    if db["tako"] || db["kani"] || db.count != cnt
      dberrprint(db, "DB::transaction")
      err = true
    end
    printf("closing the database:\n")
  }
  if dberr
    printf("%s: DB::process: %s\n", $progname, dberr)
    err = true
  end
  printf("accessing dead cursors:\n")
  dcurs.each { |cur|
    cur.get_key
  }
  printf("checking the exceptional mode:\n")
  db = DB::new(DB::GEXCEPTIONAL)
  begin
    db.open("hoge")
  rescue Error::XINVALID => e
    if e.code != Error::INVALID
      dberrprint(db, "DB::open")
      err = true
    end
  else
    dberrprint(db, "DB::open")
    err = true
  end
  printf("re-opening the database as a reader:\n")
  db = DB::new
  if !db.open(path, DB::OREADER)
    dberrprint(db, "DB::open")
    err = true
  end
  printf("traversing records by iterator:\n")
  keys = Array::new
  db.each { |key, value|
    keys.push([key, value])
    if !value.index(key)
      dberrprint(db, "Cursor::each")
      err = true
    end
  }
  printf("checking records:\n")
  keys.each { |pair|
    if db.get(pair[0]) != pair[1]
      dberrprint(db, "DB::get")
      err = true
    end
  }
  printf("closing the database:\n")
  if !db.close
    dberrprint(db, "DB::close")
    err = true
  end
  printf("re-opening the database in the concurrent mode:\n")
  db = DB::new(DB::GCONCURRENT)
  if !db.open(path, DB::OWRITER)
    dberrprint(db, "DB::open")
    err = true
  end
  if !db.set("tako", "ika")
    dberrprint(db, "DB::set")
    err = true
  end
  if db.accept("tako") { |key, value| } != nil || db.error != Error::INVALID
    dberrprint(db, "DB::accept")
    err = true
  end
  printf("removing records by cursor:\n")
  cur = db.cursor
  if !cur.jump
    dberrprint(db, "Cursor::jump")
    err = true
  end
  cnt = 0
  while key = cur.get_key(true)
    if cnt % 10 != 0
      if !db.remove(key)
        dberrprint(db, "DB::remove")
        err = true
      end
    end
    cnt += 1
  end
  if db.error != Error::NOREC
    dberrprint(db, "Cursor::get_key")
    err = true
  end
  cur.disable
  printf("processing a cursor by iterator:\n")
  db.cursor_process { |tcur|
    if !tcur.jump
      dberrprint(db, "Cursor::jump")
      err = true
    end
    value = sprintf("[%s]", tcur.get_value)
    if !tcur.set_value(value)
      dberrprint(db, "Cursor::set_value")
      err = true
    end
    if tcur.get_value != value
      dberrprint(db, "Cursor::get_value")
      err = true
    end
  }
  printf("dumping records into snapshot:\n")
  snappath = db.path
  if snappath =~ /.(kch|kct)$/
    snappath = snappath + ".kcss"
  else
    snappath = "kctest.kcss"
  end
  if !db.dump_snapshot(snappath)
    dberrprint(db, "DB::dump_snapshot")
    err = true
  end
  cnt = db.count
  printf("clearing the database:\n")
  if !db.clear
    dberrprint(db, "DB::clear")
    err = true
  end
  printf("loading records from snapshot:\n")
  if !db.load_snapshot(snappath)
    dberrprint(db, "DB::load_snapshot")
    err = true
  end
  if db.count != cnt
    dberrprint(db, "DB::load_snapshot")
    err = true
  end
  File::unlink(snappath)
  copypath = db.path
  suffix = nil
  if copypath.end_with?(".kch")
    suffix = ".kch"
  elsif copypath.end_with?(".kct")
    suffix = ".kct"
  elsif copypath.end_with?(".kcd")
    suffix = ".kcd"
  elsif copypath.end_with?(".kcf")
    suffix = ".kcf"
  end
  if suffix
    printf("performing copy and merge:\n")
    copypaths = []
    for i in 0...2
      copypaths.push(sprintf("%s.%d%s", copypath, i + 1, suffix))
    end
    srcary = []
    copypaths.each do |cpath|
      if !db.copy(cpath)
        dberrprint(db, "DB::copy")
        err = true
      end
      srcdb = DB::new
      if !srcdb.open(cpath, DB::OREADER)
        dberrprint(srcdb, "DB::open")
        err = true
      end
      srcary.push(srcdb)
    end
    if !db.merge(srcary, DB::MAPPEND)
      dberrprint(srcdb, "DB::merge")
      err = true
    end
    srcary.each do |srcdb|
      if !srcdb.close
        dberrprint(srcdb, "DB::close")
        err = true
      end
    end
    copypaths.each do |cpath|
      FileUtils::remove_entry_secure(cpath, true)
    end
  end
  printf("shifting records:\n")
  ocnt = db.count
  cnt = 0
  while db.shift
    cnt += 1
  end
  if db.error != Error::NOREC
    dberrprint(db, "DB::shift")
    err = true
  end
  if db.count != 0 || cnt != ocnt
    dberrprint(db, "DB::shift")
    err = true
  end
  printf("closing the database:\n")
  if !db.close
    dberrprint(db, "DB::close")
    err = true
  end
  db.to_s
  db.inspect
  printf("%s\n\n", err ? "error" : "ok")
  return err ? 1 : 0
end


# execute main
STDOUT.sync = true
$progname = $0.dup
$progname.gsub!(/.*\//, "")
srand
exit(main)



# END OF FILE
