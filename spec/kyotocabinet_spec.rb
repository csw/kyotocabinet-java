require 'spec_helper'

module KyotoCabinet

  class TestVisitor
    attr_reader :empty
    attr_reader :full

    def visit_empty(key)
      @empty = true
    end

    def visit_full(key, value)
      @full = value
    end
  end

  describe DB do

    BIN = "\x01\xd6"

    it "can create an in-memory tree DB" do
      db = DB.new
      db.open("%", KyotoCabinet::DB::OWRITER | KyotoCabinet::DB::OCREATE)
      db["hello"] = "world"
      db.count.should == 1
    end

    describe "with in-memory DB" do
      before(:each) do
        @db = DB.new
        @db.open("%", KyotoCabinet::DB::OWRITER | KyotoCabinet::DB::OCREATE)
        @db["hello"] = "world"
        @db["int_a"] = [0].pack("Q>")
        @db[BIN] = BIN
      end

      describe "#accept" do
        it "works" do
          v = TestVisitor.new
          @db.accept("hello", v, true)
          v.full.should == "world"
        end
      end

      describe "#accept_bulk" do
        it "works with one item" do
          v = TestVisitor.new
          @db.accept_bulk(["hello"], v, true)
          v.full.should == "world"
        end
      end

      describe "#add" do
        it "adds an entry" do
          @db.add("a", BIN)
          @db['a'].should == BIN
        end
      end

      describe "#append" do
        it "works" do
          @db.append("hello", "wide").should be_true
          @db["hello"].should == "worldwide"
        end
      end

      describe "#cas" do
        it "works on existing values" do
          @db.cas("hello", "world", BIN).should be_true
          @db["hello"].should == BIN
        end
      end

      describe "#check" do
        it "works" do
          @db.check(BIN).should == BIN.size
        end
      end

      describe "#get" do
        it "returns nil properly" do
          @db.get("foo").should be_nil
        end
      end

      describe "#get_bulk" do
        it "works on multiple records" do
          @db.get_bulk(["hello", BIN]).should == {
            'hello' => "world",
            BIN => BIN
          }
        end
      end

      describe "#increment" do
        it "creates a value with one arg" do
          @db.increment("int_z")
          @db["int_z"].should_not be_nil
        end

        it "increments with two args" do
          @db.increment("int_a", 1)
          @db["int_a"].unpack("Q>")[0].should == 1
        end
      end

      describe "#increment_double" do
        it "creates a value with one arg" do
          @db.increment_double("double_z")
          @db["double_z"].should_not be_nil
        end

        it "increments with two args" do
          @db.increment_double("double_a", 1.5).should == 1.5
        end
      end

      describe "#iterate" do
        it "works" do
          
        end
      end

      describe "#seize" do
        it "returns the correct value" do
          @db.seize("hello").should == "world"
        end
      end

      after(:each) do
        @db.close
      end
    end

  end

end
