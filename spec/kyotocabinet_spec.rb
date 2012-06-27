require 'spec_helper'

module KyotoCabinet

  describe DB do

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
      end

      describe "#get" do
        it "returns nil properly" do
          @db.get("foo").should be_nil
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

      after(:each) do
        @db.close
      end
    end

  end

end
