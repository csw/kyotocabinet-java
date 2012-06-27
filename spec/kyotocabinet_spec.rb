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
      end

      it "returns nil from get properly" do
        @db.get("foo").should be_nil
      end

      after(:each) do
        @db.close
      end
    end

  end

end
