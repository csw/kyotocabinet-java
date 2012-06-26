require 'spec_helper'

module KyotoCabinet

  describe DB do

    it "can create an in-memory tree DB" do
      db = DB.new
      db.open("%", KyotoCabinet::DB::OWRITER | KyotoCabinet::DB::OCREATE)
      db["hello"] = "world"
      db.count.should == 1
    end

  end

end
