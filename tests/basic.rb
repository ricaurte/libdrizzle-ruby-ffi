require 'set'

describe "Basic libdrizzle operation" do

  it "should perform a simple query on a MySQL server" do
    c = Drizzle::Connection.new("127.0.0.1", "root", "password", "information_schema")
    result = c.query("SELECT table_schema,table_name FROM tables")
    result.class.should.equal Drizzle::Result
    schemas, tables = Set.new, Set.new
    result.each do |row|
      schemas << row[0]
      tables << row[1]
    end
    schemas.should.include "information_schema"
    tables.should.include "COLUMNS"

    result.affected_rows.should.equal 0
    result.insert_id.should.equal 0
  end

end
