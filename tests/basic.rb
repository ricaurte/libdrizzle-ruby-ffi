require 'set'
begin
  require 'eventmachine'
rescue LoadError
end

HOST = "127.0.0.1"
USER = "root"
PASS = "password"

describe "Basic libdrizzle operation" do

  it "perform a simple synchronous query on a MySQL server" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema", :DRIZZLE_CON_MYSQL)
    schemas, tables = Set.new, Set.new
    result = c.query("SELECT table_schema,table_name FROM tables")
    result.class.should.equal Drizzle::Result
    result.affected_rows.should.equal 0
    result.insert_id.should.equal 0
    result.columns.size.should.equal 2
    result.columns.should.include :table_schema
    result.columns.should.include :table_name

    result.each do |row|
      schemas << row[0]
      tables << row[1]
    end

    schemas.should.include "information_schema"
    tables.should.include "COLUMNS"
  end

  it "send and receive a query asynchronously" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema", :DRIZZLE_CON_MYSQL)
    fd = c.async_query("SELECT table_schema,table_name FROM tables")

    schemas, tables = Set.new, Set.new
    result = c.async_result
    result.class.should.equal Drizzle::Result
    result.affected_rows.should.equal 0
    result.insert_id.should.equal 0

    result.columns.size.should.equal 2
    result.columns.should.include :table_schema
    result.columns.should.include :table_name

    result.each do |row|
      schemas << row[0]
      tables << row[1]
    end

    schemas.should.include "information_schema"
    tables.should.include "COLUMNS"
  end

  it "send and receive a query asynchronously using a callback" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema", :DRIZZLE_CON_MYSQL)
    schemas, tables = Set.new, Set.new
    columns = []
    fd = c.async_query("SELECT table_schema,table_name FROM tables") do |result|
      result.columns.each {|col| columns << col}
      result.each do |row|
        schemas << row[0]
        tables << row[1]
      end
    end

    schemas.should.be.empty
    tables.should.be.empty
    columns.should.be.empty

    c.async_result

    schemas.should.include "information_schema"
    tables.should.include "COLUMNS"
    columns.size.should.equal 2
    columns.should.include :table_schema
    columns.should.include :table_name
  end

  if defined?(EventMachine)
    it "perform an async query with evented receive using EventMachine" do
      schemas, tables = Set.new, Set.new
      columns = []
      EM.run {
        c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema", :DRIZZLE_CON_MYSQL)
        c.em_query("SELECT table_schema,table_name FROM tables") do |result|
          result.columns.each {|col| columns << col}
          result.each do |row|
            schemas << row[0]
            tables << row[1]
          end
          EM.stop
        end
      }
      schemas.should.include "information_schema"
      tables.should.include "COLUMNS"
      columns.size.should.equal 2
      columns.should.include :table_schema
      columns.should.include :table_name
    end
  end

end
