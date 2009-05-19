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
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
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

    result.columns.size.should.equal 2
    result.columns.should.include :table_schema
    result.columns.should.include :table_name
  end

  it "send and receive a query asynchronously" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
    fd = c.async_query("SELECT table_schema,table_name FROM tables")

    result = c.async_result(fd)
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

    result.columns.size.should.equal 2
    result.columns.should.include :table_schema
    result.columns.should.include :table_name
  end

  it "send and receive a query asynchronously using a callback" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
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

    c.async_result(fd)

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
        c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
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

  it "automatically build up and maintain the connection pool as needed" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
    fds = []
    5.times do
      fds << c.async_query("select now()")
    end
    c.instance_variable_get("@ready_cons").size.should.equal 0
    c.instance_variable_get("@busy_cons").size.should.equal 5
    fds.each do |fd|
      c.async_result(fd).class.should.equal Drizzle::Result
    end
    c.instance_variable_get("@ready_cons").size.should.equal 5
    c.instance_variable_get("@busy_cons").size.should.equal 0
  end

  it "connection pool should not allocate connections in excess of maximum connection limit" do
    c = Drizzle::Connection.new(HOST, USER, PASS, "information_schema")
    c.max_cons = 10
    fds = []
    10.times do
      fds << c.async_query("select now()")
    end

    # Fill
    c.instance_variable_get("@ready_cons").size.should.equal 0
    c.instance_variable_get("@busy_cons").size.should.equal 10

    # Attempt to over-fill
    c.async_query("select now()").should.equal nil

    c.instance_variable_get("@ready_cons").size.should.equal 0
    c.instance_variable_get("@busy_cons").size.should.equal 10

    # Drain
    fds.each do |fd|
      c.async_result(fd).class.should.equal Drizzle::Result
    end

    c.instance_variable_get("@ready_cons").size.should.equal 10
    c.instance_variable_get("@busy_cons").size.should.equal 0
  end

end
