module Drizzle
  extend FFI::Library
  ffi_lib "libdrizzle.dylib"

  class DrizzleException < RuntimeError; end

  enum :drizzle_return_t, [:DRIZZLE_RETURN_OK,
    :DRIZZLE_RETURN_IO_WAIT,
    :DRIZZLE_RETURN_PAUSE,
    :DRIZZLE_RETURN_ROW_BREAK,
    :DRIZZLE_RETURN_MEMORY,
    :DRIZZLE_RETURN_ERRNO,
    :DRIZZLE_RETURN_INTERNAL_ERROR,
    :DRIZZLE_RETURN_GETADDRINFO,
    :DRIZZLE_RETURN_NOT_READY,
    :DRIZZLE_RETURN_BAD_PACKET_NUMBER,
    :DRIZZLE_RETURN_BAD_HANDSHAKE_PACKET,
    :DRIZZLE_RETURN_BAD_PACKET,
    :DRIZZLE_RETURN_PROTOCOL_NOT_SUPPORTED,
    :DRIZZLE_RETURN_UNEXPECTED_DATA,
    :DRIZZLE_RETURN_NO_SCRAMBLE,
    :DRIZZLE_RETURN_AUTH_FAILED,
    :DRIZZLE_RETURN_NULL_SIZE,
    :DRIZZLE_RETURN_ERROR_CODE,
    :DRIZZLE_RETURN_TOO_MANY_COLUMNS,
    :DRIZZLE_RETURN_ROW_END,
    :DRIZZLE_RETURN_EOF,
    :DRIZZLE_RETURN_COULD_NOT_CONNECT,
    :DRIZZLE_RETURN_NO_ACTIVE_CONNECTIONS,
    :DRIZZLE_RETURN_SERVER_GONE,
    :DRIZZLE_RETURN_MAX
  ]

  enum :drizzle_con_status, [
    :DRIZZLE_CON_NONE,             0,
    :DRIZZLE_CON_ALLOCATED,        (1 << 0),
    :DRIZZLE_CON_MYSQL,            (1 << 1),
    :DRIZZLE_CON_RAW_PACKET,       (1 << 2),
    :DRIZZLE_CON_RAW_SCRAMBLE,     (1 << 3),
    :DRIZZLE_CON_READY,            (1 << 4),
    :DRIZZLE_CON_NO_RESULT_READ,   (1 << 5)
  ]

  enum :drizzle_con_options, [
    :DRIZZLE_CON_NONE,             0,
    :DRIZZLE_CON_ALLOCATED,        (1 << 0),
    :DRIZZLE_CON_MYSQL,            (1 << 1),
    :DRIZZLE_CON_RAW_PACKET,       (1 << 2),
    :DRIZZLE_CON_RAW_SCRAMBLE,     (1 << 3),
    :DRIZZLE_CON_READY,            (1 << 4),
    :DRIZZLE_CON_NO_RESULT_READ,   (1 << 5)
  ]

  def self.return_codes
    enum_type(:drizzle_return_t)
  end

  def self.con_options
    enum_type(:drizzle_con_options)
  end

  def self.con_status
    enum_type(:drizzle_con_status)
  end

  # Misc
  attach_function :version,               :drizzle_version,                   [],                                       :string

  # Drizzle objects
  attach_function :create,                :drizzle_create,                    [:pointer],                               :pointer
  attach_function :free,                  :drizzle_free,                      [:pointer],                               :void
  attach_function :error,                 :drizzle_error,                     [:pointer],                               :string

  # Connection objects
  attach_function :con_create,            :drizzle_con_create,                [:pointer, :pointer],                     :pointer
  attach_function :con_free,              :drizzle_con_free,                  [:pointer],                               :void
  attach_function :con_set_db,            :drizzle_con_set_db,                [:pointer, :string],                      :void
  attach_function :con_set_auth,          :drizzle_con_set_auth,              [:pointer, :string, :string],             :void
  attach_function :con_add_options,       :drizzle_con_add_options,           [:pointer, :drizzle_con_options],         :void

  # Querying
  attach_function :query_str,             :drizzle_query_str,                 [:pointer, :pointer, :string, :pointer],  :pointer

  # Results
  attach_function :result_buffer,         :drizzle_result_buffer,             [:pointer],                               :int
  attach_function :result_free,           :drizzle_result_free,               [:pointer],                               :drizzle_return_t
  attach_function :result_affected_rows,  :drizzle_result_affected_rows,      [:pointer],                               :uint64  
  attach_function :result_insert_id,      :drizzle_result_insert_id,          [:pointer],                               :uint64
  attach_function :row_next,              :drizzle_row_next,                  [:pointer],                               :pointer

  # Columns
  attach_function :column_next,           :drizzle_column_next,               [:pointer],                               :pointer
  attach_function :column_name,           :drizzle_column_name,               [:pointer],                               :string

  class Result < FFI::AutoPointer
    attr_reader :columns, :affected_rows, :insert_id

    def initialize(ptr)
      super(ptr)
      @rows, @columns, @rowptrs = [], [], []

      while (!(column = Drizzle.column_next(self)).null?)
        @columns << Drizzle.column_name(column).to_sym
      end

      @insert_id = Drizzle.result_insert_id(self)
      @affected_rows = Drizzle.result_affected_rows(self)
    end

    def each
      if @rows.empty?
        while (!(row = Drizzle.row_next(self)).null?)
          @rowptrs << row.read_array_of_pointer(@columns.size)
        end
        @rowptrs.each do |rowptr|
          row = []
          @columns.size.times do |i|
            row << rowptr[i].get_string(0)
          end
          yield row if block_given?
          @rows << row
        end
      else
        @rows.each do |row|
          yield row if block_given?
        end
      end
    end

    def self.release(obj)
      Drizzle.result_free(obj)
    end

  end

  class Connection < FFI::AutoPointer
    def initialize(host, user, pass, db, proto=:DRIZZLE_CON_MYSQL)
      @host, @user, @pass, @db, @proto = host, user, pass, db, proto
      @drizzle = Drizzle.create(nil)
      @conn = Drizzle.con_create(@drizzle, nil)
      Drizzle.con_add_options(@conn, Drizzle.enum_value(proto))
      Drizzle.con_set_auth(@conn, @user, @pass)
      Drizzle.con_set_db(@conn, @db)
    end

    def query(query)
      ret = FFI::MemoryPointer.new(:int)
      result = Drizzle.query_str(@conn, nil, query, ret)
      if Drizzle.return_codes[ret.get_int(0)] != :DRIZZLE_RETURN_OK
        raise DrizzleException.new("Query failed: #{Drizzle.error(@drizzle)}")
      end
      ret = Drizzle.result_buffer(result)
      if Drizzle.return_codes[ret] != :DRIZZLE_RETURN_OK
        raise DrizzleException.new("Query failed: #{Drizzle.error(@drizzle)}")
      end

      Result.new(result)
    end

    def self.release(obj)
      Drizzle.con_free(obj.instance_variable_get("@conn"))
      Drizzle.free(obj.instance_variable_get("@drizzle"))
    end
  end

end