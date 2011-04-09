module Drizzle
  extend FFI::Library
  ffi_lib "drizzle"

  class DrizzleException < RuntimeError; end

  enum :drizzle_return_t, [
    :DRIZZLE_RETURN_OK,
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

  enum :drizzle_con_options_t, [
    :DRIZZLE_CON_NONE,             0,
    :DRIZZLE_CON_ALLOCATED,        (1 << 0),
    :DRIZZLE_CON_MYSQL,            (1 << 1),
    :DRIZZLE_CON_RAW_PACKET,       (1 << 2),
    :DRIZZLE_CON_RAW_SCRAMBLE,     (1 << 3),
    :DRIZZLE_CON_READY,            (1 << 4),
    :DRIZZLE_CON_NO_RESULT_READ,   (1 << 5)
  ]

  enum :drizzle_options_t, [
    :DRIZZLE_NONE,         0,
    :DRIZZLE_ALLOCATED,    (1 << 0),
    :DRIZZLE_NON_BLOCKING, (1 << 1)
  ]

  def self.options
    enum_type(:drizzle_options_t)
  end

  def self.return_codes
    enum_type(:drizzle_return_t)
  end

  def self.con_options
    enum_type(:drizzle_con_options_t)
  end

  # Misc
  attach_function :version,               :drizzle_version,                   [],                                       :string

  # Drizzle objects
  attach_function :create,                :drizzle_create,                    [:pointer],                               :pointer
  attach_function :free,                  :drizzle_free,                      [:pointer],                               :void
  attach_function :error,                 :drizzle_error,                     [:pointer],                               :string
  attach_function :add_options,           :drizzle_add_options,               [:pointer, :drizzle_options_t],           :void

  # Connection objects
  attach_function :con_create,            :drizzle_con_create,                [:pointer, :pointer],                     :pointer
  attach_function :con_free,              :drizzle_con_free,                  [:pointer],                               :void
  attach_function :con_set_db,            :drizzle_con_set_db,                [:pointer, :string],                      :void
  attach_function :con_set_auth,          :drizzle_con_set_auth,              [:pointer, :string, :string],             :void
  attach_function :con_add_options,       :drizzle_con_add_options,           [:pointer, :drizzle_con_options_t],       :void
  attach_function :con_status,            :drizzle_con_status,                [:pointer],                               :int
  attach_function :con_fd,                :drizzle_con_fd,                    [:pointer],                               :int
  attach_function :con_clone,             :drizzle_con_clone,                 [:pointer, :pointer, :pointer],           :pointer
  attach_function :con_set_tcp,           :drizzle_con_set_tcp,               [:pointer, :string, :int],                :void

  # Querying
  attach_function :query_str,             :drizzle_query_str,                 [:pointer, :pointer, :string, :pointer],  :pointer

  # Results
  attach_function :result_create,         :drizzle_result_create,             [:pointer, :pointer],                     :pointer
  attach_function :result_buffer,         :drizzle_result_buffer,             [:pointer],                               :int
  attach_function :result_free,           :drizzle_result_free,               [:pointer],                               :drizzle_return_t
  attach_function :result_affected_rows,  :drizzle_result_affected_rows,      [:pointer],                               :uint64  
  attach_function :result_insert_id,      :drizzle_result_insert_id,          [:pointer],                               :uint64
  attach_function :row_next,              :drizzle_row_next,                  [:pointer],                               :pointer
  attach_function :result_read,           :drizzle_result_read,               [:pointer, :pointer, :pointer],           :pointer

  # Columns
  attach_function :column_next,           :drizzle_column_next,               [:pointer],                               :pointer
  attach_function :column_name,           :drizzle_column_name,               [:pointer],                               :string

  class Result
    attr_reader :columns, :affected_rows, :insert_id, :rows

    def initialize(ptr)
      @columns, @rows = [], []

      @insert_id = Drizzle.result_insert_id(ptr)
      @affected_rows = Drizzle.result_affected_rows(ptr)

      # Get columns
      until (column = Drizzle.column_next(ptr)).null?
        @columns << Drizzle.column_name(column).to_sym
      end

      # Get rows
      until (row = Drizzle.row_next(ptr)).null?
        @rows << row.get_array_of_string(0, @columns.size)
      end

      # Free the underlying buffers since we just copied it all to Ruby
      Drizzle.result_free(ptr)
    end

    def each
      @rows.each do |row|
        yield row if block_given?
      end
    end
  end
  
  class Drizzleptr < FFI::AutoPointer
    def self.release(ptr)
      Drizzle.free(ptr)
    end
  end
  
  class Connptr < FFI::AutoPointer
    def self.release(ptr)
      Drizzle.con_free(ptr)
    end
  end

  class Connection
    attr_accessor :host, :user, :pass, :db, :opts, :fd

    def initialize(host, user, pass, db=nil, opts=[], drizzle=nil)
      opts = opts.is_a?(Array) ? opts : [opts]
      @host, @user, @pass, @db, @opts = host, user, pass, db, opts
      @from_pool = true if drizzle
      @drizzle = drizzle || Drizzleptr.new(Drizzle.create(nil))
      @conn = Connptr.new(Drizzle.con_create(@drizzle, nil))
      Drizzle.con_add_options(@conn, opts.inject(0){|i,o| i | Drizzle.enum_value(o)} | Drizzle.enum_value(:DRIZZLE_CON_NO_RESULT_READ))
      Drizzle.con_set_auth(@conn, @user, @pass)
      Drizzle.con_set_tcp(@conn, @host, (opts.include?(:DRIZZLE_CON_MYSQL) ? 3306 : 4427))
      Drizzle.con_set_db(@conn, @db) if @db
      @retptr = FFI::MemoryPointer.new(:int)
    end

    # Indicates whether or not this connection was created using a drizzle_st object from somewhere else.
    def from_pool?
      @from_pool
    end

    # This executes a normal synchronous query. We simply call the async methods together.
    def query(query, proc=nil, &blk)
      proc ||= blk
      async_query(query, proc)
      async_result
    end

    # Sends off a query to the server. The return value is the file descriptor number of the socket used for this connection, for monitoring with an event loop etc.
    def async_query(query, proc=nil, &blk)
      proc ||= blk
      Drizzle.query_str(@conn, nil, query, @retptr)
      # Make sure it was successful
      check_error
      @callback = proc
      # return fd to caller
      @fd ||= Drizzle.con_fd(@conn)
    end

    # Do a blocking read for the result of an outstanding query. This results the Result object as well as fires a callback associated with it.
    def async_result
      # Do a partial blocking read into the the packet struct
      result = Drizzle.result_read(@conn, nil, @retptr)

      # See if the read was successful
      check_error

      # Buffer the result and check
      ret = Drizzle.result_buffer(result)
      if Drizzle.return_codes[ret] != :DRIZZLE_RETURN_OK
        # Free the result struct if we fail.
        Drizzle.result_free(result)
        raise DrizzleException.new("Query failed: #{Drizzle.error(@drizzle)}")
      end

      # Fire and return
      r = Result.new(result)
      @callback.call(r) if @callback
      @callback = nil
      r
    end

    def em_query(query, proc=nil, &blk)
      proc ||= blk
      fd = async_query(query, proc)
      EM.watch(fd, EMHandler, self) {|c| c.notify_readable = true}
    end

    def check_error
      if Drizzle.return_codes[@retptr.get_int(0)] != :DRIZZLE_RETURN_OK
        raise DrizzleException.new("Query failed: #{Drizzle.error(@drizzle)}")
      end
    end

  end

  module EMHandler
    def initialize(conn)
      @conn = conn
    end
    def notify_readable
      detach
      @conn.async_result
    end
  end

end