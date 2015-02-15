module Fluent

class PgJsonOutput < Fluent::BufferedOutput
  Fluent::Plugin.register_output("pgjson", self)

  config_param :host       , :string  , default: "localhost"
  config_param :port       , :integer , default: 5432
  config_param :sslmode    , :string  , default: "prefer"
  config_param :database   , :string
  config_param :table      , :string
  config_param :user       , :string  , default: nil
  config_param :password   , :string  , default: nil
  config_param :time_col   , :string  , default: "time"
  config_param :tag_col    , :string  , default: "tag"
  config_param :record_col , :string  , default: "record"
  config_param :msgpack    , :bool    , default: false

  def initialize
    super
    require "pg"
    @conn = nil
  end

  def configure(conf)
    super
  end

  def shutdown
    super

    return if !conn.nil? && !conn.finished?
    conn.close
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def write(chunk)
    conn.exec(copy_cmd)
    begin
      chunk.msgpack_each { |tag, time, record| put_copy_data(tag, time, record) }
    rescue => err
      handle_error(err)
    else
      finish_write
    end
  end

  private

  def finish_write
    conn.put_copy_end
    res = conn.get_result
    raise res.result_error_message if res.result_status != PG::PGRES_COMMAND_OK
  end

  def handle_error(err)
    errmsg = sprintf("%s while copy data: %s", err.class.name, err.message)
    conn.put_copy_end(errmsg)
    conn.get_result
    raise
  end

  def copy_cmd
    "COPY #{@table} (#{@tag_col}, #{@time_col}, #{@record_col}) FROM STDIN WITH DELIMITER E'\\x01'"
  end

  def copy_data(tag, time, record)
    "#{tag}\x01#{Time.at(time)}\x01#{record_value(record)}\n"
  end

  def put_copy_data(tag, time, record)
    conn.put_copy_data copy_data(tag, time, record)
  end

  def conn
    return @conn unless @conn.nil?

    log.debug "connecting to PostgreSQL server #{@host}:#{@port}, database #{@database}..."
    begin
      @conn = PGconn.new(dbname: @database, host: @host, port: @port, sslmode: @sslmode, user: @user, password: @password)
    rescue
      unless @conn.nil?
        @conn.close
        @conn = nil
      end
      raise "failed to initialize connection: #{$ERROR_INFO}"
    end
  end

  def record_value(record)
    if @msgpack
      "\\#{conn.escape_bytea(record.to_msgpack)}"
    else
      json = record.to_json
      json.gsub!(/\\/) { '\\\\' }
      json
    end
  end
end

end
