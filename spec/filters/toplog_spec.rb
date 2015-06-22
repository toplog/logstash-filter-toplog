require 'spec_helper'
require "logstash/filters/toplog"

describe LogStash::Filters::Toplog do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        toplog {
          message => "82.80.216.6 - - [22/Jun/2015:17:24:16 +0000] \"GET / HTTP/1.1\" 302 1144","@version":"1","@timestamp":"2015-06-22T17:26:20.850+00:00"
          default_source_location => "0"
          timestamp_format => "[%d/%b/%Y:%H:%M:%S %z]"
          timestamp_pattern => "\\[([^:]+):(\\d+:\\d+:\\d+) ([^\\]]+)\\]"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject).to include("message")
      expect(subject['toplog_timestamp']).to eq('1434993856000')
      expect(subject['default_source']).to eq('82.80.216.6')
      expect(subject['toplog_columns']).to eq({"0":"82.80.216.6","1":"-","2":"-","3":"[22/Jun/2015:17:24:16 +0000]","4":"\"GET","5":"/","6":"HTTP/1.1\"","7":"302","8":"1144")
    end
  end
end


# "message":"82.80.216.6 - - [22/Jun/2015:17:24:16 +0000] \"GET / HTTP/1.1\" 302 1144","@version":"1","@timestamp":"2015-06-22T17:26:20.850+00:00","type":"automatic","host":"toplog","installer_version":"0.5.0","log_type":"SysLog","user_id":"43","stream_id":"311","toplog_timestamp":"1434993856000","toplog_columns":{"0":"82.80.216.6","1":"-","2":"-","3":"[22/Jun/2015:17:24:16 +0000]","4":"\"GET","5":"/","6":"HTTP/1.1\"","7":"302","8":"1144"},"default_source":"82.80.216.6","random":"0.46632965638456625","logstash_checksum":"c903679820e516514c89381b06c1acbc"
