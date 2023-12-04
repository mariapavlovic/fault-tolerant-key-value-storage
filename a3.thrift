# service KeyValueService {
#   string get(1: string key) throws (1: org.apache.thrift.TException e);
#   void put(1: string key, 2: string value) throws (1: org.apache.thrift.TException e);
#   bool ping() throws (1: org.apache.thrift.TException e);
#   void forwardRequest(1: string key, 2: string value, 3: i32 sequence);
#   void completeDataTransfer(1: string host, 2: i32 port) throws (1: org.apache.thrift.TException e);
#   void setMap(1: list<string> keys, 2: list<string> values);
# }

service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  bool ping();
  void forwardRequest(1: string key, 2: string value, 3: i32 sequence);
  void completeDataTransfer(1: string host, 2: i32 port);
  void setMap(1: list<string> keys, 2: list<string> values);
}
