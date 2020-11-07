service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void forwardMap(1: map<string, string> mapFromExistedSever);
}
