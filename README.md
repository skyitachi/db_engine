### poor man's leveldb

#### single thread version
```c++
EngineRace.Write => Log.AddRecord + Memtable.Insert
EngineRace.Read => Memtable.Read + SST.Read
```