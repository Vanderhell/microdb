# TODO

Hotove:
- zalozena zakladna struktura projektu (`include/`, `src/`, `CMakeLists.txt`)
- vytvoreny verejny header `include/microdb.h` s compile-time konfiguraciou, typmi a API deklaraciami
- verejne handlye upravene podla `microdb_agent_guide.md` (`MICRODB_HANDLE_SIZE`, `MICRODB_SCHEMA_SIZE`, interny `microdb_table_t`)
- implementovane `microdb_init()`, `microdb_deinit()`, `microdb_flush()`, `microdb_stats()`
- zavedene rozdelenie jedinej alokacie RAM na KV/TS/REL slices
- pripraveny interny arena allocator a interny handle layout
- pridane samostatne moduly `src/microdb_arena.h` a `src/microdb_crc.[ch]`
- rozbehnuty funkcny zaklad KV enginu: set/get/del/exists/iter/purge_expired/clear
- TTL kontrola pri `get` a `exists`
- LRU eviction pri `MICRODB_KV_POLICY_OVERWRITE`
- compact value store po mazani alebo expirovani
- doplneny `port/ram/` HAL pre buduce testy
- build overeny mimo sandboxu, kniznica sa kompiluje
- pridany lokalny test shim `tests/microtest.h`
- KV engine dorobeny na plne spravanie pre aktualny scope:
- overwrite existujuceho kluca reuses val_pool slot bez zbytocneho growthu
- LRU eviction pre OVERWRITE policy
- samostatny build variant pre REJECT policy
- TTL/expire/get/exists/purge/iter/clear edge cases pokryte testami
- compaction trigger pri fragmentacii > 50%
- KV test suite rozsirena na 40/40 passing testov
- osetreny overwrite rovnakeho kluca bez zbytocneho rastu value pool metadata
- pripravene stuby pre TS, REL a WAL, aby sa projekt dal kompilovat a rozsirovat postupne

Rozpracovane / dalsie kroky:
- persistence cez WAL a storage HAL
- testy pre KV edge cases a limity
- TS engine zacaty:
- TS engine dokonceny pre aktualny scope:
- `register/insert/last/query/query_buf/count/clear`
- overflow policy build varianty: `DROP_OLDEST`, `REJECT`, `DOWNSAMPLE`
- downsample merge pre dve najstarsie samples
- TS test suite rozsirena na 35/35 passing testov
- REL engine dokonceny pre aktualny scope:
- schema seal/alignment, table create/get, row set/get, insert/find/find_by/delete/iter/count/clear
- sorted index + binary search + alive bitmap
- REL test suite rozsirena na 40/40 passing testov
- implementovat WAL recovery a flush do storage pages
- pripravit `port/posix/` pre WAL testy
- doplnit porty (`posix`, `ram`, `esp32`)
- doplnit testy a examples podla spec

Poznamky:
- TS/REL API je momentalne placeholder a vracia `MICRODB_ERR_DISABLED`
- WAL hook je pripraveny, ale persistence este nie je implementovana
