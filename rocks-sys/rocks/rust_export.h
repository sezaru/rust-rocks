
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/listener.h"
#include "rocksdb/slice.h"
#include "rocksdb/table_properties.h"

#include <cstdint>
#include <string>

using namespace ROCKSDB_NAMESPACE;

#ifdef __cplusplus
extern "C" {
#endif

extern void rust_hello_world();

extern void rust_drop_vec_u8(char* op, size_t len);

extern void rust_string_assign(void* s, const char* p, size_t len);

extern void rust_vec_u8_assign(void* v, const char* p, size_t len);

extern void rust_vec_backup_info_assign(void* v, const rocksdb::BackupInfo* p, size_t len);

/* compaction filter */
extern int rust_compaction_filter_call(void* f, int level,
                                       const Slice* key,  // &&[u8]
                                       CompactionFilter::ValueType value_type,
                                       const Slice* existing_value,  // &&[u8]
                                       std::string* new_value, std::string* skip_until);

extern const char* rust_compaction_filter_name(void* f);

extern char rust_compaction_filter_ignore_snapshots(void* f);

extern void rust_compaction_filter_drop(void* f);

/* slice transform */
extern void rust_slice_transform_call(void* t, const Slice* key, char* const* ret, size_t* ret_len);

extern const char* rust_slice_transform_name(void* t);

extern char rust_slice_transform_in_domain(void* t, const Slice* key);

extern void rust_slice_transform_drop(void* t);

/* merge operator*/

extern int32_t rust_associative_merge_operator_call(void* op, const Slice* key, const Slice* existing_value,
                                                    const Slice* value, char** new_value, size_t* new_value_len,
                                                    Logger* logger);

extern const char* rust_associative_merge_operator_name(void* op);

extern void rust_associative_merge_operator_drop(void* op);

extern const char* rust_merge_operator_name(void* op);

extern int32_t rust_merge_operator_call_full_merge_v2(void* op, const void* merge_in, void* merge_out);

extern void rust_merge_operator_drop(void* op);

/* comparator */

extern int rust_comparator_compare(void* cp, const Slice* a, const Slice* b);

extern char rust_comparator_equal(void* cp, const Slice* a, const Slice* b);

extern const char* rust_comparator_name(const void* cp);

extern void rust_comparator_find_shortest_separator(void* cp, std::string* start, /* std::string */
                                                    const Slice* limit);

extern void rust_comparator_find_short_successor(void* cp, std::string* key);

extern void rust_comparator_drop(void* cp);

/* table_properties */

extern void rust_table_props_collector_add_user_key(void* c, const Slice* key, const Slice* value, int type,
                                                    uint64_t seq, uint64_t file_size);

extern void rust_table_props_collector_finish(void* c, UserCollectedProperties* props);

extern const char* rust_table_props_collector_name(void* c);

extern unsigned char rust_table_props_collector_need_compact(void* c);

extern void rust_table_props_collector_drop(void* c);

// *mut TablePropertiesCollector
extern void* rust_table_props_collector_factory_new_collector(void* f, uint32_t context);

extern const char* rust_table_props_collector_factory_name(void* f);

extern void rust_table_props_collector_factory_drop(void* f);

// write_batch
extern void rust_write_batch_handler_put_cf(void* h, uint32_t column_family_id, const Slice* key, const Slice* value);

extern void rust_write_batch_handler_delete_cf(void* h, uint32_t column_family_id, const Slice* key);

extern void rust_write_batch_handler_single_delete_cf(void* h, uint32_t column_family_id, const Slice* key);

extern void rust_write_batch_handler_delete_range_cf(void* h, uint32_t column_family_id, const Slice* begin_key,
                                                     const Slice* end_key);

extern void rust_write_batch_handler_merge_cf(void* h, uint32_t column_family_id, const Slice* key, const Slice* value);

extern void rust_write_batch_handler_log_data(void* h, const Slice* blob);

extern void rust_write_batch_handler_mark_begin_prepare(void* h);

extern void rust_write_batch_handler_mark_end_prepare(void* h, const Slice* xid);

extern void rust_write_batch_handler_mark_rollback(void* h, const Slice* xid);

extern void rust_write_batch_handler_mark_commit(void* h, const Slice* xid);

extern unsigned char rust_write_batch_handler_will_continue(void* h);

extern void rust_write_batch_handler_drop(void* h);

// listener

extern void rust_event_listener_drop(void* l);

// DB** is the same as DBRef
extern void rust_event_listener_on_flush_completed(void* l, DB**, const FlushJobInfo*);

extern void rust_event_listener_on_flush_begin(void* l, DB**, const FlushJobInfo*);

extern void rust_event_listener_on_table_file_deleted(void* l, const TableFileDeletionInfo*);

extern void rust_event_listener_on_compaction_completed(void* l, DB**, const CompactionJobInfo*);

extern void rust_event_listener_on_table_file_created(void* l, const TableFileCreationInfo*);

extern void rust_event_listener_on_table_file_creation_started(void* l, const TableFileCreationBriefInfo*);

extern void rust_event_listener_on_memtable_sealed(void* l, const MemTableInfo*);

struct rocks_column_family_handle_t;

extern void rust_event_listener_on_column_family_handle_deletion_started(void* l, rocks_column_family_handle_t*);

extern void rust_event_listener_on_external_file_ingested(void* l, DB**, const ExternalFileIngestionInfo*);

struct rocks_status_t;

extern unsigned char rust_event_listener_on_background_error(void* l, BackgroundErrorReason, rocks_status_t*);

extern void rust_compaction_event_listener_drop(void* l);

extern void* rust_event_listener_get_compaction_event_listener(void* l);

/*
// CompactionEventListener
extern void rust_compaction_event_listener_on_compaction(
    void* l, int32_t level, const Slice* key, CompactionEventListener::CompactionListenerValueType value_type,
    const Slice* existing_value, uint64_t sn, unsigned char is_new);
*/

#ifdef __cplusplus
}
#endif
