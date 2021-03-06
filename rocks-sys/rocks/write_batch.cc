#include "rocksdb/write_batch.h"

#include "rocks/ctypes.hpp"

using namespace ROCKSDB_NAMESPACE;

extern "C" {
rocks_writebatch_t* rocks_writebatch_create() {
  return new rocks_writebatch_t{std::unique_ptr<WriteBatch>(new WriteBatch())};
}

rocks_writebatch_t* rocks_writebatch_create_with_reserved_bytes(size_t size) {
  return new rocks_writebatch_t{std::unique_ptr<WriteBatch>(new WriteBatch(size))};
}

rocks_writebatch_t* rocks_writebatch_create_from(const char* rep, size_t size) {
  return new rocks_writebatch_t{std::unique_ptr<WriteBatch>(new WriteBatch(std::string(rep, size)))};
}

void rocks_writebatch_destroy(rocks_writebatch_t* b) { delete b; }

void rocks_writebatch_clear(rocks_writebatch_t* b) { b->rep->Clear(); }

int rocks_writebatch_count(rocks_writebatch_t* b) { return b->rep->Count(); }

void rocks_writebatch_put(rocks_writebatch_t* b, const char* key, size_t klen, const char* val, size_t vlen) {
  b->rep->Put(Slice(key, klen), Slice(val, vlen));
}

void rocks_writebatch_put_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, const char* key,
                             size_t klen, const char* val, size_t vlen) {
  b->rep->Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocks_writebatch_putv(rocks_writebatch_t* b, int num_keys, const char* const* keys_list,
                           const size_t* keys_list_sizes, int num_values, const char* const* values_list,
                           const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(SliceParts(key_slices.data(), num_keys), SliceParts(value_slices.data(), num_values));
}

void rocks_writebatch_putv_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, int num_keys,
                              const char* const* keys_list, const size_t* keys_list_sizes, int num_values,
                              const char* const* values_list, const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(column_family->rep, SliceParts(key_slices.data(), num_keys), SliceParts(value_slices.data(), num_values));
}

void rocks_writebatch_putv_coerce(rocks_writebatch_t* b, const void* key_parts, int num_keys, const void* value_parts,
                                  int num_values) {
  b->rep->Put(SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys),
              SliceParts(reinterpret_cast<const Slice*>(value_parts), num_values));
}

void rocks_writebatch_putv_cf_coerce(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                     const void* key_parts, int num_keys, const void* value_parts, int num_values) {
  b->rep->Put(column_family->rep, SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys),
              SliceParts(reinterpret_cast<const Slice*>(value_parts), num_values));
}

void rocks_writebatch_merge(rocks_writebatch_t* b, const char* key, size_t klen, const char* val, size_t vlen) {
  b->rep->Merge(Slice(key, klen), Slice(val, vlen));
}

void rocks_writebatch_merge_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, const char* key,
                               size_t klen, const char* val, size_t vlen) {
  b->rep->Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocks_writebatch_mergev(rocks_writebatch_t* b, int num_keys, const char* const* keys_list,
                             const size_t* keys_list_sizes, int num_values, const char* const* values_list,
                             const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(SliceParts(key_slices.data(), num_keys), SliceParts(value_slices.data(), num_values));
}

void rocks_writebatch_mergev_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, int num_keys,
                                const char* const* keys_list, const size_t* keys_list_sizes, int num_values,
                                const char* const* values_list, const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
                SliceParts(value_slices.data(), num_values));
}

void rocks_writebatch_mergev_coerce(rocks_writebatch_t* b, const void* key_parts, int num_keys, const void* value_parts,
                                    int num_values) {
  b->rep->Merge(SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys),
                SliceParts(reinterpret_cast<const Slice*>(value_parts), num_values));
}

void rocks_writebatch_mergev_cf_coerce(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                       const void* key_parts, int num_keys, const void* value_parts, int num_values) {
  b->rep->Merge(column_family->rep, SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys),
                SliceParts(reinterpret_cast<const Slice*>(value_parts), num_values));
}

void rocks_writebatch_delete(rocks_writebatch_t* b, const char* key, size_t klen) { b->rep->Delete(Slice(key, klen)); }

void rocks_writebatch_delete_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, const char* key,
                                size_t klen) {
  b->rep->Delete(column_family->rep, Slice(key, klen));
}

void rocks_writebatch_deletev(rocks_writebatch_t* b, int num_keys, const char* const* keys_list,
                              const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(SliceParts(key_slices.data(), num_keys));
}

void rocks_writebatch_deletev_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family, int num_keys,
                                 const char* const* keys_list, const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void rocks_writebatch_deletev_coerce(rocks_writebatch_t* b, const void* key_parts, int num_keys) {
  b->rep->Delete(SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys));
}

void rocks_writebatch_deletev_cf_coerce(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                        const void* key_parts, int num_keys) {
  b->rep->Delete(column_family->rep, SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys));
}

void rocks_writebatch_single_delete(rocks_writebatch_t* b, const char* key, size_t klen) {
  b->rep->SingleDelete(Slice(key, klen));
}

void rocks_writebatch_single_delete_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                       const char* key, size_t klen) {
  b->rep->SingleDelete(column_family->rep, Slice(key, klen));
}

void rocks_writebatch_single_deletev_coerce(rocks_writebatch_t* b, const void* key_parts, int num_keys) {
  b->rep->SingleDelete(SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys));
}

void rocks_writebatch_single_deletev_cf_coerce(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                               const void* key_parts, int num_keys) {
  b->rep->SingleDelete(column_family->rep, SliceParts(reinterpret_cast<const Slice*>(key_parts), num_keys));
}

void rocks_writebatch_delete_range(rocks_writebatch_t* b, const char* start_key, size_t start_key_len,
                                   const char* end_key, size_t end_key_len) {
  b->rep->DeleteRange(Slice(start_key, start_key_len), Slice(end_key, end_key_len));
}

void rocks_writebatch_delete_range_cf(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                      const char* start_key, size_t start_key_len, const char* end_key,
                                      size_t end_key_len) {
  b->rep->DeleteRange(column_family->rep, Slice(start_key, start_key_len), Slice(end_key, end_key_len));
}

void rocks_writebatch_deletev_range_cf_coerce(rocks_writebatch_t* b, rocks_column_family_handle_t* column_family,
                                              const void* begin_key_parts, int num_begin_keys,
                                              const void* end_key_parts, int num_end_keys) {
  if (column_family == nullptr) {
    b->rep->DeleteRange(nullptr, SliceParts(reinterpret_cast<const Slice*>(begin_key_parts), num_begin_keys),
                        SliceParts(reinterpret_cast<const Slice*>(end_key_parts), num_end_keys));
  } else {
    b->rep->DeleteRange(column_family->rep, SliceParts(reinterpret_cast<const Slice*>(begin_key_parts), num_begin_keys),
                        SliceParts(reinterpret_cast<const Slice*>(end_key_parts), num_end_keys));
  }
}

void rocks_writebatch_put_log_data(rocks_writebatch_t* b, const char* blob, size_t len) {
  b->rep->PutLogData(Slice(blob, len));
}

void rocks_writebatch_iterate(rocks_writebatch_t* b, void* trait_obj, rocks_status_t** status) {
  auto handler = new rocks_writebatch_handler_t(trait_obj);
  auto st = b->rep->Iterate(handler);
  delete handler;
  SaveError(status, std::move(st));
}

const char* rocks_writebatch_data(rocks_writebatch_t* b, size_t* size) {
  *size = b->rep->GetDataSize();
  return b->rep->Data().c_str();
}

void rocks_writebatch_set_save_point(rocks_writebatch_t* b) { b->rep->SetSavePoint(); }

void rocks_writebatch_rollback_to_save_point(rocks_writebatch_t* b, rocks_status_t** status) {
  SaveError(status, std::move(b->rep->RollbackToSavePoint()));
}

void rocks_writebatch_pop_save_point(rocks_writebatch_t* b, rocks_status_t** status) {
  SaveError(status, std::move(b->rep->PopSavePoint()));
}

rocks_writebatch_t* rocks_writebatch_copy(rocks_writebatch_t* b) {
  return new rocks_writebatch_t{std::unique_ptr<WriteBatch>(new WriteBatch(*b->rep))};
}

unsigned char rocks_writebatch_has_put(rocks_writebatch_t* b) { return b->rep->HasPut(); }
unsigned char rocks_writebatch_has_delete(rocks_writebatch_t* b) { return b->rep->HasDelete(); }
unsigned char rocks_writebatch_has_single_delete(rocks_writebatch_t* b) { return b->rep->HasSingleDelete(); }
unsigned char rocks_writebatch_has_delete_range(rocks_writebatch_t* b) { return b->rep->HasDeleteRange(); }
unsigned char rocks_writebatch_has_merge(rocks_writebatch_t* b) { return b->rep->HasMerge(); }
unsigned char rocks_writebatch_has_begin_prepare(rocks_writebatch_t* b) { return b->rep->HasBeginPrepare(); }
unsigned char rocks_writebatch_has_end_prepare(rocks_writebatch_t* b) { return b->rep->HasEndPrepare(); }
unsigned char rocks_writebatch_has_commit(rocks_writebatch_t* b) { return b->rep->HasCommit(); }
unsigned char rocks_writebatch_has_rollback(rocks_writebatch_t* b) { return b->rep->HasRollback(); }

// this is for wrapped writebatch impl, for e.g. WriteBatchWithIndex
rocks_raw_writebatch_t* rocks_writebatch_get_writebatch(rocks_writebatch_t* b) {
  return reinterpret_cast<rocks_raw_writebatch_t*>(b->rep->GetWriteBatch());
}
}
