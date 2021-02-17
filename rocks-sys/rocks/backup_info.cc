#include "rocksdb/utilities/backupable_db.h"

#include "rocks/ctypes.hpp"
#include "rocks/rust_export.h"

using namespace ROCKSDB_NAMESPACE;

extern "C" {
void rocks_backup_info_destroy(rocks_backup_info_t* info) {
  delete info;
}

uint32_t rocks_backup_info_get_backup_id(const rocks_backup_info_t* info) {
  return info->rep.backup_id;
}

int64_t rocks_backup_info_get_timestamp(const rocks_backup_info_t* info) {
  return info->rep.timestamp;
}

uint64_t rocks_backup_info_get_size(const rocks_backup_info_t* info) {
  return info->rep.size;
}

uint32_t rocks_backup_info_get_number_files(const rocks_backup_info_t* info) {
  return info->rep.number_files;
}

const char* rocks_backup_info_get_app_metadata(const rocks_backup_info_t* info) {
  return info->rep.app_metadata.c_str();
}
}
