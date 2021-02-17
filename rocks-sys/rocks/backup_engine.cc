#include "rocksdb/utilities/backupable_db.h"

#include "rocks/ctypes.hpp"
#include "rocks/rust_export.h"
#include "rocksdb/utilities/info_log_finder.h"

using namespace ROCKSDB_NAMESPACE;

using std::shared_ptr;

extern "C" {
rocks_backup_engine_t* rocks_backup_engine_open(const rocks_backupable_db_options_t* options, rocks_status_t** status) {
  BackupEngine * be = nullptr;
  auto st = BackupEngine::Open(options->rep, nullptr, &be);
  if (SaveError(status, std::move(st))) {
    return nullptr;
  } else {
    rocks_backup_engine_t* result = new rocks_backup_engine_t;
    result->rep = be;
    return result;
  }
}

rocks_backup_engine_t* rocks_backup_engine_open_with_db_env(const rocks_backupable_db_options_t* options, rocks_env_t* env, rocks_status_t** status) {
  BackupEngine * be = nullptr;
  auto st = BackupEngine::Open(options->rep, env->rep, &be);
  if (SaveError(status, std::move(st))) {
    return nullptr;
  } else {
    rocks_backup_engine_t* result = new rocks_backup_engine_t;
    result->rep = be;
    return result;
  }
}

void rocks_backup_engine_destroy(rocks_backup_engine_t* engine) {
  delete engine->rep;
  delete engine;
}

void rocks_backup_engine_create_new_backup(rocks_backup_engine_t* engine, const rocks_create_backup_options_t* options, rocks_db_t* db, rocks_status_t** status) {
  SaveError(status, engine->rep->CreateNewBackup(options->rep, db->rep));
}

void rocks_backup_engine_create_new_backup_with_metadata(rocks_backup_engine_t* engine, const rocks_create_backup_options_t* options, rocks_db_t* db, const char* app_metadata,
                                                         rocks_status_t** status) {
  SaveError(status, engine->rep->CreateNewBackupWithMetadata(options->rep, db->rep, app_metadata));
}

void rocks_backup_engine_purge_old_backups(rocks_backup_engine_t* engine, uint32_t num_backups_to_keep, rocks_status_t** status) {
  SaveError(status, engine->rep->PurgeOldBackups(num_backups_to_keep));
}

// void rocks_backup_engine_delete_backup(rocks_backup_engine_t* engine, rocks_backup_id_t backup_id, rocks_status_t** status) {
//   SaveError(status, engine->rep->DeleteBackup(backup_id));
// }

void rocks_backup_engine_stop_backup(rocks_backup_engine_t* engine) {
  engine->rep->StopBackup();
}

// void rocks_backup_engine_get_backup_info(rocks_backup_engine_t* engine, rocks_backup_info_t** backup_info) {
//   engine->rep->GetBackupInfo(backup_info);
// }

// void rocks_backup_engine_get_corrupted_backups(rocks_backup_engine_t* engine, rocks_backup_id_t** corrupt_backup_ids) {
//   engine->rep->GetCorruptedBackups(corrupt_backup_ids);
// }

// void rocks_backup_engine_restore_db_from_backup(rocks_backup_engine_t* engine, const rocks_restore_options_t* options, rocks_backup_id_t backup_id, const char* db_dir,
//                                                 const char* wal_dir, rocks_status_t** status) {
//   SaveError(status, engine->rep->RestoreDBFromBackup(options->rep, backup_id, db_dir, wal_dir));
// }

void rocks_backup_engine_restore_db_from_latest_backup(rocks_backup_engine_t* engine, const rocks_restore_options_t* options, const char* db_dir, const char* wal_dir,
                                                       rocks_status_t** status) {
  SaveError(status, engine->rep->RestoreDBFromlatestBackup(options->rep, db_dir, wal_dir));
}

// void rocks_backup_engine_verify_backup(rocks_backup_engine_t* engine, const rocks_backup_id_t backup_id, unsigned char verify_with_checksum, rocks_status_t** status) {
//   SaveError(status, engine->rep->VerifyBackup(backup_id, verify_with_checksum));
// }

void rocks_backup_engine_garbage_collect(rocks_backup_engine_t* engine) {
  SaveError(status, engine->rep->GarbageCollect());
}
}
