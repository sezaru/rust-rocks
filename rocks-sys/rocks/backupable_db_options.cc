#include "rocksdb/utilities/backupable_db.h"

#include "rocks/ctypes.hpp"

using namespace ROCKSDB_NAMESPACE;

extern "C" {

rocks_backupable_db_options_t* rocks_backupable_db_options_create(const char* dir) {
  auto opt = new rocks_backupable_db_options_t;

  opt->rep = BackupableDBOptions(dir);

  return opt;
}

void rocks_backupable_db_options_destroy(rocks_backupable_db_options_t* options) { delete options; }

void rocks_backupable_db_options_backup_dir(rocks_backupable_db_options_t* opt, const char* dir) { opt->rep.backup_dir = dir; }

void rocks_backupable_db_options_share_table_files(rocks_backupable_db_options_t* opt, unsigned char share_table_files) {
  opt->rep.share_table_files = share_table_files;
}

void rocks_backupable_db_options_options_set_info_log(rocks_backupable_db_options_t* opt, rocks_logger_t* l) {
  if (l) {
    opt->rep.info_log = l->rep;
  }
}

void rocks_backupable_db_options_sync(rocks_backupable_db_options_t* opt, unsigned char sync) {
  opt->rep.sync = sync;
}

void rocks_backupable_db_options_destroy_old_data(rocks_backupable_db_options_t* opt, unsigned char destroy_old_data) {
  opt->rep.destroy_old_data = destroy_old_data;
}

void rocks_backupable_db_options_backup_log_files(rocks_backupable_db_options_t* opt, unsigned char backup_log_files) {
  opt->rep.backup_log_files = backup_log_files;
}

void rocks_backupable_db_options_backup_rate_limit(rocks_backupable_db_options_t* opt, uint64_t backup_rate_limit) {
  opt->rep.backup_rate_limit = backup_rate_limit
}

void rocks_backupable_db_options_set_backup_ratelimiter(rocks_backupable_db_options_t* opt, rocks_ratelimiter_t* limiter) {
  if (limiter != nullptr) {
    opt->rep.backup_rate_limiter = limiter->rep;
  } else {
    opt->rep.backup_rate_limiter.reset((RateLimiter*)nullptr);
  }
}

void rocks_backupable_db_options_restore_rate_limit(rocks_backupable_db_options_t* opt, uint64_t restore_rate_limit) {
  opt->rep.restore_rate_limit = restore_rate_limit
}

void rocks_backupable_db_options_set_restore_ratelimiter(rocks_backupable_db_options_t* opt, rocks_ratelimiter_t* limiter) {
  if (limiter != nullptr) {
    opt->rep.restore_rate_limiter = limiter->rep;
  } else {
    opt->rep.restore_rate_limiter.reset((RateLimiter*)nullptr);
  }
}

void rocks_backupable_db_options_share_files_with_checksum(rocks_backupable_db_options_t* opt, unsigned char share_files_with_checksum) {
  opt->rep.share_files_with_checksum = share_files_with_checksum;
}

void rocks_backupable_db_options_max_background_operations(rocks_backupable_db_options_t* opt, int max_background_operations) {
  opt->rep.max_background_operations = max_background_operations;
}

void rocks_backupable_db_options_callback_trigger_interval_size(rocks_backupable_db_options_t* opt, uint64_t callback_trigger_interval_size) {
  opt->rep.callback_trigger_interval_size = callback_trigger_interval_size;
}

void rocks_backupable_db_options_max_valid_backups_to_open(rocks_backupable_db_options_t* opt, int max_valid_backups_to_open) {
  opt->rep.max_valid_backups_to_open = max_valid_backups_to_open;
}

// void rocks_backupable_db_options_share_files_with_checksum_naming(rocks_backupable_db_options_t* opt, int share_files_with_checksum_naming) {
//   opt->rep.share_files_with_checksum_naming = share_files_with_checksum_naming;
// }
}

extern "C" {
rocks_create_backup_options_t* rocks_create_backup_options_create() {
  return new rocks_create_backup_options_t;
}

void rocks_create_backup_options_destroy(rocks_create_backup_options_t* options) { delete options; }

void rocks_create_backup_options_flush_before_backup(rocks_create_backup_options_t* opt, unsigned char flush_before_backup) {
  opt->rep.flush_before_backup = flush_before_backup;
}

void rocks_create_backup_options_decrease_background_thread_cpu_priority(rocks_create_backup_options_t* opt, unsigned char decrease_background_thread_cpu_priority) {
  opt->rep.decrease_background_thread_cpu_priority = decrease_background_thread_cpu_priority;
}

// void rocks_create_backup_options_background_thread_cpu_priority(rocks_create_backup_options_t* opt, int background_thread_cpu_priority) {
//   opt->rep.background_thread_cpu_priority = background_thread_cpu_priority;
// }
}

extern "C" {
rocks_restore_options_t* rocks_restore_options_create(unsigned char keep_log_files) {
  auto options =  new rocks_restore_options_t;
  options->rep = RestoreOptions(keep_log_files);

  return options;
}

void rocks_restore_options_destroy(rocks_restore_options_t* options) { delete options; }
}
