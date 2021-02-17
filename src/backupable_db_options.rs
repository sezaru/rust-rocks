use std::path::Path;
use std::ptr;
use std::u64;

use rocks_sys as ll;

use crate::env::Logger;
use crate::rate_limiter::RateLimiter;

use crate::to_raw::{FromRaw, ToRaw};

pub struct BackupableDBOptions {
    raw: *mut ll::rocks_backupable_db_options_t,
}

impl ToRaw<ll::rocks_backupable_db_options_t> for BackupableDBOptions {
    fn raw(&self) -> *mut ll::rocks_backupable_db_options_t {
        self.raw
    }
}

impl FromRaw<ll::rocks_backupable_db_options_t> for BackupableDBOptions {
    unsafe fn from_ll(raw: *mut ll::rocks_backupable_db_options_t) -> Self {
        BackupableDBOptions { raw }
    }
}

impl Drop for BackupableDBOptions {
    fn drop(&mut self) {
        unsafe {
            ll::rocks_backupable_db_options_destroy(self.raw);
        }
    }
}

impl BackupableDBOptions {
    /// Create BackupableDBOptions with default values for all fields
    pub fn new<P: AsRef<Path>>(path: P) -> BackupableDBOptions {
        BackupableDBOptions {
            raw: unsafe {
                let path_str = path.as_ref().to_str().unwrap();
                ll::rocks_backupable_db_options_create(path_str.as_ptr() as _)
            },
        }
    }

    unsafe fn from_ll(raw: *mut ll::rocks_backupable_db_options_t) -> BackupableDBOptions {
        BackupableDBOptions { raw: raw }
    }

    pub fn backup_dir<P: AsRef<Path>>(self, path: P) -> Self {
        unsafe {
            let path_str = path.as_ref().to_str().unwrap();
            ll::rocks_backupable_db_options_backup_dir(self.raw, path_str.as_ptr() as _);
        }
        self
    }

    pub fn share_table_files(self, val: bool) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_share_table_files(self.raw, val as u8);
        }
        self
    }

    pub fn info_log(self, val: Option<Logger>) -> Self {
        unsafe {
            if let Some(logger) = val {
                ll::rocks_backupable_db_options_set_info_log(self.raw, logger.raw());
            } else {
                ll::rocks_backupable_db_options_set_info_log(self.raw, ptr::null_mut());
            }
        }
        self
    }

    pub fn sync(self, val: bool) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_sync(self.raw, val as u8);
        }
        self
    }

    pub fn destroy_old_data(self, val: bool) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_destroy_old_data(self.raw, val as u8);
        }
        self
    }

    pub fn backup_log_files(self, val: bool) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_backup_log_files(self.raw, val as u8);
        }
        self
    }

    pub fn backup_rate_limit(self, val: u64) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_backup_rate_limit(self.raw, val);
        }
        self
    }

    pub fn backup_rate_limiter(self, val: Option<RateLimiter>) -> Self {
        unsafe {
            if let Some(limiter) = val {
                ll::rocks_backupable_db_options_set_backup_ratelimiter(self.raw, limiter.raw());
            } else {
                ll::rocks_backupable_db_options_set_backup_ratelimiter(self.raw, ptr::null_mut());
            }
        }
        self
    }

    pub fn restore_rate_limit(self, val: u64) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_restore_rate_limit(self.raw, val);
        }
        self
    }

    pub fn restore_rate_limiter(self, val: Option<RateLimiter>) -> Self {
        unsafe {
            if let Some(limiter) = val {
                ll::rocks_backupable_db_options_set_restore_ratelimiter(self.raw, limiter.raw());
            } else {
                ll::rocks_backupable_db_options_set_restore_ratelimiter(self.raw, ptr::null_mut());
            }
        }
        self
    }

    pub fn share_files_with_checksum(self, val: bool) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_share_files_with_checksum(self.raw, val as u8);
        }
        self
    }

    pub fn max_background_operations(self, val: i32) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_max_background_operations(self.raw, val);
        }
        self
    }

    pub fn callback_trigger_interval_size(self, val: u64) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_callback_trigger_interval_size(self.raw, val);
        }
        self
    }

    pub fn max_valid_backups_to_open(self, val: i32) -> Self {
        unsafe {
            ll::rocks_backupable_db_options_max_valid_backups_to_open(self.raw, val);
        }
        self
    }
}

pub struct CreateBackupOptions {
    raw: *mut ll::rocks_create_backup_options_t,
}

impl ToRaw<ll::rocks_create_backup_options_t> for CreateBackupOptions {
    fn raw(&self) -> *mut ll::rocks_create_backup_options_t {
        self.raw
    }
}

impl FromRaw<ll::rocks_create_backup_options_t> for CreateBackupOptions {
    unsafe fn from_ll(raw: *mut ll::rocks_create_backup_options_t) -> Self {
        CreateBackupOptions { raw }
    }
}

impl Default for CreateBackupOptions {
    fn default() -> Self {
        CreateBackupOptions {
            raw: unsafe { ll::rocks_create_backup_options_create() },
        }
    }
}

impl Drop for CreateBackupOptions {
    fn drop(&mut self) {
        unsafe {
            ll::rocks_create_backup_options_destroy(self.raw);
        }
    }
}

impl CreateBackupOptions {
    pub fn new() -> CreateBackupOptions {
        CreateBackupOptions {
            raw: unsafe { ll::rocks_create_backup_options_create() },
        }
    }

    unsafe fn from_ll(raw: *mut ll::rocks_create_backup_options_t) -> CreateBackupOptions {
        CreateBackupOptions { raw: raw }
    }

    pub fn flush_before_backup(self, val: bool) -> Self {
        unsafe {
            ll::rocks_create_backup_options_flush_before_backup(self.raw, val as u8);
        }
        self
    }

    pub fn decrease_background_thread_cpu_priority(self, val: bool) -> Self {
        unsafe {
            ll::rocks_create_backup_options_decrease_background_thread_cpu_priority(self.raw, val as u8);
        }
        self
    }
}

pub struct RestoreOptions {
    raw: *mut ll::rocks_restore_options_t,
}

impl ToRaw<ll::rocks_restore_options_t> for RestoreOptions {
    fn raw(&self) -> *mut ll::rocks_restore_options_t {
        self.raw
    }
}

impl FromRaw<ll::rocks_restore_options_t> for RestoreOptions {
    unsafe fn from_ll(raw: *mut ll::rocks_restore_options_t) -> Self {
        RestoreOptions { raw }
    }
}

impl Drop for RestoreOptions {
    fn drop(&mut self) {
        unsafe {
            ll::rocks_restore_options_destroy(self.raw);
        }
    }
}

impl RestoreOptions {
    pub fn new(keep_log_files: bool) -> RestoreOptions {
        RestoreOptions {
            raw: unsafe { ll::rocks_restore_options_create(keep_log_files as u8) },
        }
    }

    unsafe fn from_ll(raw: *mut ll::rocks_restore_options_t) -> RestoreOptions {
        RestoreOptions { raw: raw }
    }
}
