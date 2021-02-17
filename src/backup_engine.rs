use std::ffi::CString;
use std::ops;
use std::path::Path;
use std::ptr;
use std::str;
use std::sync::Arc;

use rocks_sys as ll;

use crate::db::DB;
use crate::env::Env;
use crate::backupable_db_options::{
    BackupableDBOptions, CreateBackupOptions, RestoreOptions
};
use crate::to_raw::{FromRaw, ToRaw};
use crate::{Error, Result};

/// Borrowed BackupEngine handle
pub struct BackupEngineRef {
    raw: *mut ll::rocks_backup_engine_t,
}

impl Drop for BackupEngineRef {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ll::rocks_backup_engine_destroy(self.raw);
        }
    }
}

impl ToRaw<ll::rocks_backup_engine_t> for BackupEngineRef {
    fn raw(&self) -> *mut ll::rocks_backup_engine_t {
        self.raw
    }
}

unsafe impl Sync for BackupEngineRef {}
unsafe impl Send for BackupEngineRef {}

pub struct BackupEngine {
    context: Arc<BackupEngineRef>,
}

impl ops::Deref for BackupEngine {
    type Target = BackupEngineRef;

    fn deref(&self) -> &BackupEngineRef {
        &self.context
    }
}

unsafe impl Sync for BackupEngine {}
unsafe impl Send for BackupEngine {}

impl ToRaw<ll::rocks_backup_engine_t> for BackupEngine {
    fn raw(&self) -> *mut ll::rocks_backup_engine_t {
        self.context.raw
    }
}

impl FromRaw<ll::rocks_backup_engine_t> for BackupEngine {
    unsafe fn from_ll(raw: *mut ll::rocks_backup_engine_t) -> BackupEngine {
        let context = BackupEngineRef { raw: raw };
        BackupEngine {
            context: Arc::new(context),
        }
    }
}

impl BackupEngine {
    pub fn open<T: AsRef<BackupableDBOptions>>(options: T) -> Result<BackupEngine> {
        let opt = options.as_ref().raw();
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe {
            let db_ptr = ll::rocks_backup_engine_open(opt, &mut status);
            Error::from_ll(status).map(|_| BackupEngine::from_ll(db_ptr))
        }
    }

    pub fn open_with_db_env<T: AsRef<BackupableDBOptions>>(options: T, env: &'static Env) -> Result<BackupEngine> {
        let opt = options.as_ref().raw();
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe {
            let db_ptr = ll::rocks_backup_engine_open_with_db_env(opt, env.raw(), &mut status);
            Error::from_ll(status).map(|_| BackupEngine::from_ll(db_ptr))
        }
    }
}

impl BackupEngineRef {
    pub unsafe fn create_new_backup(&self, options: &CreateBackupOptions, db: &DB) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        ll::rocks_backup_engine_create_new_backup(self.raw(), options.raw(), db.raw(), &mut status);
        Error::from_ll(status)
    }

    pub unsafe fn create_new_backup_with_metadata(&self, options: &CreateBackupOptions, db: &DB, app_metadata: &str) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        let app_metadata_str = CString::new(app_metadata).unwrap();
        ll::rocks_backup_engine_create_new_backup_with_metadata(self.raw(), options.raw(), db.raw(), app_metadata_str.as_ptr(), &mut status);
        Error::from_ll(status)
    }

    pub unsafe fn purge_old_backups(&self, num_backups_to_keep: usize) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        ll::rocks_backup_engine_purge_old_backups(self.raw(), num_backups_to_keep, &mut status);
        Error::from_ll(status)
    }

    pub unsafe fn engine_stop_backup(&self) {
        ll::rocks_backup_engine_stop_backup(self.raw());
    }

    pub unsafe fn restore_db_from_latest_backup<P: AsRef<Path>>(&self, options: &RestoreOptions, db_dir: P, wal_dir: P) -> Result<()> {
        let db_dir_str = db_dir.as_ref().to_str().expect("valid utf8");
        let wal_dir_str = wal_dir.as_ref().to_str().expect("valid utf8");

        let mut status = ptr::null_mut::<ll::rocks_status_t>();

        ll::rocks_backup_engine_restore_db_from_latest_backup(
            self.raw(),
            options.raw(),
            db_dir_str.as_ptr() as *const _,
            wal_dir_str.as_ptr() as *const _,
            &mut status
        );

        Error::from_ll(status)
    }

     pub unsafe fn engine_garbage_collect(&self) {
        ll::rocks_backup_engine_garbage_collect(self.raw());
    }
}
