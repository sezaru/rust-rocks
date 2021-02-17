use std::ffi::CString;
use std::ops;
use std::ptr;
use std::str;
use std::sync::Arc;

use rocks_sys as ll;

use crate::db::DB;
use crate::env::Env;
use crate::backup_info::BackupInfo;
use crate::backupable_db_options::{
    BackupableDBOptions, CreateBackupOptions, RestoreOptions
};
use crate::to_raw::{FromRaw, ToRaw};
use crate::{Error, Result};

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
    pub fn open(options: BackupableDBOptions) -> Result<BackupEngine> {
        let opt = options.raw();
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        let env = Env::default_instance();

        unsafe {
            let db_ptr = ll::rocks_backup_engine_open(opt, env.raw(), &mut status);
            Error::from_ll(status).map(|_| BackupEngine::from_ll(db_ptr))
        }
    }

    pub fn open_with_env(options: BackupableDBOptions, env: &'static Env) -> Result<BackupEngine> {
        let opt = options.raw();
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe {
            let db_ptr = ll::rocks_backup_engine_open(opt, env.raw(), &mut status);
            Error::from_ll(status).map(|_| BackupEngine::from_ll(db_ptr))
        }
    }
}

impl BackupEngineRef {
    pub fn create_new_backup(&self, options: &CreateBackupOptions, db: &DB) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe {
            ll::rocks_backup_engine_create_new_backup(self.raw(), options.raw(), db.raw(), &mut status);
        }

        Error::from_ll(status)
    }

    pub fn create_new_backup_with_metadata(&self, options: &CreateBackupOptions, db: &DB, app_metadata: &str) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        let app_metadata_str = CString::new(app_metadata).unwrap();
        unsafe {
            ll::rocks_backup_engine_create_new_backup_with_metadata(self.raw(), options.raw(), db.raw(), app_metadata_str.as_ptr(), &mut status);
        }
        Error::from_ll(status)
    }

    pub fn purge_old_backups(&self, num_backups_to_keep: u32) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe { ll::rocks_backup_engine_purge_old_backups(self.raw(), num_backups_to_keep, &mut status); }
        Error::from_ll(status)
    }

    pub fn delete_backup(&self, backup_id: u32) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe { ll::rocks_backup_engine_purge_old_backups(self.raw(), backup_id, &mut status); }
        Error::from_ll(status)
    }

    pub fn stop_backup(&self) {
        unsafe { ll::rocks_backup_engine_stop_backup(self.raw()); }
    }

    pub fn get_backup_info(&self) -> Option<Vec<BackupInfo>> {
        let size = unsafe { ll::rocks_backup_engine_get_backup_info_size(self.raw()) };

        let mut backup_infos = vec![ptr::null_mut(); size];

        let response = unsafe { ll::rocks_backup_engine_get_backup_info(self.raw(), backup_infos.as_mut_ptr(), size) };

        if response != 0 {
            Some(backup_infos.into_iter().map(|b| unsafe { BackupInfo::from_ll(b) }).collect())
        } else {
            None
        }
    }

    pub fn restore_db_from_backup(&self, options: &RestoreOptions, backup_id: u32, db_dir: &str, wal_dir: &str) -> Result<()> {
        let db_dir_str = CString::new(db_dir).expect("valid utf8");
        let wal_dir_str = CString::new(wal_dir).expect("valid utf8");

        let mut status = ptr::null_mut::<ll::rocks_status_t>();

        unsafe {
        ll::rocks_backup_engine_restore_db_from_backup(
            self.raw(),
            options.raw(),
            backup_id,
            db_dir_str.as_ptr(),
            wal_dir_str.as_ptr(),
            &mut status
        );
        }

        Error::from_ll(status)
    }

    pub fn restore_db_from_latest_backup(&self, options: &RestoreOptions, db_dir: &str, wal_dir: &str) -> Result<()> {
        let db_dir_str = CString::new(db_dir).expect("valid utf8");
        let wal_dir_str = CString::new(wal_dir).expect("valid utf8");

        let mut status = ptr::null_mut::<ll::rocks_status_t>();

        unsafe {
        ll::rocks_backup_engine_restore_db_from_latest_backup(
            self.raw(),
            options.raw(),
            db_dir_str.as_ptr(),
            wal_dir_str.as_ptr(),
            &mut status
        );
        }

        Error::from_ll(status)
    }

    pub fn verify_backup(&self, backup_id: u32, verify_with_checksum: bool) -> Result<()> {
        let mut status = ptr::null_mut::<ll::rocks_status_t>();
        unsafe { ll::rocks_backup_engine_verify_backup(self.raw(), backup_id, verify_with_checksum as u8, &mut status); }
        Error::from_ll(status)
    }

     pub fn engine_garbage_collect(&self) {
         unsafe {ll::rocks_backup_engine_garbage_collect(self.raw());}
    }
}
