use std::ffi::{CStr};
use std::ops;
use std::sync::Arc;

use rocks_sys as ll;

use crate::to_raw::{FromRaw, ToRaw};

pub struct BackupInfoRef {
    raw: *mut ll::rocks_backup_info_t,
}

impl Drop for BackupInfoRef {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            ll::rocks_backup_info_destroy(self.raw);
        }
    }
}

impl ToRaw<ll::rocks_backup_info_t> for BackupInfoRef {
    fn raw(&self) -> *mut ll::rocks_backup_info_t {
        self.raw
    }
}

unsafe impl Sync for BackupInfoRef {}
unsafe impl Send for BackupInfoRef {}

pub struct BackupInfo {
    context: Arc<BackupInfoRef>,
}

impl ops::Deref for BackupInfo {
    type Target = BackupInfoRef;

    fn deref(&self) -> &BackupInfoRef {
        &self.context
    }
}

unsafe impl Sync for BackupInfo {}
unsafe impl Send for BackupInfo {}

impl ToRaw<ll::rocks_backup_info_t> for BackupInfo {
    fn raw(&self) -> *mut ll::rocks_backup_info_t {
        self.context.raw
    }
}

impl FromRaw<ll::rocks_backup_info_t> for BackupInfo {
    unsafe fn from_ll(raw: *mut ll::rocks_backup_info_t) -> BackupInfo {
        let context = BackupInfoRef { raw: raw };
        BackupInfo {
            context: Arc::new(context),
        }
    }
}

impl BackupInfoRef {
    pub fn backup_id(&self) -> u32 {
        unsafe { ll::rocks_backup_info_get_backup_id(self.raw()) }
    }

    pub fn timestamp(&self) -> i64 {
        unsafe { ll::rocks_backup_info_get_timestamp(self.raw()) }
    }

    pub fn size(&self) -> u64 {
        unsafe { ll::rocks_backup_info_get_size(self.raw()) }
    }

    pub fn number_of_files(&self) -> u32 {
        unsafe { ll::rocks_backup_info_get_number_files(self.raw()) }
    }

    pub fn app_metadata(&self) -> String {
        unsafe {
            CStr::from_ptr(ll::rocks_backup_info_get_app_metadata(self.raw()))
                .to_string_lossy()
                .to_owned()
                .to_string()
        }
    }
}
